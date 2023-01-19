// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

//! Query handler

use std::{io::Cursor, mem, time::Instant};

use arrow_ext::ipc::{Compression, RecordBatchesEncoder};
use ceresdbproto::{
    common::ResponseHeader,
    storage::{
        arrow_payload, sql_query_response, storage_service_client::StorageServiceClient,
        ArrowPayload, SqlQueryRequest, SqlQueryResponse,
    },
};
use common_types::{record_batch::RecordBatch, request_id::RequestId};
use common_util::time::InstantExt;
use futures::FutureExt;
use http::StatusCode;
use interpreters::{context::Context as InterpreterContext, factory::Factory, interpreter::Output};
use log::{error, info, warn};
use query_engine::executor::Executor as QueryExecutor;
use router::endpoint::Endpoint;
use snafu::{ensure, ResultExt};
use sql::{
    frontend::{Context as SqlContext, Frontend},
    provider::CatalogMetaProvider,
};
use tonic::{transport::Channel, IntoRequest};

use crate::grpc::{
    forward::{ForwardRequest, ForwardResult},
    storage_service::{
        error::{ErrNoCause, ErrWithCause, Result},
        HandlerContext,
    },
};

/// Builder for building [`SqlQueryResponse`].
#[derive(Debug, Default)]
pub struct QueryResponseBuilder {
    header: ResponseHeader,
}

impl QueryResponseBuilder {
    pub fn with_ok_header() -> Self {
        let header = ResponseHeader {
            code: StatusCode::OK.as_u16() as u32,
            ..Default::default()
        };
        Self { header }
    }

    pub fn build_with_affected_rows(self, affected_rows: usize) -> SqlQueryResponse {
        let output = Some(sql_query_response::Output::AffectedRows(
            affected_rows as u32,
        ));
        SqlQueryResponse {
            header: Some(self.header),
            output,
        }
    }

    pub fn build_with_empty_arrow_payload(self) -> SqlQueryResponse {
        let payload = ArrowPayload {
            record_batches: Vec::new(),
            compression: arrow_payload::Compression::None as i32,
        };
        self.build_with_arrow_payload(payload)
    }

    pub fn build_with_arrow_payload(self, payload: ArrowPayload) -> SqlQueryResponse {
        let output = Some(sql_query_response::Output::Arrow(payload));
        SqlQueryResponse {
            header: Some(self.header),
            output,
        }
    }
}

async fn maybe_forward_query<Q: QueryExecutor + 'static>(
    ctx: &HandlerContext<'_, Q>,
    req: &SqlQueryRequest,
) -> Option<Result<SqlQueryResponse>> {
    let forwarder = ctx.forwarder.as_ref()?;

    if req.tables.len() != 1 {
        warn!(
            "Unable to forward query without exactly one table, req:{:?}",
            req
        );

        return None;
    }

    let forward_req = ForwardRequest {
        schema: ctx.schema.clone(),
        table: req.tables[0].clone(),
        req: req.clone().into_request(),
    };
    let do_query = |mut client: StorageServiceClient<Channel>,
                    request: tonic::Request<SqlQueryRequest>,
                    _: &Endpoint| {
        let query = async move {
            client
                .sql_query(request)
                .await
                .map(|resp| resp.into_inner())
                .map_err(|e| Box::new(e) as _)
                .context(ErrWithCause {
                    code: StatusCode::INTERNAL_SERVER_ERROR,
                    msg: "Forwarded query failed".to_string(),
                })
        }
        .boxed();

        Box::new(query) as _
    };

    match forwarder.forward(forward_req, do_query).await {
        Ok(forward_res) => match forward_res {
            ForwardResult::Forwarded(v) => Some(v),
            ForwardResult::Original => None,
        },
        Err(e) => {
            error!("Failed to forward req but the error is ignored, err:{}", e);
            None
        }
    }
}

pub async fn handle_query<Q: QueryExecutor + 'static>(
    ctx: &HandlerContext<'_, Q>,
    req: SqlQueryRequest,
) -> Result<SqlQueryResponse> {
    let req = match maybe_forward_query(ctx, &req).await {
        Some(resp) => return resp,
        None => req,
    };

    let output = fetch_query_output(ctx, &req).await?;
    convert_output(
        &output,
        ctx.min_rows_per_batch,
        ctx.datum_compression_threshold,
    )
    .map_err(|e| Box::new(e) as _)
    .with_context(|| ErrWithCause {
        code: StatusCode::INTERNAL_SERVER_ERROR,
        msg: format!("Failed to convert output, sql:{}", &req.sql),
    })
}

pub async fn fetch_query_output<Q: QueryExecutor + 'static>(
    ctx: &HandlerContext<'_, Q>,
    req: &SqlQueryRequest,
) -> Result<Output> {
    let request_id = RequestId::next_id();
    let begin_instant = Instant::now();
    let deadline = ctx.timeout.map(|t| begin_instant + t);

    info!(
        "Grpc handle query begin, catalog:{}, schema:{}, request_id:{}, request:{:?}",
        ctx.catalog(),
        ctx.schema(),
        request_id,
        req,
    );

    let instance = &ctx.instance;
    // TODO(yingwen): Privilege check, cannot access data of other tenant
    // TODO(yingwen): Maybe move MetaProvider to instance
    let provider = CatalogMetaProvider {
        manager: instance.catalog_manager.clone(),
        default_catalog: ctx.catalog(),
        default_schema: ctx.schema(),
        function_registry: &*instance.function_registry,
    };
    let frontend = Frontend::new(provider);

    let mut sql_ctx = SqlContext::new(request_id, deadline);
    // Parse sql, frontend error of invalid sql already contains sql
    // TODO(yingwen): Maybe move sql from frontend error to outer error
    let mut stmts = frontend
        .parse_sql(&mut sql_ctx, &req.sql)
        .map_err(|e| Box::new(e) as _)
        .context(ErrWithCause {
            code: StatusCode::BAD_REQUEST,
            msg: "failed to parse sql",
        })?;

    ensure!(
        !stmts.is_empty(),
        ErrNoCause {
            code: StatusCode::BAD_REQUEST,
            msg: format!("No valid query statement provided, sql:{}", req.sql),
        }
    );

    // TODO(yingwen): For simplicity, we only support executing one statement now
    // TODO(yingwen): INSERT/UPDATE/DELETE can be batched
    ensure!(
        stmts.len() == 1,
        ErrNoCause {
            code: StatusCode::BAD_REQUEST,
            msg: format!(
                "Only support execute one statement now, current num:{}, sql:{}",
                stmts.len(),
                req.sql
            ),
        }
    );

    // Create logical plan
    // Note: Remember to store sql in error when creating logical plan
    let plan = frontend
        // TODO(yingwen): Check error, some error may indicate that the sql is invalid. Now we
        // return internal server error in those cases
        .statement_to_plan(&mut sql_ctx, stmts.remove(0))
        .map_err(|e| Box::new(e) as _)
        .with_context(|| ErrWithCause {
            code: StatusCode::INTERNAL_SERVER_ERROR,
            msg: format!("Failed to create plan, query:{}", req.sql),
        })?;

    ctx.instance
        .limiter
        .try_limit(&plan)
        .map_err(|e| Box::new(e) as _)
        .context(ErrWithCause {
            code: StatusCode::FORBIDDEN,
            msg: "Query is blocked",
        })?;

    if let Some(deadline) = deadline {
        if deadline.check_deadline() {
            return ErrNoCause {
                code: StatusCode::REQUEST_TIMEOUT,
                msg: "Query timeout",
            }
            .fail();
        }
    }

    // Execute in interpreter
    let interpreter_ctx = InterpreterContext::builder(request_id, deadline)
        // Use current ctx's catalog and schema as default catalog and schema
        .default_catalog_and_schema(ctx.catalog().to_string(), ctx.schema().to_string())
        .build();
    let interpreter_factory = Factory::new(
        instance.query_executor.clone(),
        instance.catalog_manager.clone(),
        instance.table_engine.clone(),
        instance.table_manipulator.clone(),
    );
    let interpreter = interpreter_factory
        .create(interpreter_ctx, plan)
        .map_err(|e| Box::new(e) as _)
        .with_context(|| ErrWithCause {
            code: StatusCode::INTERNAL_SERVER_ERROR,
            msg: "Failed to create interpreter",
        })?;

    let output = if let Some(deadline) = deadline {
        tokio::time::timeout_at(
            tokio::time::Instant::from_std(deadline),
            interpreter.execute(),
        )
        .await
        .map_err(|e| Box::new(e) as _)
        .context(ErrWithCause {
            code: StatusCode::REQUEST_TIMEOUT,
            msg: "Query timeout",
        })?
    } else {
        interpreter.execute().await
    }
    .map_err(|e| Box::new(e) as _)
    .with_context(|| ErrWithCause {
        code: StatusCode::INTERNAL_SERVER_ERROR,
        msg: format!("Failed to execute interpreter, sql:{}", req.sql),
    })?;

    info!(
        "Grpc handle query success, catalog:{}, schema:{}, request_id:{}, cost:{}ms, request:{:?}",
        ctx.catalog(),
        ctx.schema(),
        request_id,
        begin_instant.saturating_elapsed().as_millis(),
        req,
    );

    Ok(output)
}

// TODO(chenxiang): Output can have both `rows` and `affected_rows`
fn convert_output(
    output: &Output,
    min_rows_per_batch: usize,
    datum_compression_threshold: usize,
) -> Result<SqlQueryResponse> {
    match output {
        Output::Records(batches) => {
            let mut writer =
                QueryResponseWriter::new(min_rows_per_batch, datum_compression_threshold);
            writer.write_batches(batches)?;
            writer.finish()
        }
        Output::AffectedRows(rows) => {
            Ok(QueryResponseBuilder::with_ok_header().build_with_affected_rows(*rows))
        }
    }
}

/// Writer for encoding multiple [`RecordBatch`]es to the [`SqlQueryResponse`].
///
/// Multiple record batches may be encoded into one batch in the query response
/// to ensure the one batch contains at least `min_rows_per_batch` records.
///
/// Whether to do compression depends on the size of the encoded bytes.
///
/// REQUIRE: Multiple record batches must share the same schema.
pub struct QueryResponseWriter {
    min_rows_per_batch: usize,
    compression_size_threshold: usize,
    encoder: RecordBatchesEncoder,
    encoded_batches: Vec<Vec<u8>>,
}

impl QueryResponseWriter {
    const DEFAULT_ZSTD_LEVEL: i32 = 3;

    pub fn new(min_rows_per_batch: usize, compression_size_threshold: usize) -> Self {
        Self {
            min_rows_per_batch,
            compression_size_threshold,
            encoder: RecordBatchesEncoder::new(Compression::None),
            encoded_batches: Vec::new(),
        }
    }

    pub fn write(&mut self, batch: &RecordBatch) -> Result<()> {
        self.encoder
            .write(batch.as_arrow_record_batch())
            .map_err(|e| Box::new(e) as _)
            .with_context(|| ErrWithCause {
                code: StatusCode::INTERNAL_SERVER_ERROR,
                msg: "failed to encode record batch".to_string(),
            })?;

        if self.encoder.num_rows() < self.min_rows_per_batch {
            Ok(())
        } else {
            let new_encoder = RecordBatchesEncoder::new(Compression::None);
            let old_encoder = mem::replace(&mut self.encoder, new_encoder);
            let encoded_batch = Self::finish_encoder(old_encoder)?;
            self.encoded_batches.push(encoded_batch);
            Ok(())
        }
    }

    pub fn write_batches(&mut self, record_batch: &[RecordBatch]) -> Result<()> {
        self.encoded_batches.reserve(record_batch.len());

        for batch in record_batch {
            self.write(batch)?;
        }

        Ok(())
    }

    pub fn finish(mut self) -> Result<SqlQueryResponse> {
        if self.encoder.num_rows() > 0 {
            let encoder = mem::take(&mut self.encoder);
            self.encoded_batches.push(Self::finish_encoder(encoder)?);
        }

        if self.encoded_batches.is_empty() {
            return Ok(QueryResponseBuilder::with_ok_header().build_with_empty_arrow_payload());
        }

        let compression = self.maybe_do_compression()?;

        let resp = QueryResponseBuilder::with_ok_header().build_with_arrow_payload(ArrowPayload {
            record_batches: self.encoded_batches,
            compression: compression as i32,
        });

        Ok(resp)
    }

    fn maybe_do_compression(&mut self) -> Result<arrow_payload::Compression> {
        let total_size: usize = self.encoded_batches.iter().map(|v| v.len()).sum();
        if total_size > self.compression_size_threshold {
            self.do_compression()?;
            Ok(arrow_payload::Compression::Zstd)
        } else {
            Ok(arrow_payload::Compression::None)
        }
    }

    fn do_compression(&mut self) -> Result<()> {
        let compressed_batches = Vec::with_capacity(self.encoded_batches.len());
        let old_encoded_batches = mem::replace(&mut self.encoded_batches, compressed_batches);
        for encoded_batch in old_encoded_batches {
            let compressed_batch =
                zstd::stream::encode_all(Cursor::new(encoded_batch), Self::DEFAULT_ZSTD_LEVEL)
                    .map_err(|e| Box::new(e) as _)
                    .context(ErrWithCause {
                        code: StatusCode::INTERNAL_SERVER_ERROR,
                        msg: "failed to compress record batch",
                    })?;
            self.encoded_batches.push(compressed_batch);
        }

        Ok(())
    }

    fn finish_encoder(encoder: RecordBatchesEncoder) -> Result<Vec<u8>> {
        encoder
            .finish()
            .map_err(|e| Box::new(e) as _)
            .with_context(|| ErrWithCause {
                code: StatusCode::INTERNAL_SERVER_ERROR,
                msg: "failed to encode record batch".to_string(),
            })
    }
}
