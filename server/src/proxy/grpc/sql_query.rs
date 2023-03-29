// Copyright 2023 CeresDB Project Authors. Licensed under Apache-2.0.

//! Query handler

use std::{sync::Arc, time::Instant};

use arrow_ext::ipc::{CompressOptions, CompressionMethod, RecordBatchesEncoder};
use ceresdbproto::{
    common::ResponseHeader,
    storage::{
        arrow_payload, sql_query_response, storage_service_client::StorageServiceClient,
        ArrowPayload, SqlQueryRequest, SqlQueryResponse,
    },
};
use common_types::{record_batch::RecordBatch, request_id::RequestId};
use common_util::{error::BoxError, time::InstantExt};
use futures::{stream, stream::BoxStream, FutureExt, StreamExt};
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
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{transport::Channel, IntoRequest};

use crate::proxy::{
    error::{self, ErrNoCause, ErrWithCause, Error, Result},
    forward::{ForwardRequest, ForwardResult},
    Context, Proxy,
};

const STREAM_QUERY_CHANNEL_LEN: usize = 20;

impl<Q: QueryExecutor + 'static> Proxy<Q> {
    pub async fn handle_sql_query(&self, ctx: Context, req: SqlQueryRequest) -> SqlQueryResponse {
        self.hotspot_recorder.inc_sql_query_reqs(&req);
        match self.handle_sql_query_internal(ctx, req).await {
            Err(e) => {
                error!("Failed to handle sql query, err:{e}");
                SqlQueryResponse {
                    header: Some(error::build_err_header(e)),
                    ..Default::default()
                }
            }
            Ok(v) => v,
        }
    }

    pub async fn handle_stream_sql_query(
        self: Arc<Self>,
        ctx: Context,
        req: SqlQueryRequest,
    ) -> BoxStream<'static, SqlQueryResponse> {
        self.hotspot_recorder.inc_sql_query_reqs(&req);
        match self.clone().handle_stream_query_internal(ctx, req).await {
            Err(e) => stream::once(async {
                error!("Failed to handle stream sql query, err:{e}");
                SqlQueryResponse {
                    header: Some(error::build_err_header(e)),
                    ..Default::default()
                }
            })
            .boxed(),
            Ok(v) => v,
        }
    }

    async fn handle_sql_query_internal(
        &self,
        ctx: Context,
        req: SqlQueryRequest,
    ) -> Result<SqlQueryResponse> {
        let req = match self.maybe_forward_sql_query(&req).await {
            Some(resp) => return resp,
            None => req,
        };

        let output = self.fetch_sql_query_output(ctx, &req).await?;
        convert_output(&output, self.resp_compress_min_length)
    }

    async fn handle_stream_query_internal(
        self: Arc<Self>,
        ctx: Context,
        req: SqlQueryRequest,
    ) -> Result<BoxStream<'static, SqlQueryResponse>> {
        let req = match self
            .clone()
            .maybe_forward_stream_sql_query(req.clone())
            .await
        {
            Some(resp) => return resp,
            None => req,
        };
        let (tx, rx) = mpsc::channel(STREAM_QUERY_CHANNEL_LEN);
        let runtime = ctx.runtime.clone();
        let resp_compress_min_length = self.resp_compress_min_length;
        let output = self.as_ref().fetch_sql_query_output(ctx, &req).await?;
        runtime.spawn(async move {
            match output {
                Output::AffectedRows(rows) => {
                    let resp =
                        QueryResponseBuilder::with_ok_header().build_with_affected_rows(rows);
                    if tx.send(resp).await.is_err() {
                        error!("Failed to send affected rows resp in stream sql query");
                    }
                }
                Output::Records(batches) => {
                    for batch in &batches {
                        let resp = {
                            let mut writer = QueryResponseWriter::new(resp_compress_min_length);
                            writer.write(batch)?;
                            writer.finish()
                        }?;

                        if tx.send(resp).await.is_err() {
                            error!("Failed to send record batches resp in stream sql query");
                            break;
                        }
                    }
                }
            }
            Ok::<(), Error>(())
        });
        Ok(ReceiverStream::new(rx).boxed())
    }

    async fn maybe_forward_sql_query(
        &self,
        req: &SqlQueryRequest,
    ) -> Option<Result<SqlQueryResponse>> {
        if req.tables.len() != 1 {
            warn!("Unable to forward sql query without exactly one table, req:{req:?}",);

            return None;
        }

        let req_ctx = req.context.as_ref().unwrap();
        let forward_req = ForwardRequest {
            schema: req_ctx.database.clone(),
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
                    .box_err()
                    .context(ErrWithCause {
                        code: StatusCode::INTERNAL_SERVER_ERROR,
                        msg: "Forwarded sql query failed",
                    })
            }
            .boxed();

            Box::new(query) as _
        };

        match self.forwarder.forward(forward_req, do_query).await {
            Ok(forward_res) => match forward_res {
                ForwardResult::Forwarded(v) => Some(v),
                ForwardResult::Original => None,
            },
            Err(e) => {
                error!("Failed to forward sql req but the error is ignored, err:{e}");
                None
            }
        }
    }

    async fn maybe_forward_stream_sql_query(
        self: Arc<Self>,
        req: SqlQueryRequest,
    ) -> Option<Result<BoxStream<'static, SqlQueryResponse>>> {
        if req.tables.len() != 1 {
            warn!("Unable to forward sql query without exactly one table, req:{req:?}",);

            return None;
        }

        let req_ctx = req.context.as_ref().unwrap();
        let forward_req = ForwardRequest {
            schema: req_ctx.database.clone(),
            table: req.tables[0].clone(),
            req: req.clone().into_request(),
        };
        let do_query = |mut client: StorageServiceClient<Channel>,
                        request: tonic::Request<SqlQueryRequest>,
                        _: &Endpoint| {
            let query = async move {
                client
                    .stream_sql_query(request)
                    .await
                    .map(|resp| resp.into_inner().boxed())
                    .box_err()
                    .context(ErrWithCause {
                        code: StatusCode::INTERNAL_SERVER_ERROR,
                        msg: "Forwarded stream sql query failed",
                    })
                    .map(|stream| {
                        stream
                            .map(|item| {
                                item.box_err()
                                    .context(ErrWithCause {
                                        code: StatusCode::INTERNAL_SERVER_ERROR,
                                        msg: "Fail to fetch stream sql query response",
                                    })
                                    .unwrap_or_else(|e| SqlQueryResponse {
                                        header: Some(error::build_err_header(e)),
                                        ..Default::default()
                                    })
                            })
                            .boxed()
                    })
            }
            .boxed();

            Box::new(query) as _
        };

        match self.forwarder.forward(forward_req, do_query).await {
            Ok(forward_res) => match forward_res {
                ForwardResult::Forwarded(v) => Some(v),
                ForwardResult::Original => None,
            },
            Err(e) => {
                error!("Failed to forward stream sql req but the error is ignored, err:{e}");
                None
            }
        }
    }

    async fn fetch_sql_query_output(&self, ctx: Context, req: &SqlQueryRequest) -> Result<Output> {
        let request_id = RequestId::next_id();
        let begin_instant = Instant::now();
        let deadline = ctx.timeout.map(|t| begin_instant + t);
        let catalog = self.instance.catalog_manager.default_catalog_name();

        info!("Handle sql query, request_id:{request_id}, request:{req:?}");

        let req_ctx = req.context.as_ref().unwrap();
        let schema = &req_ctx.database;
        let instance = &self.instance;
        // TODO(yingwen): Privilege check, cannot access data of other tenant
        // TODO(yingwen): Maybe move MetaProvider to instance
        let provider = CatalogMetaProvider {
            manager: instance.catalog_manager.clone(),
            default_catalog: catalog,
            default_schema: schema,
            function_registry: &*instance.function_registry,
        };
        let frontend = Frontend::new(provider);

        let mut sql_ctx = SqlContext::new(request_id, deadline);
        // Parse sql, frontend error of invalid sql already contains sql
        // TODO(yingwen): Maybe move sql from frontend error to outer error
        let mut stmts = frontend
            .parse_sql(&mut sql_ctx, &req.sql)
            .box_err()
            .context(ErrWithCause {
                code: StatusCode::BAD_REQUEST,
                msg: "Failed to parse sql",
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
            .box_err()
            .with_context(|| ErrWithCause {
                code: StatusCode::INTERNAL_SERVER_ERROR,
                msg: format!("Failed to create plan, query:{}", req.sql),
            })?;

        instance
            .limiter
            .try_limit(&plan)
            .box_err()
            .context(ErrWithCause {
                code: StatusCode::INTERNAL_SERVER_ERROR,
                msg: "Query is blocked",
            })?;

        if let Some(deadline) = deadline {
            if deadline.check_deadline() {
                return ErrNoCause {
                    code: StatusCode::INTERNAL_SERVER_ERROR,
                    msg: "Query timeout",
                }
                .fail();
            }
        }

        // Execute in interpreter
        let interpreter_ctx = InterpreterContext::builder(request_id, deadline)
            // Use current ctx's catalog and schema as default catalog and schema
            .default_catalog_and_schema(catalog.to_string(), schema.to_string())
            .build();
        let interpreter_factory = Factory::new(
            instance.query_executor.clone(),
            instance.catalog_manager.clone(),
            instance.table_engine.clone(),
            instance.table_manipulator.clone(),
        );
        let interpreter = interpreter_factory
            .create(interpreter_ctx, plan)
            .box_err()
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
            .box_err()
            .context(ErrWithCause {
                code: StatusCode::INTERNAL_SERVER_ERROR,
                msg: "Query timeout",
            })?
        } else {
            interpreter.execute().await
        }
        .box_err()
        .with_context(|| ErrWithCause {
            code: StatusCode::INTERNAL_SERVER_ERROR,
            msg: format!("Failed to execute interpreter, sql:{}", req.sql),
        })?;

        info!(
        "Handle sql query success, catalog:{}, schema:{}, request_id:{}, cost:{}ms, request:{:?}",
        catalog,
        schema,
        request_id,
        begin_instant.saturating_elapsed().as_millis(),
        req,
    );

        Ok(output)
    }
}

// TODO(chenxiang): Output can have both `rows` and `affected_rows`
pub fn convert_output(
    output: &Output,
    resp_compress_min_length: usize,
) -> Result<SqlQueryResponse> {
    match output {
        Output::Records(batches) => {
            let mut writer = QueryResponseWriter::new(resp_compress_min_length);
            writer.write_batches(batches)?;
            writer.finish()
        }
        Output::AffectedRows(rows) => {
            Ok(QueryResponseBuilder::with_ok_header().build_with_affected_rows(*rows))
        }
    }
}

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

/// Writer for encoding multiple [`RecordBatch`]es to the [`SqlQueryResponse`].
///
/// Whether to do compression depends on the size of the encoded bytes.
pub struct QueryResponseWriter {
    encoder: RecordBatchesEncoder,
}

impl QueryResponseWriter {
    pub fn new(compress_min_length: usize) -> Self {
        let compress_opts = CompressOptions {
            compress_min_length,
            method: CompressionMethod::Zstd,
        };
        Self {
            encoder: RecordBatchesEncoder::new(compress_opts),
        }
    }

    pub fn write(&mut self, batch: &RecordBatch) -> Result<()> {
        self.encoder
            .write(batch.as_arrow_record_batch())
            .box_err()
            .context(ErrWithCause {
                code: StatusCode::INTERNAL_SERVER_ERROR,
                msg: "Failed to encode record batch",
            })
    }

    pub fn write_batches(&mut self, record_batch: &[RecordBatch]) -> Result<()> {
        for batch in record_batch {
            self.write(batch)?;
        }

        Ok(())
    }

    pub fn finish(self) -> Result<SqlQueryResponse> {
        let compress_output = self.encoder.finish().box_err().context(ErrWithCause {
            code: StatusCode::INTERNAL_SERVER_ERROR,
            msg: "Failed to encode record batch",
        })?;

        if compress_output.payload.is_empty() {
            return Ok(QueryResponseBuilder::with_ok_header().build_with_empty_arrow_payload());
        }

        let compression = match compress_output.method {
            CompressionMethod::None => arrow_payload::Compression::None,
            CompressionMethod::Zstd => arrow_payload::Compression::Zstd,
        };
        let resp = QueryResponseBuilder::with_ok_header().build_with_arrow_payload(ArrowPayload {
            record_batches: vec![compress_output.payload],
            compression: compression as i32,
        });

        Ok(resp)
    }
}
