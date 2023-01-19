// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

//! Query handler

use std::time::Instant;

use arrow_ext::ipc::{Compression, RecordBatchesEncoder};
use ceresdbproto::{
    common::ResponseHeader,
    storage::{
        arrow_payload, sql_query_response::RowsPayload,
        storage_service_client::StorageServiceClient, ArrowPayload, SqlQueryRequest,
        SqlQueryResponse,
    },
};
use common_types::{record_batch::RecordBatch, request_id::RequestId};
use common_util::time::InstantExt;
use futures::FutureExt;
use http::StatusCode;
use interpreters::{context::Context as InterpreterContext, factory::Factory, interpreter::Output};
use log::{error, info, warn};
use query_engine::executor::{Executor as QueryExecutor, RecordBatchVec};
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

fn empty_ok_resp() -> SqlQueryResponse {
    let header = ResponseHeader {
        code: StatusCode::OK.as_u16() as u32,
        ..Default::default()
    };

    SqlQueryResponse {
        header: Some(header),
        ..Default::default()
    }
}

fn make_query_resp_with_arrow_payload(payload: ArrowPayload) -> SqlQueryResponse {
    let header = ResponseHeader {
        code: StatusCode::OK.as_u16() as u32,
        ..Default::default()
    };

    let rows_payload = Some(RowsPayload::Arrow(payload));
    SqlQueryResponse {
        header: Some(header),
        affected_rows: 0,
        rows_payload,
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

    let output_result = fetch_query_output(ctx, &req).await?;
    if let Some(output) = output_result {
        convert_output(
            &output,
            ctx.min_rows_per_batch,
            ctx.datum_compression_threshold,
        )
        .map_err(|e| Box::new(e) as _)
        .with_context(|| ErrWithCause {
            code: StatusCode::INTERNAL_SERVER_ERROR,
            msg: format!("Failed to convert output, query:{}", &req.ql),
        })
    } else {
        Ok(empty_ok_resp())
    }
}

pub async fn fetch_query_output<Q: QueryExecutor + 'static>(
    ctx: &HandlerContext<'_, Q>,
    req: &SqlQueryRequest,
) -> Result<Option<Output>> {
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
        .parse_sql(&mut sql_ctx, &req.ql)
        .map_err(|e| Box::new(e) as _)
        .context(ErrWithCause {
            code: StatusCode::BAD_REQUEST,
            msg: "failed to parse sql",
        })?;

    if stmts.is_empty() {
        return Ok(None);
    }

    // TODO(yingwen): For simplicity, we only support executing one statement now
    // TODO(yingwen): INSERT/UPDATE/DELETE can be batched
    ensure!(
        stmts.len() == 1,
        ErrNoCause {
            code: StatusCode::BAD_REQUEST,
            msg: format!(
                "Only support execute one statement now, current num:{}, query:{}",
                stmts.len(),
                req.ql
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
            msg: format!("Failed to create plan, query:{}", req.ql),
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
        msg: format!("Failed to execute interpreter, query:{}", req.ql),
    })?;

    info!(
        "Grpc handle query success, catalog:{}, schema:{}, request_id:{}, cost:{}ms, request:{:?}",
        ctx.catalog(),
        ctx.schema(),
        request_id,
        begin_instant.saturating_elapsed().as_millis(),
        req,
    );

    Ok(Some(output))
}

// TODO(chenxiang): Output can have both `rows` and `affected_rows`
fn convert_output(
    output: &Output,
    min_rows_per_batch: usize,
    datum_compression_threshold: usize,
) -> Result<SqlQueryResponse> {
    match output {
        Output::Records(records) => {
            convert_records(records, min_rows_per_batch, datum_compression_threshold)
        }
        Output::AffectedRows(rows) => {
            let mut resp = empty_ok_resp();
            resp.affected_rows = *rows as u32;
            Ok(resp)
        }
    }
}

pub fn get_record_batch(op: Option<Output>) -> Option<RecordBatchVec> {
    if let Some(output) = op {
        match output {
            Output::Records(records) => Some(records),
            _ => unreachable!(),
        }
    } else {
        None
    }
}

/// Convert the record batches into the sql query response.
///
/// Multiple record batches will be encoded as one batch in the query response
/// to ensure the one batch contains at least `min_rows_per_batch` records.
///
/// REQUIRE: Record batches have same schema.
pub fn convert_records(
    record_batches: &[RecordBatch],
    min_rows_per_batch: usize,
    datum_compression_threshold: usize,
) -> Result<SqlQueryResponse> {
    if record_batches.is_empty() {
        return Ok(empty_ok_resp());
    }

    let compression = {
        let num_datum = record_batches
            .iter()
            .map(|batch| batch.num_rows() * batch.num_columns())
            .sum::<usize>();
        if num_datum > datum_compression_threshold {
            Compression::Zstd
        } else {
            Compression::None
        }
    };

    let mut encoded_record_batches = Vec::with_capacity(record_batches.len());
    let mut encoder: Option<RecordBatchesEncoder> = None;
    for batch in record_batches {
        let mut enc = if let Some(v) = encoder.take() {
            v
        } else {
            RecordBatchesEncoder::new(compression)
        };

        if enc.num_rows() < min_rows_per_batch {
            enc.write(batch.as_arrow_record_batch())
                .map_err(|e| Box::new(e) as _)
                .with_context(|| ErrWithCause {
                    code: StatusCode::INTERNAL_SERVER_ERROR,
                    msg: "failed to encode record batch".to_string(),
                })?;
            encoder = Some(enc);
        } else {
            let encoded_record_batch =
                enc.finish()
                    .map_err(|e| Box::new(e) as _)
                    .with_context(|| ErrWithCause {
                        code: StatusCode::INTERNAL_SERVER_ERROR,
                        msg: "failed to encode record batch".to_string(),
                    })?;
            encoded_record_batches.push(encoded_record_batch);
        }
    }

    if let Some(enc) = encoder {
        let encoded_record_batch =
            enc.finish()
                .map_err(|e| Box::new(e) as _)
                .with_context(|| ErrWithCause {
                    code: StatusCode::INTERNAL_SERVER_ERROR,
                    msg: "failed to encode record batch".to_string(),
                })?;
        encoded_record_batches.push(encoded_record_batch);
    }

    let pb_compression = match compression {
        Compression::None => arrow_payload::Compression::None,
        Compression::Zstd => arrow_payload::Compression::Zstd,
    };

    let resp = make_query_resp_with_arrow_payload(ArrowPayload {
        record_batches: encoded_record_batches,
        compression: pb_compression as i32,
    });

    Ok(resp)
}
