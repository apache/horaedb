// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

//! Query handler

use std::time::Instant;

use ceresdbproto::{
    common::ResponseHeader,
    storage::{
        query_response, storage_service_client::StorageServiceClient, QueryRequest, QueryResponse,
    },
};
use common_types::{record_batch::RecordBatch, request_id::RequestId};
use common_util::{avro, time::InstantExt};
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

/// Schema name of the record
const RECORD_NAME: &str = "Result";

fn empty_ok_resp() -> QueryResponse {
    let header = ResponseHeader {
        code: StatusCode::OK.as_u16() as u32,
        ..Default::default()
    };

    QueryResponse {
        header: Some(header),
        ..Default::default()
    }
}

async fn maybe_forward_query<Q: QueryExecutor + 'static>(
    ctx: &HandlerContext<'_, Q>,
    req: &QueryRequest,
) -> Option<Result<QueryResponse>> {
    let forwarder = ctx.forwarder.as_ref()?;

    if req.metrics.len() != 1 {
        warn!(
            "Unable to forward query without exactly one metric, req:{:?}",
            req
        );

        return None;
    }

    let forward_req = ForwardRequest {
        schema: ctx.schema.clone(),
        metric: req.metrics[0].clone(),
        req: req.clone().into_request(),
    };
    let do_query = |mut client: StorageServiceClient<Channel>,
                    request: tonic::Request<QueryRequest>,
                    _: &Endpoint| {
        let query = async move {
            client
                .query(request)
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
    req: QueryRequest,
) -> Result<QueryResponse> {
    let req = match maybe_forward_query(ctx, &req).await {
        Some(resp) => return resp,
        None => req,
    };

    let output_result = fetch_query_output(ctx, &req).await?;
    if let Some(output) = output_result {
        convert_output(&output)
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
    req: &QueryRequest,
) -> Result<Option<Output>> {
    let request_id = RequestId::next_id();
    let begin_instant = Instant::now();
    let deadline = begin_instant + ctx.timeout;

    info!(
        "Grpc handle query begin, catalog:{}, tenant:{}, request_id:{}, request:{:?}",
        ctx.catalog(),
        ctx.tenant(),
        request_id,
        req,
    );

    let instance = &ctx.instance;
    // We use tenant as schema
    // TODO(yingwen): Privilege check, cannot access data of other tenant
    // TODO(yingwen): Maybe move MetaProvider to instance
    let provider = CatalogMetaProvider {
        manager: instance.catalog_manager.clone(),
        default_catalog: ctx.catalog(),
        default_schema: ctx.tenant(),
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

    if Instant::now() >= deadline {
        return ErrNoCause {
            code: StatusCode::REQUEST_TIMEOUT,
            msg: "Query timeout",
        }
        .fail();
    }

    // Execute in interpreter
    let interpreter_ctx = InterpreterContext::builder(request_id, deadline)
        // Use current ctx's catalog and tenant as default catalog and tenant
        .default_catalog_and_schema(ctx.catalog().to_string(), ctx.tenant().to_string())
        .build();
    let interpreter_factory = Factory::new(
        instance.query_executor.clone(),
        instance.catalog_manager.clone(),
        instance.table_engine.clone(),
        instance.table_manipulator.clone(),
    );
    let interpreter = interpreter_factory.create(interpreter_ctx, plan);

    let output = interpreter
        .execute()
        .await
        .map_err(|e| Box::new(e) as _)
        .with_context(|| ErrWithCause {
            code: StatusCode::INTERNAL_SERVER_ERROR,
            msg: format!("Failed to execute interpreter, query:{}", req.ql),
        })?;

    info!(
        "Grpc handle query success, catalog:{}, tenant:{}, request_id:{}, cost:{}ms, request:{:?}",
        ctx.catalog(),
        ctx.tenant(),
        request_id,
        begin_instant.saturating_elapsed().as_millis(),
        req,
    );

    Ok(Some(output))
}

// TODO(chenxiang): Output can have both `rows` and `affected_rows`
fn convert_output(output: &Output) -> Result<QueryResponse> {
    match output {
        Output::Records(records) => convert_records(records),
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

/// REQUIRE: records have same schema
pub fn convert_records(records: &[RecordBatch]) -> Result<QueryResponse> {
    if records.is_empty() {
        return Ok(empty_ok_resp());
    }

    let mut resp = empty_ok_resp();
    let mut avro_schema_opt = None;

    let total_row = records.iter().map(|v| v.num_rows()).sum();
    resp.rows = Vec::with_capacity(total_row);
    for record_batch in records {
        if avro_schema_opt.as_ref().is_none() {
            let avro_schema = avro::to_avro_schema(RECORD_NAME, record_batch.schema());

            // We only set schema_json once, so all record batches need to have same schema
            resp.schema_type = query_response::SchemaType::Avro as i32;
            resp.schema_content = avro_schema.canonical_form();

            avro_schema_opt = Some(avro_schema);
        }

        let mut rows = avro::record_batch_to_avro_rows(record_batch)
            .map_err(|e| Box::new(e) as _)
            .context(ErrWithCause {
                code: StatusCode::INTERNAL_SERVER_ERROR,
                msg: "failed to convert record batch",
            })?;
        resp.rows.append(&mut rows);
    }

    Ok(resp)
}
