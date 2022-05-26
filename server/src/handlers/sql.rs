// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

//! SQL request handler

use std::collections::HashMap;

use arrow_deps::arrow::error::Result as ArrowResult;
use common_types::{datum::Datum, request_id::RequestId};
use interpreters::{context::Context as InterpreterContext, factory::Factory, interpreter::Output};
use log::info;
use query_engine::executor::RecordBatchVec;
use serde_derive::Serialize;
use snafu::ensure;
use sql::{
    frontend::{Context as SqlContext, Frontend},
    provider::CatalogMetaProvider,
};

use crate::handlers::{
    error::{ArrowToString, CreatePlan, InterpreterExec, ParseSql, TooMuchStmt},
    prelude::*,
};

#[derive(Debug, Deserialize)]
pub struct Request {
    query: String,
}

// TODO(yingwen): Improve serialize performance
#[derive(Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Response {
    AffectedRows(usize),
    Rows(Vec<HashMap<String, Datum>>),
}

pub async fn handle_sql<C: CatalogManager + 'static, Q: QueryExecutor + 'static>(
    ctx: RequestContext,
    instance: InstanceRef<C, Q>,
    request: Request,
) -> Result<Response> {
    let request_id = RequestId::next_id();

    info!(
        "sql handler try to process request, request_id:{}, request:{:?}",
        request_id, request
    );

    // We use tenant as schema
    // TODO(yingwen): Privilege check, cannot access data of other tenant
    // TODO(yingwen): Maybe move MetaProvider to instance
    let provider = CatalogMetaProvider {
        manager: &instance.catalog_manager,
        default_catalog: &ctx.catalog,
        default_schema: &ctx.tenant,
        function_registry: &*instance.function_registry,
    };
    let frontend = Frontend::new(provider);

    let mut sql_ctx = SqlContext::new(request_id);
    // Parse sql, frontend error of invalid sql already contains sql
    // TODO(yingwen): Maybe move sql from frontend error to outer error
    let mut stmts = frontend
        .parse_sql(&mut sql_ctx, &request.query)
        .context(ParseSql)?;

    if stmts.is_empty() {
        return Ok(Response::AffectedRows(0));
    }

    // TODO(yingwen): For simplicity, we only support executing one statement now
    // TODO(yingwen): INSERT/UPDATE/DELETE can be batched
    ensure!(
        stmts.len() == 1,
        TooMuchStmt {
            len: stmts.len(),
            query: request.query,
        }
    );

    // Create logical plan
    // Note: Remember to store sql in error when creating logical plan
    let plan = frontend
        .statement_to_plan(&mut sql_ctx, stmts.remove(0))
        .context(CreatePlan {
            query: &request.query,
        })?;

    // Execute in interpreter
    let interpreter_ctx = InterpreterContext::builder(request_id)
        // Use current ctx's catalog and tenant as default catalog and tenant
        .default_catalog_and_schema(ctx.catalog, ctx.tenant)
        .build();
    let interpreter_factory = Factory::new(
        instance.query_executor.clone(),
        instance.catalog_manager.clone(),
        instance.table_engine.clone(),
    );
    let interpreter = interpreter_factory.create(interpreter_ctx, plan);

    let output = interpreter.execute().await.context(InterpreterExec {
        query: &request.query,
    })?;

    // Convert output to json
    let resp = convert_output(output).context(ArrowToString {
        query: &request.query,
    })?;

    info!(
        "sql handler finished processing request, request:{:?}",
        request
    );

    Ok(resp)
}

fn convert_output(output: Output) -> ArrowResult<Response> {
    match output {
        Output::AffectedRows(n) => Ok(Response::AffectedRows(n)),
        Output::Records(records) => convert_records(records),
    }
}

fn convert_records(records: RecordBatchVec) -> ArrowResult<Response> {
    let total_rows = records.iter().map(|v| v.num_rows()).sum();
    let mut resp = Vec::with_capacity(total_rows);
    for record_batch in records {
        let num_cols = record_batch.num_columns();
        let num_rows = record_batch.num_rows();
        let schema = record_batch.schema();

        for row_idx in 0..num_rows {
            let mut row = HashMap::with_capacity(num_cols);
            for col_idx in 0..num_cols {
                let column = record_batch.column(col_idx);
                let column = column.datum(row_idx);

                let column_name = schema.column(col_idx).name.clone();
                row.insert(column_name, column);
            }

            resp.push(row);
        }
    }

    Ok(Response::Rows(resp))
}
