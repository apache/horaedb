// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

//! SQL request handler

use std::time::Instant;

use arrow::error::Result as ArrowResult;
use common_types::{
    bytes::Bytes,
    datum::{Datum, DatumKind},
    request_id::RequestId,
};
use common_util::time::InstantExt;
use interpreters::{context::Context as InterpreterContext, factory::Factory, interpreter::Output};
use log::info;
use query_engine::executor::RecordBatchVec;
use serde::{
    ser::{SerializeMap, SerializeSeq},
    Serialize,
};
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
    Rows(ResponseRows),
}

pub struct ResponseRows {
    pub column_names: Vec<ResponseColumn>,
    pub data: Vec<Vec<Datum>>,
}

pub struct ResponseColumn {
    pub name: String,
    pub data_type: DatumKind,
}

struct Row<'a>(Vec<(&'a String, &'a Datum)>);

impl<'a> Serialize for Row<'a> {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let rows = &self.0;
        let mut map = serializer.serialize_map(Some(rows.len()))?;
        for (key, value) in rows {
            map.serialize_entry(key, value)?;
        }
        map.end()
    }
}

impl Serialize for ResponseRows {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let total_count = self.data.len();
        let mut seq = serializer.serialize_seq(Some(total_count))?;

        for rows in &self.data {
            let data = rows
                .iter()
                .enumerate()
                .map(|(col_idx, datum)| {
                    let column_name = &self.column_names[col_idx].name;
                    (column_name, datum)
                })
                .collect::<Vec<_>>();
            let row = Row(data);
            seq.serialize_element(&row)?;
        }

        seq.end()
    }
}

impl From<String> for Request {
    fn from(query: String) -> Self {
        Self { query }
    }
}

impl From<Bytes> for Request {
    fn from(bytes: Bytes) -> Self {
        Request::from(String::from_utf8_lossy(&bytes).to_string())
    }
}

pub async fn handle_sql<Q: QueryExecutor + 'static>(
    ctx: RequestContext,
    instance: InstanceRef<Q>,
    request: Request,
) -> Result<Response> {
    let request_id = RequestId::next_id();
    let begin_instant = Instant::now();
    info!(
        "sql handler try to process request, request_id:{}, request:{:?}",
        request_id, request
    );

    // We use tenant as schema
    // TODO(yingwen): Privilege check, cannot access data of other tenant
    // TODO(yingwen): Maybe move MetaProvider to instance
    let provider = CatalogMetaProvider {
        manager: instance.catalog_manager.clone(),
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
        instance.table_manipulator.clone(),
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
        "sql handler finished, request_id:{}, cost:{}ms, request:{:?}",
        request_id,
        begin_instant.saturating_elapsed().as_millis(),
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
    if records.is_empty() {
        return Ok(Response::Rows(ResponseRows {
            column_names: Vec::new(),
            data: Vec::new(),
        }));
    }

    let record_batch = &records[0];
    let num_cols = record_batch.num_columns();
    let num_rows = record_batch.num_rows();
    let schema = record_batch.schema();

    let mut column_names = Vec::with_capacity(num_cols);
    let mut column_data = Vec::with_capacity(num_rows);

    for col_idx in 0..num_cols {
        let column_schema = schema.column(col_idx).clone();
        column_names.push(ResponseColumn {
            name: column_schema.name,
            data_type: column_schema.data_type,
        });
    }

    for row_idx in 0..num_rows {
        let mut row_data = Vec::with_capacity(num_cols);
        for col_idx in 0..num_cols {
            let column = record_batch.column(col_idx);
            let column = column.datum(row_idx);

            row_data.push(column);
        }

        column_data.push(row_data);
    }

    Ok(Response::Rows(ResponseRows {
        column_names,
        data: column_data,
    }))
}
