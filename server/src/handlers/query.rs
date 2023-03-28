// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

//! SQL request handler

use std::time::Instant;

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
use snafu::{ensure, ResultExt};
use sql::{
    frontend::{Context as SqlContext, Frontend},
    provider::CatalogMetaProvider,
};

use crate::handlers::{
    error::{
        CreatePlan, InterpreterExec, ParseInfluxql, ParseSql, QueryBlock, QueryTimeout, TooMuchStmt,
    },
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

#[derive(Debug)]
pub enum QueryRequest {
    Sql(Request),
    // TODO: influxql include more parameters, we should add it in later.
    Influxql(Request),
}
impl QueryRequest {
    pub fn query(&self) -> &str {
        match self {
            QueryRequest::Sql(request) => request.query.as_str(),
            QueryRequest::Influxql(request) => request.query.as_str(),
        }
    }
}

pub async fn handle_query<Q: QueryExecutor + 'static>(
    ctx: &RequestContext,
    instance: InstanceRef<Q>,
    query_request: QueryRequest,
) -> Result<Output> {
    let request_id = RequestId::next_id();
    let begin_instant = Instant::now();
    let deadline = ctx.timeout.map(|t| begin_instant + t);

    info!(
        "Query handler try to process request, request_id:{}, request:{:?}",
        request_id, query_request
    );

    // TODO(yingwen): Privilege check, cannot access data of other tenant
    // TODO(yingwen): Maybe move MetaProvider to instance
    let provider = CatalogMetaProvider {
        manager: instance.catalog_manager.clone(),
        default_catalog: &ctx.catalog,
        default_schema: &ctx.schema,
        function_registry: &*instance.function_registry,
    };
    let frontend = Frontend::new(provider);
    let mut sql_ctx = SqlContext::new(request_id, deadline);

    let plan = match &query_request {
        QueryRequest::Sql(request) => {
            // Parse sql, frontend error of invalid sql already contains sql
            // TODO(yingwen): Maybe move sql from frontend error to outer error
            let mut stmts = frontend
                .parse_sql(&mut sql_ctx, &request.query)
                .context(ParseSql)?;

            if stmts.is_empty() {
                return Ok(Output::AffectedRows(0));
            }

            // TODO(yingwen): For simplicity, we only support executing one statement now
            // TODO(yingwen): INSERT/UPDATE/DELETE can be batched
            ensure!(
                stmts.len() == 1,
                TooMuchStmt {
                    len: stmts.len(),
                    query: &request.query,
                }
            );

            // Create logical plan
            // Note: Remember to store sql in error when creating logical plan
            frontend
                .statement_to_plan(&mut sql_ctx, stmts.remove(0))
                .context(CreatePlan {
                    query: &request.query,
                })?
        }

        QueryRequest::Influxql(request) => {
            let mut stmts = frontend
                .parse_influxql(&mut sql_ctx, &request.query)
                .context(ParseInfluxql)?;

            if stmts.is_empty() {
                return Ok(Output::AffectedRows(0));
            }

            ensure!(
                stmts.len() == 1,
                TooMuchStmt {
                    len: stmts.len(),
                    query: &request.query,
                }
            );

            frontend
                .influxql_stmt_to_plan(&mut sql_ctx, stmts.remove(0))
                .context(CreatePlan {
                    query: &request.query,
                })?
        }
    };

    instance.limiter.try_limit(&plan).context(QueryBlock {
        query: query_request.query(),
    })?;

    // Execute in interpreter
    let interpreter_ctx = InterpreterContext::builder(request_id, deadline)
        // Use current ctx's catalog and tenant as default catalog and tenant
        .default_catalog_and_schema(ctx.catalog.to_string(), ctx.schema.to_string())
        .enable_partition_table_access(ctx.enable_partition_table_access)
        .build();
    let interpreter_factory = Factory::new(
        instance.query_executor.clone(),
        instance.catalog_manager.clone(),
        instance.table_engine.clone(),
        instance.table_manipulator.clone(),
    );
    let interpreter =
        interpreter_factory
            .create(interpreter_ctx, plan)
            .context(InterpreterExec {
                query: query_request.query(),
            })?;

    let output = if let Some(deadline) = deadline {
        tokio::time::timeout_at(
            tokio::time::Instant::from_std(deadline),
            interpreter.execute(),
        )
        .await
        .context(QueryTimeout {
            query: query_request.query(),
        })
        .and_then(|v| {
            v.context(InterpreterExec {
                query: query_request.query(),
            })
        })?
    } else {
        interpreter.execute().await.context(InterpreterExec {
            query: query_request.query(),
        })?
    };

    info!(
        "Query handler finished, request_id:{}, cost:{}ms, request:{:?}",
        request_id,
        begin_instant.saturating_elapsed().as_millis(),
        query_request
    );

    Ok(output)
}

// Convert output to json
pub fn convert_output(output: Output) -> Response {
    match output {
        Output::AffectedRows(n) => Response::AffectedRows(n),
        Output::Records(records) => convert_records(records),
    }
}

fn convert_records(records: RecordBatchVec) -> Response {
    if records.is_empty() {
        return Response::Rows(ResponseRows {
            column_names: Vec::new(),
            data: Vec::new(),
        });
    }

    let mut column_names = vec![];
    let mut column_data = vec![];

    for record_batch in records {
        let num_cols = record_batch.num_columns();
        let num_rows = record_batch.num_rows();
        let schema = record_batch.schema();

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
    }

    Response::Rows(ResponseRows {
        column_names,
        data: column_data,
    })
}
