// Copyright 2023 CeresDB Project Authors. Licensed under Apache-2.0.

use std::{io::Cursor, time::Instant};

use arrow::{ipc::reader::StreamReader, record_batch::RecordBatch as ArrowRecordBatch};
use ceresdbproto::storage::{
    arrow_payload::Compression, sql_query_response::Output as OutputPb, ArrowPayload,
    RequestContext as GrpcRequestContext, SqlQueryRequest, SqlQueryResponse,
};
use common_types::{
    datum::{Datum, DatumKind},
    record_batch::RecordBatch,
    request_id::RequestId,
};
use common_util::{error::BoxError, time::InstantExt};
use http::StatusCode;
use interpreters::interpreter::Output;
use log::info;
use query_engine::executor::{Executor as QueryExecutor, RecordBatchVec};
use query_frontend::{
    frontend,
    frontend::{Context as SqlContext, Frontend},
    provider::CatalogMetaProvider,
};
use serde::{
    ser::{SerializeMap, SerializeSeq},
    Deserialize, Serialize,
};
use snafu::{ensure, OptionExt, ResultExt};

use crate::{
    context::RequestContext,
    handlers::influxdb::InfluxqlRequest,
    proxy::{
        error::{ErrNoCause, ErrWithCause, Internal, InternalNoCause, Result},
        execute_plan,
        forward::ForwardResult,
        Proxy,
    },
};

impl<Q: QueryExecutor + 'static> Proxy<Q> {
    pub async fn handle_query(
        &self,
        ctx: &RequestContext,
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
            manager: self.instance.catalog_manager.clone(),
            default_catalog: &ctx.catalog,
            default_schema: &ctx.schema,
            function_registry: &*self.instance.function_registry,
        };
        let frontend = Frontend::new(provider);
        let mut sql_ctx = SqlContext::new(request_id, deadline);

        let plan = match &query_request {
            QueryRequest::Sql(request) => {
                // Parse sql, frontend error of invalid sql already contains sql
                // TODO(yingwen): Maybe move sql from frontend error to outer error
                let mut stmts = frontend
                    .parse_sql(&mut sql_ctx, &request.query)
                    .box_err()
                    .with_context(|| ErrWithCause {
                        code: StatusCode::BAD_REQUEST,
                        msg: format!("Failed to parse sql, query:{}", request.query),
                    })?;

                if stmts.is_empty() {
                    return Ok(Output::AffectedRows(0));
                }

                // TODO(yingwen): For simplicity, we only support executing one statement now
                // TODO(yingwen): INSERT/UPDATE/DELETE can be batched
                ensure!(
                    stmts.len() == 1,
                    ErrNoCause {
                        code: StatusCode::BAD_REQUEST,
                        msg: format!(
                            "Only support execute one statement now, current num:{}, query:{}.",
                            stmts.len(),
                            request.query
                        ),
                    }
                );

                let sql_query_request = SqlQueryRequest {
                    context: Some(GrpcRequestContext {
                        database: ctx.schema.clone(),
                    }),
                    tables: vec![],
                    sql: request.query.clone(),
                };

                if let Some(resp) = self.maybe_forward_sql_query(&sql_query_request).await? {
                    match resp {
                        ForwardResult::Forwarded(resp) => {
                            return convert_sql_response_to_output(resp?)
                        }
                        ForwardResult::Local => (),
                    }
                };

                // Open partition table if needed.
                let table_name = frontend::parse_table_name(&stmts);
                if let Some(table_name) = table_name {
                    self.maybe_open_partition_table_if_not_exist(
                        &ctx.catalog,
                        &ctx.schema,
                        &table_name,
                    )
                    .await?;
                }

                // Create logical plan
                // Note: Remember to store sql in error when creating logical plan
                frontend
                    .statement_to_plan(&mut sql_ctx, stmts.remove(0))
                    .box_err()
                    .with_context(|| ErrWithCause {
                        code: StatusCode::BAD_REQUEST,
                        msg: format!("Failed to build plan, query:{}", request.query),
                    })?
            }

            QueryRequest::Influxql(request) => {
                let mut stmts = frontend
                    .parse_influxql(&mut sql_ctx, &request.query)
                    .box_err()
                    .with_context(|| ErrWithCause {
                        code: StatusCode::BAD_REQUEST,
                        msg: format!("Failed to parse influxql, query:{}", request.query),
                    })?;

                if stmts.is_empty() {
                    return Ok(Output::AffectedRows(0));
                }

                ensure!(
                    stmts.len() == 1,
                    ErrNoCause {
                        code: StatusCode::BAD_REQUEST,
                        msg: format!(
                            "Only support execute one statement now, current num:{}, query:{}.",
                            stmts.len(),
                            request.query
                        ),
                    }
                );

                frontend
                    .influxql_stmt_to_plan(&mut sql_ctx, stmts.remove(0))
                    .box_err()
                    .with_context(|| ErrWithCause {
                        code: StatusCode::BAD_REQUEST,
                        msg: format!("Failed to build plan, query:{}", request.query),
                    })?
            }
        };

        self.instance
            .limiter
            .try_limit(&plan)
            .box_err()
            .context(ErrWithCause {
                code: StatusCode::INTERNAL_SERVER_ERROR,
                msg: "Query is blocked",
            })?;
        let output = execute_plan(
            request_id,
            &ctx.catalog,
            &ctx.schema,
            self.instance.clone(),
            plan,
            deadline,
        )
        .await?;

        info!(
            "Query handler finished, request_id:{}, cost:{}ms, request:{:?}",
            request_id,
            begin_instant.saturating_elapsed().as_millis(),
            query_request
        );

        Ok(output)
    }
}
#[derive(Debug, Deserialize)]
pub struct Request {
    pub query: String,
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

#[derive(Debug)]
pub enum QueryRequest {
    Sql(Request),
    // TODO: influxql include more parameters, we should add it in later.
    // TODO: remove dead_code after implement influxql with proxy
    #[allow(dead_code)]
    Influxql(InfluxqlRequest),
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

fn convert_sql_response_to_output(sql_query_response: SqlQueryResponse) -> Result<Output> {
    let output_pb = sql_query_response.output.context(InternalNoCause {
        msg: "Output is empty in sql query response".to_string(),
    })?;
    let output = match output_pb {
        OutputPb::AffectedRows(affected) => Output::AffectedRows(affected as usize),
        OutputPb::Arrow(arrow_payload) => {
            let arrow_record_batches = decode_arrow_payload(arrow_payload)?;
            let rows_group: Vec<RecordBatch> = arrow_record_batches
                .into_iter()
                .map(TryInto::<RecordBatch>::try_into)
                .map(|v| {
                    v.box_err().context(Internal {
                        msg: "decode arrow payload",
                    })
                })
                .collect::<Result<Vec<_>>>()?;
            Output::Records(rows_group)
        }
    };

    Ok(output)
}

fn decode_arrow_payload(arrow_payload: ArrowPayload) -> Result<Vec<ArrowRecordBatch>> {
    let compression = arrow_payload.compression();
    let byte_batches = arrow_payload.record_batches;

    // Maybe unzip payload bytes firstly.
    let unzip_byte_batches = byte_batches
        .into_iter()
        .map(|bytes_batch| match compression {
            Compression::None => Ok(bytes_batch),
            Compression::Zstd => zstd::stream::decode_all(Cursor::new(bytes_batch))
                .box_err()
                .context(Internal {
                    msg: "decode arrow payload",
                }),
        })
        .collect::<Result<Vec<Vec<u8>>>>()?;

    // Decode the byte batches to record batches, multiple record batches may be
    // included in one byte batch.
    let record_batches_group = unzip_byte_batches
        .into_iter()
        .map(|byte_batch| {
            // Decode bytes to `RecordBatch`.
            let stream_reader = match StreamReader::try_new(Cursor::new(byte_batch), None)
                .box_err()
                .context(Internal {
                    msg: "decode arrow payload",
                }) {
                Ok(reader) => reader,
                Err(e) => return Err(e),
            };

            stream_reader
                .into_iter()
                .map(|decode_result| {
                    decode_result.box_err().context(Internal {
                        msg: "decode arrow payload",
                    })
                })
                .collect::<Result<Vec<_>>>()
        })
        .collect::<Result<Vec<Vec<_>>>>()?;

    let record_batches = record_batches_group
        .into_iter()
        .flatten()
        .collect::<Vec<_>>();

    Ok(record_batches)
}
