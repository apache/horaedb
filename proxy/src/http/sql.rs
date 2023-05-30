// Copyright 2023 CeresDB Project Authors. Licensed under Apache-2.0.

use std::io::Cursor;

use arrow::{ipc::reader::StreamReader, record_batch::RecordBatch as ArrowRecordBatch};
use ceresdbproto::storage::{
    arrow_payload::Compression, sql_query_response::Output as OutputPb, ArrowPayload,
    SqlQueryResponse,
};
use common_types::{
    datum::{Datum, DatumKind},
    record_batch::{CachedRecordBatchesConverter, RecordBatch},
};
use common_util::error::BoxError;
use interpreters::interpreter::Output;
use query_engine::executor::{Executor as QueryExecutor, RecordBatchVec};
use serde::{
    ser::{SerializeMap, SerializeSeq},
    Deserialize, Serialize,
};
use snafu::{OptionExt, ResultExt};

use crate::{
    context::RequestContext,
    error::{Internal, InternalNoCause, Result},
    read::SqlResponse,
    Context, Proxy,
};

impl<Q: QueryExecutor + 'static> Proxy<Q> {
    pub async fn handle_http_sql_query(
        &self,
        ctx: &RequestContext,
        req: Request,
    ) -> Result<Output> {
        let context = Context {
            timeout: ctx.timeout,
            runtime: self.engine_runtimes.read_runtime.clone(),
            enable_partition_table_access: true,
        };

        match self.handle_sql(context, &ctx.schema, &req.query).await? {
            SqlResponse::Forwarded(resp) => convert_sql_response_to_output(resp),
            SqlResponse::Local(output) => Ok(output),
        }
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

    let mut record_batch_builder = CachedRecordBatchesConverter::default();
    let output = match output_pb {
        OutputPb::AffectedRows(affected) => Output::AffectedRows(affected as usize),
        OutputPb::Arrow(arrow_payload) => {
            let arrow_record_batches = decode_arrow_payload(arrow_payload)?;
            let rows_group: Vec<RecordBatch> = arrow_record_batches
                .into_iter()
                .map(|arrow_record_batch| record_batch_builder.convert(arrow_record_batch))
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
