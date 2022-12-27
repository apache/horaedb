// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

//! Model for remote table engine

use avro_rs::types::Record;
use common_types::row::RowGroup;
use common_util::avro_util;
use snafu::{Backtrace, OptionExt, ResultExt, Snafu};

use crate::table::{ReadRequest as TableReadRequest, WriteRequest as TableWriteRequest};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Failed to convert read request to pb, err:{}", source))]
    ReadRequestToPb { source: crate::table::Error },

    #[snafu(display(
        "Failed to convert write request to pb, msg:{}.\nBacktrace:\n{}",
        msg,
        backtrace
    ))]
    WriteRequestToPbNoCause { msg: String, backtrace: Backtrace },

    #[snafu(display("Failed to convert write request to pb, msg:{}, err:{}", msg, source))]
    WriteRequestToPbWithCause {
        msg: String,
        source: Box<dyn std::error::Error + Send + Sync>,
    },
}

define_result!(Error);

#[allow(dead_code)]
#[derive(Debug, Clone)]
pub struct TableIdentifier {
    pub catalog: String,
    pub schema: String,
    pub table: String,
}

#[allow(dead_code)]
pub struct ReadRequest {
    pub table: TableIdentifier,
    pub table_request: TableReadRequest,
}

#[allow(dead_code)]
pub struct WriteRequest {
    pub table: TableIdentifier,
    pub table_request: TableWriteRequest,
}

impl From<TableIdentifier> for proto::remote_engine::TableIdentifier {
    fn from(table_ident: TableIdentifier) -> Self {
        Self {
            catalog: table_ident.catalog,
            schema: table_ident.schema,
            table: table_ident.table,
        }
    }
}

impl TryFrom<ReadRequest> for proto::remote_engine::ReadRequest {
    type Error = Error;

    fn try_from(request: ReadRequest) -> std::result::Result<Self, Self::Error> {
        let table_pb = request.table.into();
        let request_pb = request.table_request.try_into().context(ReadRequestToPb)?;

        Ok(Self {
            table: Some(table_pb),
            read_request: Some(request_pb),
        })
    }
}

impl TryFrom<WriteRequest> for proto::remote_engine::WriteRequest {
    type Error = Error;

    fn try_from(request: WriteRequest) -> std::result::Result<Self, Self::Error> {
        let table_pb = request.table.into();
        let row_group_pb = convert_row_group_to_pb(request.table_request.row_group)?;

        Ok(Self {
            table: Some(table_pb),
            row_group: Some(row_group_pb),
        })
    }
}

fn convert_row_group_to_pb(row_group: RowGroup) -> Result<proto::remote_engine::RowGroup> {
    let column_schemas = row_group.schema().columns();
    let avro_schema = avro_util::columns_to_avro_schema("RemoteEngine", column_schemas);

    let mut rows = Vec::with_capacity(row_group.num_rows());
    let row_len = row_group.num_rows();
    for row_idx in 0..row_len {
        // Convert `Row` to `Record` in avro.
        let row = row_group.get_row(row_idx).unwrap();
        let mut avro_record =
            Record::new(&avro_schema).with_context(|| WriteRequestToPbNoCause {
                msg: format!(
                    "new avro record with schema failed, schema:{:?}",
                    avro_schema
                ),
            })?;

        for (col_idx, column_schema) in column_schemas.iter().enumerate() {
            let column_value = row[col_idx].clone();
            let avro_value = avro_util::datum_to_value(column_value, column_schema.is_nullable);
            avro_record.put(&column_schema.name, avro_value);
        }

        let row_bytes = avro_rs::to_avro_datum(&avro_schema, avro_record)
            .map_err(|e| Box::new(e) as _)
            .context(WriteRequestToPbWithCause {
                msg: format!(
                    "new avro record with schema failed, schema:{:?}",
                    avro_schema
                ),
            })?;
        rows.push(row_bytes);
    }

    Ok(proto::remote_engine::RowGroup {
        table_schema: Some(row_group.schema().into()),
        rows,
        min_timestamp: row_group.min_timestamp().as_i64(),
        max_timestamp: row_group.max_timestmap().as_i64(),
    })
}
