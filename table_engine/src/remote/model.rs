// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

//! Model for remote table engine

use avro_rs::types::Record;
use common_types::{row::RowGroup, schema::Schema};
use common_util::avro;
use snafu::{Backtrace, OptionExt, ResultExt, Snafu};

use crate::table::{ReadRequest as TableReadRequest, WriteRequest as TableWriteRequest};

const ENCODE_ROWS_WITH_AVRO: u32 = 0;

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

    #[snafu(display("Empty table identifier.\nBacktrace:\n{}", backtrace))]
    EmptyTableIdentifier { backtrace: Backtrace },

    #[snafu(display("Empty table read request.\nBacktrace:\n{}", backtrace))]
    EmptyTableReadRequest { backtrace: Backtrace },

    #[snafu(display("Empty table schema.\nBacktrace:\n{}", backtrace))]
    EmptyTableSchema { backtrace: Backtrace },

    #[snafu(display("Empty row group.\nBacktrace:\n{}", backtrace))]
    EmptyRowGroup { backtrace: Backtrace },

    #[snafu(display("Failed to covert table read request, err:{}", source))]
    ConvertTableReadRequest {
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display("Failed to covert table schema, err:{}", source))]
    ConvertTableSchema {
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display("Failed to covert row group, err:{}", source))]
    ConvertRowGroup {
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display(
        "Failed to covert row group, encoding version:{}.\nBacktrace:\n{}",
        version,
        backtrace
    ))]
    UnsupportedConvertRowGroup { version: u32, backtrace: Backtrace },
}

define_result!(Error);

#[allow(dead_code)]
#[derive(Debug, Clone)]
pub struct TableIdentifier {
    pub catalog: String,
    pub schema: String,
    pub table: String,
}

impl From<proto::remote_engine::TableIdentifier> for TableIdentifier {
    fn from(pb: proto::remote_engine::TableIdentifier) -> Self {
        Self {
            catalog: pb.catalog,
            schema: pb.schema,
            table: pb.table,
        }
    }
}

pub struct ReadRequest {
    pub table: TableIdentifier,
    pub read_request: TableReadRequest,
}

impl TryFrom<proto::remote_engine::ReadRequest> for ReadRequest {
    type Error = Error;

    fn try_from(pb: proto::remote_engine::ReadRequest) -> std::result::Result<Self, Self::Error> {
        let table_identifier = pb.table.context(EmptyTableIdentifier)?;
        let table_read_request = pb.read_request.context(EmptyTableReadRequest)?;
        Ok(Self {
            table: table_identifier.into(),
            read_request: table_read_request
                .try_into()
                .map_err(|e| Box::new(e) as _)
                .context(ConvertTableReadRequest)?,
        })
    }
}

#[allow(dead_code)]
pub struct WriteRequest {
    pub table: TableIdentifier,
    pub write_request: TableWriteRequest,
}

impl TryFrom<proto::remote_engine::WriteRequest> for WriteRequest {
    type Error = Error;

    fn try_from(pb: proto::remote_engine::WriteRequest) -> std::result::Result<Self, Self::Error> {
        let table_identifier = pb.table.context(EmptyTableIdentifier)?;
        let row_group_pb = pb.row_group.context(EmptyRowGroup)?;
        let table_schema: Schema = row_group_pb
            .table_schema
            .context(EmptyTableSchema)?
            .try_into()
            .map_err(|e| Box::new(e) as _)
            .context(ConvertTableSchema)?;
        let row_group = if row_group_pb.version == ENCODE_ROWS_WITH_AVRO {
            avro::avro_rows_to_row_group(table_schema, &row_group_pb.rows)
                .map_err(|e| Box::new(e) as _)
                .context(ConvertRowGroup)?
        } else {
            UnsupportedConvertRowGroup {
                version: row_group_pb.version,
            }
            .fail()?
        };
        Ok(Self {
            table: table_identifier.into(),
            write_request: TableWriteRequest { row_group },
        })
    }
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
        let request_pb = request.read_request.try_into().context(ReadRequestToPb)?;

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
        let row_group_pb = convert_row_group_to_pb(request.write_request.row_group)?;

        Ok(Self {
            table: Some(table_pb),
            row_group: Some(row_group_pb),
        })
    }
}

fn convert_row_group_to_pb(row_group: RowGroup) -> Result<proto::remote_engine::RowGroup> {
    let column_schemas = row_group.schema().columns();
    let avro_schema = avro::columns_to_avro_schema("RemoteEngine", column_schemas);

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
            let avro_value = avro::datum_to_avro_value(column_value, column_schema.is_nullable);
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
        version: ENCODE_ROWS_WITH_AVRO,
        table_schema: Some(row_group.schema().into()),
        rows,
        min_timestamp: row_group.min_timestamp().as_i64(),
        max_timestamp: row_group.max_timestmap().as_i64(),
    })
}
