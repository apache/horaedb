// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

//! Model for remote table engine

use arrow_ext::{
    ipc,
    ipc::{CompressOptions, CompressionMethod},
};
use ceresdbproto::{
    remote_engine::{row_group, row_group::Rows::Arrow},
    storage::{arrow_payload, ArrowPayload},
};
use common_types::{
    record_batch::{RecordBatch, RecordBatchWithKeyBuilder},
    row::RowGroupBuilder,
};
use common_util::error::{BoxError, GenericError};
use snafu::{ensure, Backtrace, OptionExt, ResultExt, Snafu};

use crate::table::{ReadRequest as TableReadRequest, WriteRequest as TableWriteRequest};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Failed to convert read request to pb, err:{}", source))]
    ReadRequestToPb { source: crate::table::Error },

    #[snafu(display(
        "Failed to convert write request to pb, table_ident:{:?}, msg:{}.\nBacktrace:\n{}",
        table_ident,
        msg,
        backtrace
    ))]
    WriteRequestToPbNoCause {
        table_ident: TableIdentifier,
        msg: String,
        backtrace: Backtrace,
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
    ConvertTableReadRequest { source: GenericError },

    #[snafu(display("Failed to covert table schema, err:{}", source))]
    ConvertTableSchema { source: GenericError },

    #[snafu(display("Failed to covert row group, err:{}", source))]
    ConvertRowGroup { source: GenericError },
}

define_result!(Error);

#[derive(Debug, Clone, Hash, Eq, PartialEq)]
pub struct TableIdentifier {
    pub catalog: String,
    pub schema: String,
    pub table: String,
}

impl From<ceresdbproto::remote_engine::TableIdentifier> for TableIdentifier {
    fn from(pb: ceresdbproto::remote_engine::TableIdentifier) -> Self {
        Self {
            catalog: pb.catalog,
            schema: pb.schema,
            table: pb.table,
        }
    }
}

impl From<TableIdentifier> for ceresdbproto::remote_engine::TableIdentifier {
    fn from(table_ident: TableIdentifier) -> Self {
        Self {
            catalog: table_ident.catalog,
            schema: table_ident.schema,
            table: table_ident.table,
        }
    }
}

pub struct ReadRequest {
    pub table: TableIdentifier,
    pub read_request: TableReadRequest,
}

impl TryFrom<ceresdbproto::remote_engine::ReadRequest> for ReadRequest {
    type Error = Error;

    fn try_from(
        pb: ceresdbproto::remote_engine::ReadRequest,
    ) -> std::result::Result<Self, Self::Error> {
        let table_identifier = pb.table.context(EmptyTableIdentifier)?;
        let table_read_request = pb.read_request.context(EmptyTableReadRequest)?;
        Ok(Self {
            table: table_identifier.into(),
            read_request: table_read_request
                .try_into()
                .box_err()
                .context(ConvertTableReadRequest)?,
        })
    }
}

impl TryFrom<ReadRequest> for ceresdbproto::remote_engine::ReadRequest {
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

pub struct WriteRequest {
    pub table: TableIdentifier,
    pub write_request: TableWriteRequest,
}

impl TryFrom<ceresdbproto::remote_engine::WriteRequest> for WriteRequest {
    type Error = Error;

    fn try_from(
        pb: ceresdbproto::remote_engine::WriteRequest,
    ) -> std::result::Result<Self, Self::Error> {
        let table_identifier = pb.table.context(EmptyTableIdentifier)?;
        let row_group_pb = pb.row_group.context(EmptyRowGroup)?;
        let rows = row_group_pb.rows.context(EmptyRowGroup)?;
        let row_group = match rows {
            Arrow(v) => {
                ensure!(v.record_batches.len() > 0, EmptyRowGroup);

                let compression = match v.compression() {
                    arrow_payload::Compression::None => CompressionMethod::None,
                    arrow_payload::Compression::Zstd => CompressionMethod::Zstd,
                };

                let mut record_batch_vec = vec![];
                for data in v.record_batches {
                    let arrow_record_batch_vec = ipc::decode_record_batches(data, compression)
                        .map_err(|e| Box::new(e) as _)
                        .context(ConvertRowGroup)?;
                    record_batch_vec.append(
                        &mut arrow_record_batch_vec
                            .try_into()
                            .map_err(|e| Box::new(e) as _)
                            .context(ConvertRowGroup)?,
                    );
                }

                let mut row_group_builder = RowGroupBuilder::new(
                    record_batch_vec[0]
                        .schema()
                        .try_into()
                        .map_err(|e| Box::new(e) as _)
                        .context(ConvertRowGroup)?,
                );

                for record_batch in record_batch_vec {
                    let record_batch: RecordBatch = record_batch
                        .try_into()
                        .map_err(|e| Box::new(e) as _)
                        .context(ConvertRowGroup)?;

                    let num_cols = record_batch.num_columns();
                    let num_rows = record_batch.num_rows();
                    for row_idx in 0..num_rows {
                        let mut row_builder = row_group_builder.row_builder();
                        for col_idx in 0..num_cols {
                            let val = record_batch.column(col_idx).datum(row_idx);
                            row_builder = row_builder
                                .append_datum(val)
                                .map_err(|e| Box::new(e) as _)
                                .context(ConvertRowGroup)?;
                        }
                        row_builder
                            .finish()
                            .map_err(|e| Box::new(e) as _)
                            .context(ConvertRowGroup)?;
                    }
                }

                row_group_builder.build()
            }
        };

        Ok(Self {
            table: table_identifier.into(),
            write_request: TableWriteRequest { row_group },
        })
    }
}

impl TryFrom<WriteRequest> for ceresdbproto::remote_engine::WriteRequest {
    type Error = Error;

    fn try_from(request: WriteRequest) -> std::result::Result<Self, Self::Error> {
        // Row group to pb.
        let row_group = request.write_request.row_group;
        let table_schema = row_group.schema();
        let min_timestamp = row_group.min_timestamp().as_i64();
        let max_timestamp = row_group.max_timestamp().as_i64();

        let mut builder = RecordBatchWithKeyBuilder::new(table_schema.to_record_schema_with_key());

        for row in row_group {
            builder
                .append_row(row)
                .map_err(|e| Box::new(e) as _)
                .context(ConvertRowGroup)?;
        }

        let record_batch_with_key = builder
            .build()
            .map_err(|e| Box::new(e) as _)
            .context(ConvertRowGroup)?;
        let record_batch = record_batch_with_key.into_record_batch();
        let compress_output = ipc::encode_record_batch(
            &record_batch.into_arrow_record_batch(),
            // TODO: Set compress_min_size to 0 for now, we should set it to a
            // proper value.
            CompressOptions {
                compress_min_length: 0,
                method: ipc::CompressionMethod::Zstd,
            },
        )
        .map_err(|e| Box::new(e) as _)
        .context(ConvertRowGroup)?;

        let compression = match compress_output.method {
            CompressionMethod::None => arrow_payload::Compression::None,
            CompressionMethod::Zstd => arrow_payload::Compression::Zstd,
        };

        let row_group_pb = ceresdbproto::remote_engine::RowGroup {
            min_timestamp,
            max_timestamp,
            rows: Some(row_group::Rows::Arrow(ArrowPayload {
                record_batches: vec![compress_output.payload],
                compression: compression as i32,
            })),
        };

        // Table ident to pb.
        let table_pb = request.table.into();

        Ok(Self {
            table: Some(table_pb),
            row_group: Some(row_group_pb),
        })
    }
}
