// Copyright 2022-2023 CeresDB Project Authors. Licensed under Apache-2.0.

//! Model for remote table engine

use std::collections::HashMap;

use arrow_ext::{
    ipc,
    ipc::{CompressOptions, CompressionMethod},
};
use ceresdbproto::{
    remote_engine,
    remote_engine::row_group::Rows::Arrow,
    storage::{arrow_payload, ArrowPayload},
};
use common_types::{
    record_batch::{RecordBatchBuilder, RecordBatchWithKeyBuilder},
    row::{RowGroup, RowGroupBuilder},
    schema::Schema,
};
use common_util::error::{BoxError, GenericError, GenericResult};
use snafu::{ensure, Backtrace, OptionExt, ResultExt, Snafu};

use crate::{
    partition::PartitionInfo,
    table::{
        ReadRequest as TableReadRequest, SchemaId, TableId, WriteRequest as TableWriteRequest,
    },
};

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

    #[snafu(display("Record batches can't be empty.\nBacktrace:\n{}", backtrace,))]
    EmptyRecordBatch { backtrace: Backtrace },
}

define_result!(Error);

const DEFAULT_COMPRESS_MIN_LENGTH: usize = 80 * 1024;

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

#[derive(Default)]
pub struct WriteBatchRequest {
    pub batch: Vec<WriteRequest>,
}

impl TryFrom<ceresdbproto::remote_engine::WriteBatchRequest> for WriteBatchRequest {
    type Error = Error;

    fn try_from(
        pb: ceresdbproto::remote_engine::WriteBatchRequest,
    ) -> std::result::Result<Self, Self::Error> {
        let batch = pb
            .batch
            .into_iter()
            .map(WriteRequest::try_from)
            .collect::<std::result::Result<Vec<_>, Self::Error>>()?;

        Ok(WriteBatchRequest { batch })
    }
}

impl TryFrom<WriteBatchRequest> for ceresdbproto::remote_engine::WriteBatchRequest {
    type Error = Error;

    fn try_from(batch_request: WriteBatchRequest) -> std::result::Result<Self, Self::Error> {
        let batch = batch_request
            .batch
            .into_iter()
            .map(remote_engine::WriteRequest::try_from)
            .collect::<std::result::Result<Vec<_>, Self::Error>>()?;

        Ok(remote_engine::WriteBatchRequest { batch })
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
                ensure!(!v.record_batches.is_empty(), EmptyRecordBatch);

                let compression = match v.compression() {
                    arrow_payload::Compression::None => CompressionMethod::None,
                    arrow_payload::Compression::Zstd => CompressionMethod::Zstd,
                };

                let mut record_batch_vec = vec![];
                for data in v.record_batches {
                    let mut arrow_record_batch_vec = ipc::decode_record_batches(data, compression)
                        .map_err(|e| Box::new(e) as _)
                        .context(ConvertRowGroup)?;
                    record_batch_vec.append(&mut arrow_record_batch_vec);
                }

                build_row_group_from_record_batch(record_batch_vec)?
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
            CompressOptions {
                compress_min_length: DEFAULT_COMPRESS_MIN_LENGTH,
                method: CompressionMethod::Zstd,
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
            rows: Some(Arrow(ArrowPayload {
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

pub struct WriteBatchResult {
    pub table_idents: Vec<TableIdentifier>,
    pub result: GenericResult<u64>,
}

pub struct GetTableInfoRequest {
    pub table: TableIdentifier,
}

impl TryFrom<ceresdbproto::remote_engine::GetTableInfoRequest> for GetTableInfoRequest {
    type Error = Error;

    fn try_from(
        value: ceresdbproto::remote_engine::GetTableInfoRequest,
    ) -> std::result::Result<Self, Self::Error> {
        let table = value.table.context(EmptyTableIdentifier)?.into();
        Ok(Self { table })
    }
}

impl TryFrom<GetTableInfoRequest> for ceresdbproto::remote_engine::GetTableInfoRequest {
    type Error = Error;

    fn try_from(value: GetTableInfoRequest) -> std::result::Result<Self, Self::Error> {
        let table = value.table.into();
        Ok(Self { table: Some(table) })
    }
}

pub struct TableInfo {
    /// Catalog name
    pub catalog_name: String,
    /// Schema name
    pub schema_name: String,
    /// Schema id
    pub schema_id: SchemaId,
    /// Table name
    pub table_name: String,
    /// Table id
    pub table_id: TableId,
    /// Table schema
    pub table_schema: Schema,
    /// Table engine type
    pub engine: String,
    /// Table options
    pub options: HashMap<String, String>,
    /// Partition Info
    pub partition_info: Option<PartitionInfo>,
}

fn build_row_group_from_record_batch(
    arrow_record_batches: Vec<arrow::record_batch::RecordBatch>,
) -> Result<RowGroup> {
    ensure!(!arrow_record_batches.is_empty(), EmptyRecordBatch);

    let mut row_group_builder = RowGroupBuilder::new(
        arrow_record_batches[0]
            .schema()
            .try_into()
            .map_err(|e| Box::new(e) as _)
            .context(ConvertRowGroup)?,
    );

    let mut record_batch_builder = RecordBatchBuilder::default();
    for arrow_record_batch in arrow_record_batches {
        let record_batch = record_batch_builder
            .build(arrow_record_batch)
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

    Ok(row_group_builder.build())
}
