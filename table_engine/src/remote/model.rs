// Copyright 2023 The CeresDB Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Model for remote table engine

use std::collections::HashMap;

use arrow_ext::{ipc, ipc::CompressionMethod};
use bytes_ext::ByteVec;
use ceresdbproto::{
    remote_engine::{self, row_group::Rows::Contiguous},
    storage::{arrow_payload, ArrowPayload},
};
use common_types::{
    record_batch::RecordBatch,
    row::{
        contiguous::{ContiguousRow, ContiguousRowReader, ContiguousRowWriter},
        Row, RowGroup, RowGroupBuilder,
    },
    schema::{IndexInWriterSchema, Schema},
};
use generic_error::{BoxError, GenericError, GenericResult};
use macros::define_result;
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
        "Failed to convert write request to pb, table_ident:{table_ident:?}, err:{source}",
    ))]
    WriteRequestToPb {
        table_ident: TableIdentifier,
        source: common_types::row::contiguous::Error,
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

    fn try_from(pb: ceresdbproto::remote_engine::ReadRequest) -> Result<Self> {
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

    fn try_from(request: ReadRequest) -> Result<Self> {
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

impl WriteBatchRequest {
    pub fn convert_into_pb(self) -> Result<ceresdbproto::remote_engine::WriteBatchRequest> {
        let batch = self
            .batch
            .into_iter()
            .map(|req| req.convert_into_pb())
            .collect::<Result<Vec<_>>>()?;

        Ok(remote_engine::WriteBatchRequest { batch })
    }
}

pub struct WriteRequest {
    pub table: TableIdentifier,
    pub write_request: TableWriteRequest,
}

impl WriteRequest {
    pub fn new(table_ident: TableIdentifier, row_group: RowGroup) -> Self {
        Self {
            table: table_ident,
            write_request: TableWriteRequest { row_group },
        }
    }

    pub fn decode_row_group_from_arrow_payload(payload: ArrowPayload) -> Result<RowGroup> {
        ensure!(!payload.record_batches.is_empty(), EmptyRecordBatch);

        let compression = match payload.compression() {
            arrow_payload::Compression::None => CompressionMethod::None,
            arrow_payload::Compression::Zstd => CompressionMethod::Zstd,
        };

        let mut record_batch_vec = vec![];
        for data in payload.record_batches {
            let mut arrow_record_batch_vec = ipc::decode_record_batches(data, compression)
                .map_err(|e| Box::new(e) as _)
                .context(ConvertRowGroup)?;
            record_batch_vec.append(&mut arrow_record_batch_vec);
        }

        build_row_group_from_record_batch(record_batch_vec)
    }

    pub fn decode_row_group_from_contiguous_payload(
        payload: ceresdbproto::remote_engine::ContiguousRows,
        schema: &Schema,
    ) -> Result<RowGroup> {
        let mut row_group_builder =
            RowGroupBuilder::with_capacity(schema.clone(), payload.encoded_rows.len());
        for encoded_row in payload.encoded_rows {
            let reader = ContiguousRowReader::try_new(&encoded_row, schema)
                .box_err()
                .context(ConvertRowGroup)?;

            let mut datums = Vec::with_capacity(schema.num_columns());
            for (index, column_schema) in schema.columns().iter().enumerate() {
                let datum_view = reader.datum_view_at(index, &column_schema.data_type);
                datums.push(datum_view.to_datum());
            }
            row_group_builder.push_checked_row(Row::from_datums(datums));
        }
        Ok(row_group_builder.build())
    }

    pub fn convert_into_pb(self) -> Result<ceresdbproto::remote_engine::WriteRequest> {
        let row_group = self.write_request.row_group;
        let table_schema = row_group.schema();
        let min_timestamp = row_group.min_timestamp().as_i64();
        let max_timestamp = row_group.max_timestamp().as_i64();

        let mut encoded_rows = Vec::with_capacity(row_group.num_rows());
        let index_in_schema = IndexInWriterSchema::for_same_schema(table_schema.num_columns());
        for row in &row_group {
            let mut buf = ByteVec::new();
            let mut writer = ContiguousRowWriter::new(&mut buf, table_schema, &index_in_schema);
            writer.write_row(row).with_context(|| WriteRequestToPb {
                table_ident: self.table.clone(),
            })?;
            encoded_rows.push(buf);
        }

        let contiguous_rows = ceresdbproto::remote_engine::ContiguousRows {
            schema_version: table_schema.version(),
            encoded_rows,
        };
        let row_group_pb = ceresdbproto::remote_engine::RowGroup {
            min_timestamp,
            max_timestamp,
            rows: Some(Contiguous(contiguous_rows)),
        };

        // Table ident to pb.
        let table_pb = self.table.into();

        Ok(ceresdbproto::remote_engine::WriteRequest {
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

    fn try_from(value: ceresdbproto::remote_engine::GetTableInfoRequest) -> Result<Self> {
        let table = value.table.context(EmptyTableIdentifier)?.into();
        Ok(Self { table })
    }
}

impl TryFrom<GetTableInfoRequest> for ceresdbproto::remote_engine::GetTableInfoRequest {
    type Error = Error;

    fn try_from(value: GetTableInfoRequest) -> Result<Self> {
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
    record_batches: Vec<arrow::record_batch::RecordBatch>,
) -> Result<RowGroup> {
    ensure!(!record_batches.is_empty(), EmptyRecordBatch);

    let mut row_group_builder = RowGroupBuilder::new(
        record_batches[0]
            .schema()
            .try_into()
            .map_err(|e| Box::new(e) as _)
            .context(ConvertRowGroup)?,
    );

    for record_batch in record_batches {
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

    Ok(row_group_builder.build())
}
