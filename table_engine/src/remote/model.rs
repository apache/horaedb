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

use std::{
    collections::HashMap,
    time::{Duration, Instant},
};

use bytes_ext::{ByteVec, Bytes};
use ceresdbproto::remote_engine::{
    self, execute_plan_request, row_group::Rows::Contiguous, ColumnDesc,
};
use common_types::{
    request_id::RequestId,
    row::{
        contiguous::{ContiguousRow, ContiguousRowReader, ContiguousRowWriter},
        Row, RowGroup,
    },
    schema::{IndexInWriterSchema, RecordSchema, Schema, Version},
};
use generic_error::{BoxError, GenericError, GenericResult};
use itertools::Itertools;
use macros::define_result;
use snafu::{ensure, Backtrace, OptionExt, ResultExt, Snafu};

use crate::{
    partition::PartitionInfo,
    table::{
        ReadRequest as TableReadRequest, SchemaId, TableId, WriteRequest as TableWriteRequest,
        NO_TIMEOUT,
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

    #[snafu(display(
        "Schema mismatch with the write request, msg:{msg}.\nBacktrace:\n{}",
        backtrace
    ))]
    SchemaMismatch { msg: String, backtrace: Backtrace },

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

    #[snafu(display(
        "Failed to convert remote execute request, msg:{}.\nBacktrace:\n{}",
        msg,
        backtrace,
    ))]
    ConvertRemoteExecuteRequest { msg: String, backtrace: Backtrace },
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

    pub fn decode_row_group_from_contiguous_payload(
        payload: ceresdbproto::remote_engine::ContiguousRows,
        schema: &Schema,
    ) -> Result<RowGroup> {
        validate_contiguous_payload_schema(schema, &payload.column_descs)?;

        let mut rows = Vec::with_capacity(payload.encoded_rows.len());
        for encoded_row in payload.encoded_rows {
            let reader = ContiguousRowReader::try_new(&encoded_row, schema)
                .box_err()
                .context(ConvertRowGroup)?;

            let mut datums = Vec::with_capacity(schema.num_columns());
            for (index, column_schema) in schema.columns().iter().enumerate() {
                let datum_view = reader.datum_view_at(index, &column_schema.data_type);
                // TODO: The clone can be avoided if we can encode the final payload directly
                // from the DatumView.
                datums.push(datum_view.to_datum());
            }
            rows.push(Row::from_datums(datums));
        }

        // The rows is decoded according to the schema, so there is no need to do a
        // more check here.
        Ok(RowGroup::new_unchecked(schema.clone(), rows))
    }

    pub fn convert_into_pb(self) -> Result<ceresdbproto::remote_engine::WriteRequest> {
        let row_group = self.write_request.row_group;
        let table_schema = row_group.schema();

        let mut encoded_rows = Vec::with_capacity(row_group.num_rows());
        // TODO: The schema of the written row group may be different from the original
        // one, so the compatibility for that should be considered.
        let index_in_schema = IndexInWriterSchema::for_same_schema(table_schema.num_columns());
        for row in &row_group {
            let mut buf = ByteVec::new();
            let mut writer = ContiguousRowWriter::new(&mut buf, table_schema, &index_in_schema);
            writer.write_row(row).with_context(|| WriteRequestToPb {
                table_ident: self.table.clone(),
            })?;
            encoded_rows.push(buf);
        }

        let column_descs = table_schema
            .columns()
            .iter()
            .map(ColumnDesc::from)
            .collect_vec();
        let contiguous_rows = ceresdbproto::remote_engine::ContiguousRows {
            schema_version: table_schema.version(),
            encoded_rows,
            column_descs,
        };
        let row_group_pb = ceresdbproto::remote_engine::RowGroup {
            // Deprecated: the two timestamps are not used anymore.
            min_timestamp: 0,
            max_timestamp: 0,
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

/// Validate the provided column descriptions with schema.
fn validate_contiguous_payload_schema(schema: &Schema, column_descs: &[ColumnDesc]) -> Result<()> {
    ensure!(
        schema.num_columns() == column_descs.len(),
        SchemaMismatch {
            msg: format!(
                "expect {} columns, but got {}",
                schema.num_columns(),
                column_descs.len(),
            )
        }
    );

    for (expect_column_schema, desc) in schema.columns().iter().zip(column_descs.iter()) {
        ensure!(
            expect_column_schema.is_correct_desc(desc),
            SchemaMismatch {
                msg: format!("expect column schema:{expect_column_schema:?}, but got {desc:?}")
            }
        );
    }

    Ok(())
}

pub struct WriteBatchResult {
    pub table_idents: Vec<TableIdentifier>,
    pub result: GenericResult<u64>,
}

#[derive(Debug)]
pub struct AlterTableSchemaRequest {
    pub table_ident: TableIdentifier,
    pub table_schema: Schema,
    /// Previous schema version before alteration.
    pub pre_schema_version: Version,
}

impl TryFrom<ceresdbproto::remote_engine::AlterTableSchemaRequest> for AlterTableSchemaRequest {
    type Error = Error;

    fn try_from(value: remote_engine::AlterTableSchemaRequest) -> Result<Self> {
        let table = value.table.context(EmptyTableIdentifier)?.into();
        let table_schema = value
            .table_schema
            .context(EmptyTableSchema)?
            .try_into()
            .box_err()
            .context(ConvertTableSchema)?;
        Ok(Self {
            table_ident: table,
            table_schema,
            pre_schema_version: value.pre_schema_version,
        })
    }
}

impl From<AlterTableSchemaRequest> for ceresdbproto::remote_engine::AlterTableSchemaRequest {
    fn from(value: AlterTableSchemaRequest) -> Self {
        let table = value.table_ident.into();
        let table_schema = (&value.table_schema).into();
        Self {
            table: Some(table),
            table_schema: Some(table_schema),
            pre_schema_version: value.pre_schema_version,
        }
    }
}

#[derive(Debug)]
pub struct AlterTableOptionsRequest {
    pub table_ident: TableIdentifier,
    pub options: HashMap<String, String>,
}

impl TryFrom<ceresdbproto::remote_engine::AlterTableOptionsRequest> for AlterTableOptionsRequest {
    type Error = Error;

    fn try_from(value: remote_engine::AlterTableOptionsRequest) -> Result<Self> {
        let table = value.table.context(EmptyTableIdentifier)?.into();
        let options = value.options;
        Ok(Self {
            table_ident: table,
            options,
        })
    }
}

impl From<AlterTableOptionsRequest> for ceresdbproto::remote_engine::AlterTableOptionsRequest {
    fn from(value: AlterTableOptionsRequest) -> Self {
        let table = value.table_ident.into();
        let options = value.options;
        Self {
            table: Some(table),
            options,
        }
    }
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

/// Request for remote executing physical plan
pub struct ExecutePlanRequest {
    /// Schema of the encoded physical plan
    pub plan_schema: RecordSchema,

    /// Remote plan execution request
    pub remote_request: RemoteExecuteRequest,
}

impl ExecutePlanRequest {
    pub fn new(
        table: TableIdentifier,
        plan_schema: RecordSchema,
        context: ExecContext,
        physical_plan: PhysicalPlan,
    ) -> Self {
        let remote_request = RemoteExecuteRequest {
            table,
            context,
            physical_plan,
        };

        Self {
            plan_schema,
            remote_request,
        }
    }
}

pub struct RemoteExecuteRequest {
    /// Table information for routing
    pub table: TableIdentifier,

    /// Execution Context
    pub context: ExecContext,

    /// Physical plan for remote execution
    pub physical_plan: PhysicalPlan,
}

pub struct ExecContext {
    pub request_id: RequestId,
    pub deadline: Option<Instant>,
    pub default_catalog: String,
    pub default_schema: String,
    pub sql: String,
}

pub enum PhysicalPlan {
    Datafusion(Bytes),
}

impl From<RemoteExecuteRequest> for ceresdbproto::remote_engine::ExecutePlanRequest {
    fn from(value: RemoteExecuteRequest) -> Self {
        let rest_duration_ms = if let Some(deadline) = value.context.deadline {
            deadline.duration_since(Instant::now()).as_millis() as i64
        } else {
            NO_TIMEOUT
        };

        let pb_context = ceresdbproto::remote_engine::ExecContext {
            request_id: value.context.request_id.as_u64(),
            default_catalog: value.context.default_catalog,
            default_schema: value.context.default_schema,
            timeout_ms: rest_duration_ms,
            priority: 0, // not used now
            sql: value.context.sql,
        };

        let pb_plan = match value.physical_plan {
            PhysicalPlan::Datafusion(plan) => {
                execute_plan_request::PhysicalPlan::Datafusion(plan.to_vec())
            }
        };

        let pb_table = ceresdbproto::remote_engine::TableIdentifier::from(value.table);

        Self {
            table: Some(pb_table),
            context: Some(pb_context),
            physical_plan: Some(pb_plan),
        }
    }
}

impl TryFrom<ceresdbproto::remote_engine::ExecutePlanRequest> for RemoteExecuteRequest {
    type Error = crate::remote::model::Error;

    fn try_from(
        value: ceresdbproto::remote_engine::ExecutePlanRequest,
    ) -> std::result::Result<Self, Self::Error> {
        // Table ident
        let pb_table = value.table.context(ConvertRemoteExecuteRequest {
            msg: "missing table ident",
        })?;
        let table = TableIdentifier::from(pb_table);

        // Execution context
        let pb_exec_ctx = value.context.context(ConvertRemoteExecuteRequest {
            msg: "missing exec ctx",
        })?;
        let ceresdbproto::remote_engine::ExecContext {
            request_id,
            default_catalog,
            default_schema,
            timeout_ms,
            sql,
            ..
        } = pb_exec_ctx;

        let request_id = RequestId::from(request_id);
        let deadline = if timeout_ms >= 0 {
            Some(Instant::now() + Duration::from_millis(timeout_ms as u64))
        } else {
            None
        };

        let exec_ctx = ExecContext {
            request_id,
            deadline,
            default_catalog,
            default_schema,
            sql,
        };

        // Plan
        let pb_plan = value.physical_plan.context(ConvertRemoteExecuteRequest {
            msg: "missing physical plan",
        })?;
        let plan = match pb_plan {
            ceresdbproto::remote_engine::execute_plan_request::PhysicalPlan::Datafusion(plan) => {
                PhysicalPlan::Datafusion(Bytes::from(plan))
            }
        };

        Ok(Self {
            context: exec_ctx,
            physical_plan: plan,
            table,
        })
    }
}
