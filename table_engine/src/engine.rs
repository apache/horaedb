// Copyright 2022-2023 CeresDB Project Authors. Licensed under Apache-2.0.

//! Table factory trait

use std::{collections::HashMap, sync::Arc};

use async_trait::async_trait;
use ceresdbproto::sys_catalog as sys_catalog_pb;
use common_types::{
    schema::Schema,
    table::{ShardId, DEFAULT_SHARD_ID},
};
use common_util::{error::GenericError, runtime::Runtime};
use snafu::{ensure, Backtrace, Snafu};

use crate::{
    partition::PartitionInfo,
    table::{SchemaId, TableId, TableInfo, TableRef},
};

#[derive(Debug, Snafu)]
#[snafu(visibility(pub))]
pub enum Error {
    #[snafu(display("Invalid table path, path:{}.\nBacktrace:\n{}", path, backtrace))]
    InvalidTablePath { path: String, backtrace: Backtrace },

    #[snafu(display("Table already exists, table:{}.\nBacktrace:\n{}", table, backtrace))]
    TableExists { table: String, backtrace: Backtrace },

    #[snafu(display("Invalid arguments, table:{table}, err:{source}"))]
    InvalidArguments { table: String, source: GenericError },

    #[snafu(display("Failed to write meta data, err:{}", source))]
    WriteMeta { source: GenericError },

    #[snafu(display("Unexpected error, err:{}", source))]
    Unexpected { source: GenericError },

    #[snafu(display("Unexpected error, msg:{}", msg))]
    UnexpectedNoCause { msg: String },

    #[snafu(display(
        "Unknown engine type, type:{}.\nBacktrace:\n{}",
        engine_type,
        backtrace
    ))]
    UnknownEngineType {
        engine_type: String,
        backtrace: Backtrace,
    },

    #[snafu(display(
        "Invalid table state transition, from:{:?}, to:{:?}.\nBacktrace:\n{}",
        from,
        to,
        backtrace
    ))]
    InvalidTableStateTransition {
        from: TableState,
        to: TableState,
        backtrace: Backtrace,
    },

    #[snafu(display("Failed to close the table engine, err:{}", source))]
    Close { source: GenericError },
}

define_result!(Error);

/// The state of table.
///
/// Transition rule is defined in the validate function.
#[derive(Clone, Copy, Debug)]
pub enum TableState {
    Stable = 0,
    Dropping = 1,
    Dropped = 2,
}

impl TableState {
    pub fn validate(&self, to: TableState) -> bool {
        match self {
            TableState::Stable => matches!(to, TableState::Stable | TableState::Dropping),
            TableState::Dropping => matches!(to, TableState::Dropped),
            TableState::Dropped => false,
        }
    }

    /// Try to transit from the self state to the `to` state.
    ///
    /// Returns error if it is a invalid transition.
    pub fn try_transit(&mut self, to: TableState) -> Result<()> {
        ensure!(
            self.validate(to),
            InvalidTableStateTransition { from: *self, to }
        );
        *self = to;

        Ok(())
    }
}

impl From<TableState> for sys_catalog_pb::TableState {
    fn from(state: TableState) -> Self {
        match state {
            TableState::Stable => Self::Stable,
            TableState::Dropping => Self::Dropping,
            TableState::Dropped => Self::Dropped,
        }
    }
}

impl From<sys_catalog_pb::TableState> for TableState {
    fn from(state: sys_catalog_pb::TableState) -> TableState {
        match state {
            sys_catalog_pb::TableState::Stable => TableState::Stable,
            sys_catalog_pb::TableState::Dropping => TableState::Dropping,
            sys_catalog_pb::TableState::Dropped => TableState::Dropped,
        }
    }
}

#[derive(Copy, Clone)]
pub enum TableRequestType {
    Create,
    Drop,
}

/// Create table request
// TODO(yingwen): Add option for create_if_not_exists?
#[derive(Debug, Clone)]
pub struct CreateTableRequest {
    /// Catalog name
    pub catalog_name: String,
    /// Schema name
    pub schema_name: String,
    /// Schema id
    pub schema_id: SchemaId,
    /// Table id
    pub table_id: TableId,
    // TODO(yingwen): catalog and schema, or add a table path struct?
    /// Table name
    pub table_name: String,
    /// Table schema
    pub table_schema: Schema,
    /// Table engine type
    pub engine: String,
    /// Table options used by each engine
    pub options: HashMap<String, String>,
    /// Tells state of the table
    pub state: TableState,
    /// Shard id, shard is the table set about scheduling from nodes
    /// It will be assigned the default value in standalone mode,
    /// and just be useful in cluster mode
    pub shard_id: ShardId,
    /// Partition info if this is a partitioned table
    pub partition_info: Option<PartitionInfo>,
}

impl From<CreateTableRequest> for sys_catalog_pb::TableEntry {
    fn from(req: CreateTableRequest) -> Self {
        sys_catalog_pb::TableEntry {
            catalog_name: req.catalog_name,
            schema_name: req.schema_name,
            schema_id: req.schema_id.as_u32(),
            table_id: req.table_id.as_u64(),
            table_name: req.table_name,
            engine: req.engine,
            state: sys_catalog_pb::TableState::from(req.state) as i32,
            created_time: 0,
            modified_time: 0,
        }
    }
}

impl From<CreateTableRequest> for TableInfo {
    fn from(req: CreateTableRequest) -> Self {
        Self {
            catalog_name: req.catalog_name,
            schema_name: req.schema_name,
            schema_id: req.schema_id,
            table_name: req.table_name,
            table_id: req.table_id,
            engine: req.engine,
            state: req.state,
        }
    }
}

/// Drop table request
#[derive(Debug, Clone)]
pub struct DropTableRequest {
    /// Catalog name
    pub catalog_name: String,
    /// Schema name
    pub schema_name: String,
    /// Schema id
    pub schema_id: SchemaId,
    /// Table name
    pub table_name: String,
    /// Table engine type
    pub engine: String,
}

#[derive(Debug, Clone)]
pub struct OpenTableRequest {
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
    /// Table engine type
    pub engine: String,
    /// Shard id, shard is the table set about scheduling from nodes
    pub shard_id: ShardId,
}

impl From<TableInfo> for OpenTableRequest {
    /// The `shard_id` is not persisted and just assigned a default value
    /// while recovered from `TableInfo`.
    /// This conversion will just happen in standalone mode.
    fn from(table_info: TableInfo) -> Self {
        Self {
            catalog_name: table_info.catalog_name,
            schema_name: table_info.schema_name,
            schema_id: table_info.schema_id,
            table_name: table_info.table_name,
            table_id: table_info.table_id,
            engine: table_info.engine,
            shard_id: DEFAULT_SHARD_ID,
        }
    }
}

#[derive(Debug, Clone)]
pub struct CloseTableRequest {
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
    /// Table engine type
    pub engine: String,
}

/// Table engine
// TODO(yingwen): drop table support to release resource owned by the table
#[async_trait]
pub trait TableEngine: Send + Sync {
    /// Returns the name of engine.
    fn engine_type(&self) -> &str;

    /// Close the engine gracefully.
    async fn close(&self) -> Result<()>;

    /// Create table
    async fn create_table(&self, request: CreateTableRequest) -> Result<TableRef>;

    /// Drop table
    async fn drop_table(&self, request: DropTableRequest) -> Result<bool>;

    /// Open table, return None if table not exists
    async fn open_table(&self, request: OpenTableRequest) -> Result<Option<TableRef>>;

    /// Close table
    async fn close_table(&self, request: CloseTableRequest) -> Result<()>;
}

/// A reference counted pointer to table engine
pub type TableEngineRef = Arc<dyn TableEngine>;

#[derive(Clone, Debug)]
pub struct EngineRuntimes {
    pub read_runtime: Arc<Runtime>,
    pub write_runtime: Arc<Runtime>,
    pub meta_runtime: Arc<Runtime>,
    pub bg_runtime: Arc<Runtime>,
}
