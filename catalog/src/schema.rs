// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

//! Schema contains one or more tables

use std::{collections::HashMap, sync::Arc};

use async_trait::async_trait;
use common_types::column_schema::ColumnSchema;
use snafu::{Backtrace, Snafu};
use table_engine::{
    engine::{self, TableEngineRef, TableState},
    table::{SchemaId, TableId, TableRef},
};

#[derive(Debug, Snafu)]
#[snafu(visibility(pub))]
pub enum Error {
    #[snafu(display("Unsupported method, msg:{}.\nBacktrace:\n{}", msg, backtrace))]
    UnSupported { msg: String, backtrace: Backtrace },

    #[snafu(display(
        "Failed to allocate table id, schema:{}, table:{}, err:{}",
        schema,
        table,
        source
    ))]
    AllocateTableId {
        schema: String,
        table: String,
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display(
        "Failed to invalidate table id, schema:{}, table:{}, table_id:{}, err:{}",
        schema,
        table_name,
        table_id,
        source
    ))]
    InvalidateTableId {
        schema: String,
        table_name: String,
        table_id: TableId,
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display(
        "Failed to create table, request:{:?}, msg:{}.\nBacktrace:\n{}",
        request,
        msg,
        backtrace
    ))]
    CreateTable {
        request: CreateTableRequest,
        msg: String,
        backtrace: Backtrace,
    },

    #[snafu(display("Failed to create table, err:{}", source))]
    CreateTableWithCause {
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display(
        "Failed to drop table, request:{:?}, msg:{}.\nBacktrace:\n{}",
        request,
        msg,
        backtrace
    ))]
    DropTable {
        request: DropTableRequest,
        msg: String,
        backtrace: Backtrace,
    },

    #[snafu(display("Failed to drop table, err:{}", source))]
    DropTableWithCause {
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display(
        "Failed to open table, request:{:?}, msg:{}.\nBacktrace:\n{}",
        request,
        msg,
        backtrace
    ))]
    OpenTable {
        request: OpenTableRequest,
        msg: String,
        backtrace: Backtrace,
    },

    #[snafu(display("Failed to open table, source:{}", source))]
    OpenTableWithCause {
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display(
        "Failed to close table, request:{:?}, msg:{}.\nBacktrace:\n{}",
        request,
        msg,
        backtrace
    ))]
    CloseTable {
        request: CloseTableRequest,
        msg: String,
        backtrace: Backtrace,
    },

    #[snafu(display("Failed to close table, source:{}", source))]
    CloseTableWithCause {
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display(
        "Failed to create table, table already exists, table:{}.\nBacktrace:\n{}",
        table,
        backtrace
    ))]
    CreateExistTable { table: String, backtrace: Backtrace },

    #[snafu(display(
        "Failed to create table, cannot persist meta, table:{}, err:{}",
        table,
        source
    ))]
    WriteTableMeta {
        table: String,
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display(
        "Catalog mismatch, expect:{}, given:{}.\nBacktrace:\n{}",
        expect,
        given,
        backtrace
    ))]
    CatalogMismatch {
        expect: String,
        given: String,
        backtrace: Backtrace,
    },

    #[snafu(display(
        "Schema mismatch, expect:{}, given:{}.\nBacktrace:\n{}",
        expect,
        given,
        backtrace
    ))]
    SchemaMismatch {
        expect: String,
        given: String,
        backtrace: Backtrace,
    },

    #[snafu(display(
        "Invalid table id, msg:{}, table_id:{}.\nBacktrace:\n{}",
        msg,
        table_id,
        backtrace
    ))]
    InvalidTableId {
        msg: &'static str,
        table_id: TableId,
        backtrace: Backtrace,
    },

    #[snafu(display("Failed to find table, table:{}.\nBacktrace:\n{}", table, backtrace))]
    TableNotFound { table: String, backtrace: Backtrace },

    #[snafu(display("Failed to alter table, err:{}", source))]
    AlterTable {
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display(
        "Too many table, cannot create table, schema:{}, table:{}.\nBacktrace:\n{}",
        schema,
        table,
        backtrace
    ))]
    TooManyTable {
        schema: String,
        table: String,
        backtrace: Backtrace,
    },
}

define_result!(Error);

/// A name reference.
pub type NameRef<'a> = &'a str;
// TODO: This name is conflict with [table_engine::schema::SchemaRef].
pub type SchemaRef = Arc<dyn Schema + Send + Sync>;

/// Request of creating table.
#[derive(Debug, Clone)]
pub struct CreateTableRequest {
    /// Catalog name
    pub catalog_name: String,
    /// Schema name
    pub schema_name: String,
    /// Schema id
    pub schema_id: SchemaId,
    /// Table name
    pub table_name: String,
    /// Table schema
    pub table_schema: common_types::schema::Schema,
    /// Table engine type
    pub engine: String,
    /// Table options used by each engine
    pub options: HashMap<String, String>,
    /// Tells state of the table
    pub state: TableState,
}

impl CreateTableRequest {
    pub fn into_engine_create_request(self, table_id: TableId) -> engine::CreateTableRequest {
        engine::CreateTableRequest {
            catalog_name: self.catalog_name,
            schema_name: self.schema_name,
            schema_id: self.schema_id,
            table_name: self.table_name,
            table_id,
            table_schema: self.table_schema,
            partition_info: None,
            engine: self.engine,
            options: self.options,
            state: self.state,
        }
    }
}

/// Create table options.
#[derive(Clone)]
pub struct CreateOptions {
    /// Table engine
    // FIXME(yingwen): We have engine type in create request, remove this
    pub table_engine: TableEngineRef,
    /// Create if not exists, if table already exists, wont return error
    // TODO(yingwen): Maybe remove this?
    pub create_if_not_exists: bool,
}

pub type DropTableRequest = engine::DropTableRequest;

/// Drop table options.
#[derive(Clone)]
pub struct DropOptions {
    /// Table engine
    pub table_engine: TableEngineRef,
}

pub type OpenTableRequest = engine::OpenTableRequest;

/// Open table options.
#[derive(Clone)]
pub struct OpenOptions {
    /// Table engine
    pub table_engine: TableEngineRef,
}

pub type CloseTableRequest = engine::CloseTableRequest;

/// Close table options.
#[derive(Clone)]
pub struct CloseOptions {
    /// Table engine
    pub table_engine: TableEngineRef,
}

/// Alter table operations.
#[derive(Debug)]
pub enum AlterTableOperation {
    /// Add column operation, the column id in [ColumnSchema] will be ignored.
    /// Primary key column is not allowed to be added, so all columns will
    /// be added as normal columns.
    AddColumn(ColumnSchema),
}

/// Alter table request.
#[derive(Debug)]
pub struct AlterTableRequest {
    pub table_name: String,
    pub operations: Vec<AlterTableOperation>,
}

/// Schema manage tables.
#[async_trait]
pub trait Schema {
    /// Get schema name.
    fn name(&self) -> NameRef;

    /// Get schema id
    fn id(&self) -> SchemaId;

    /// Find table by name.
    fn table_by_name(&self, name: NameRef) -> Result<Option<TableRef>>;

    /// Create table according to `request`.
    async fn create_table(
        &self,
        request: CreateTableRequest,
        opts: CreateOptions,
    ) -> Result<TableRef>;

    /// Drop table according to `request`.
    ///
    /// Returns true if the table is really dropped.
    async fn drop_table(&self, request: DropTableRequest, opts: DropOptions) -> Result<bool>;

    /// Open the table according to `request`.
    ///
    /// Return None if table does not exist.
    async fn open_table(
        &self,
        request: OpenTableRequest,
        opts: OpenOptions,
    ) -> Result<Option<TableRef>>;

    /// Close the table according to `request`.
    ///
    /// Return false if table does not exist.
    async fn close_table(&self, request: CloseTableRequest, opts: CloseOptions) -> Result<()>;

    /// All tables
    fn all_tables(&self) -> Result<Vec<TableRef>>;
}
