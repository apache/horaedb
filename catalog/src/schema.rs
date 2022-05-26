// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

//! Schema contains one or more tables

use std::sync::Arc;

use async_trait::async_trait;
use common_types::column_schema::ColumnSchema;
use snafu::{Backtrace, Snafu};
use table_engine::{
    engine::{CreateTableRequest, DropTableRequest, TableEngineRef},
    table::{TableId, TableRef},
};

#[derive(Debug, Snafu)]
#[snafu(visibility(pub))]
pub enum Error {
    #[snafu(display("Unsupported method, msg:{}.\nBacktrace:\n{}", msg, backtrace))]
    UnSupported { msg: String, backtrace: Backtrace },

    #[snafu(display("Failed to create table, err:{}", source))]
    CreateTable { source: table_engine::engine::Error },

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

    #[snafu(display("Failed to drop table, err:{}", source))]
    DropTable { source: table_engine::engine::Error },

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

/// Drop table options.
#[derive(Clone)]
pub struct DropOptions {
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

    /// Find table by name.
    fn table_by_name(&self, name: NameRef) -> Result<Option<TableRef>>;

    /// Allocate a table id for given table.
    fn alloc_table_id(&self, name: NameRef) -> Result<TableId>;

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

    /// All tables
    fn all_tables(&self) -> Result<Vec<TableRef>>;
}

/// A name reference
pub type NameRef<'a> = &'a str;
/// A reference counted schema pointer
// TODO(yingwen): This name is conflict with [table_engine::schema::SchemaRef].
pub type SchemaRef = Arc<dyn Schema + Send + Sync>;
