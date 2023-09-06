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

//! Schema contains one or more tables

use std::sync::Arc;

use async_trait::async_trait;
use common_types::{column_schema::ColumnSchema, table::ShardId};
use generic_error::GenericError;
use macros::define_result;
use snafu::{Backtrace, Snafu};
use table_engine::{
    engine::{self, CreateTableParams, TableEngineRef, TableState},
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
        source: GenericError,
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
        source: GenericError,
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
    CreateTableWithCause { source: GenericError },

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
    DropTableWithCause { source: GenericError },

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
    OpenTableWithCause { source: GenericError },

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
    CloseTableWithCause { source: GenericError },

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
    WriteTableMeta { table: String, source: GenericError },

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
    AlterTable { source: GenericError },

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
    pub params: CreateTableParams,
    /// Table id
    // TODO: remove this field
    pub table_id: Option<TableId>,
    /// Table schema
    /// Tells state of the table
    pub state: TableState,
    /// Shard id of the table
    pub shard_id: ShardId,
}

impl CreateTableRequest {
    pub fn into_engine_create_request(
        self,
        table_id: Option<TableId>,
        schema_id: SchemaId,
    ) -> engine::CreateTableRequest {
        let table_id = self.table_id.unwrap_or(table_id.unwrap_or(TableId::MIN));

        engine::CreateTableRequest {
            params: self.params,
            schema_id,
            table_id,
            state: self.state,
            shard_id: self.shard_id,
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

/// Drop table request
#[derive(Debug, Clone)]
pub struct DropTableRequest {
    /// Catalog name
    pub catalog_name: String,
    /// Schema name
    pub schema_name: String,
    /// Table name
    pub table_name: String,
    /// Table engine type
    pub engine: String,
}

impl DropTableRequest {
    pub fn into_engine_drop_request(self, schema_id: SchemaId) -> engine::DropTableRequest {
        engine::DropTableRequest {
            catalog_name: self.catalog_name,
            schema_name: self.schema_name,
            schema_id,
            table_name: self.table_name,
            engine: self.engine,
        }
    }
}
/// Drop table options
#[derive(Clone)]
pub struct DropOptions {
    /// Table engine
    pub table_engine: TableEngineRef,
}

/// Open table request
#[derive(Debug, Clone)]
pub struct OpenTableRequest {
    /// Catalog name
    pub catalog_name: String,
    /// Schema name
    pub schema_name: String,
    /// Table name
    pub table_name: String,
    /// Table id
    pub table_id: TableId,
    /// Table engine type
    pub engine: String,
    /// Shard id, shard is the table set about scheduling from nodes
    pub shard_id: ShardId,
}

impl OpenTableRequest {
    pub fn into_engine_open_request(self, schema_id: SchemaId) -> engine::OpenTableRequest {
        engine::OpenTableRequest {
            catalog_name: self.catalog_name,
            schema_name: self.schema_name,
            schema_id,
            table_name: self.table_name,
            table_id: self.table_id,
            engine: self.engine,
            shard_id: self.shard_id,
        }
    }
}
/// Open table options.
#[derive(Clone)]
pub struct OpenOptions {
    /// Table engine
    pub table_engine: TableEngineRef,
}

/// Close table request
#[derive(Clone, Debug)]
pub struct CloseTableRequest {
    /// Catalog name
    pub catalog_name: String,
    /// Schema name
    pub schema_name: String,
    /// Table name
    pub table_name: String,
    /// Table id
    pub table_id: TableId,
    /// Table engine type
    pub engine: String,
}

impl CloseTableRequest {
    pub fn into_engine_close_request(self, schema_id: SchemaId) -> engine::CloseTableRequest {
        engine::CloseTableRequest {
            catalog_name: self.catalog_name,
            schema_name: self.schema_name,
            schema_id,
            table_name: self.table_name,
            table_id: self.table_id,
            engine: self.engine,
        }
    }
}

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

#[derive(Debug, Clone)]
pub struct OpenShardRequest {
    /// Shard id
    pub shard_id: ShardId,

    /// Table infos
    pub table_defs: Vec<TableDef>,

    /// Table engine type
    pub engine: String,
}

#[derive(Clone, Debug)]
pub struct TableDef {
    pub catalog_name: String,
    pub schema_name: String,
    pub id: TableId,
    pub name: String,
}

impl TableDef {
    pub fn into_engine_table_def(self, schema_id: SchemaId) -> engine::TableDef {
        engine::TableDef {
            catalog_name: self.catalog_name,
            schema_name: self.schema_name,
            schema_id,
            id: self.id,
            name: self.name,
        }
    }
}

pub type CloseShardRequest = OpenShardRequest;

/// Schema manage tables.
#[async_trait]
pub trait Schema {
    /// Get schema name.
    fn name(&self) -> NameRef;

    /// Get schema id
    fn id(&self) -> SchemaId;

    /// Find table by name.
    fn table_by_name(&self, name: NameRef) -> Result<Option<TableRef>>;

    /// TODO: remove this method afterwards.
    /// Create table according to `request`.
    async fn create_table(
        &self,
        request: CreateTableRequest,
        opts: CreateOptions,
    ) -> Result<TableRef>;

    /// TODO: remove this method afterwards.
    /// Drop table according to `request`.
    ///
    /// Returns true if the table is really dropped.
    async fn drop_table(&self, request: DropTableRequest, opts: DropOptions) -> Result<bool>;

    /// All tables
    fn all_tables(&self) -> Result<Vec<TableRef>>;

    /// Register the opened table into schema.
    fn register_table(&self, table: TableRef);

    /// Unregister table
    fn unregister_table(&self, table_name: &str);
}
