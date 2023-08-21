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

use std::sync::Arc;

use async_trait::async_trait;
use generic_error::GenericError;
use macros::define_result;
use query_frontend::plan::{CreateTablePlan, DropTablePlan};
use snafu::{Backtrace, Snafu};
use table_engine::engine::TableEngineRef;

use crate::{context::Context, interpreter::Output};

pub mod catalog_based;
pub mod meta_based;

pub type TableManipulatorRef = Arc<dyn TableManipulator + Send + Sync>;

#[derive(Debug, Snafu)]
#[snafu(visibility(pub(crate)))]
pub enum Error {
    #[snafu(display("Failed to find catalog, name:{}, err:{}", name, source))]
    FindCatalog {
        name: String,
        source: catalog::manager::Error,
    },

    #[snafu(display("Catalog not exists, name:{}.\nBacktrace:\n{}", name, backtrace))]
    CatalogNotExists { name: String, backtrace: Backtrace },

    #[snafu(display("Failed to find schema, name:{}, err:{}", name, source))]
    FindSchema {
        name: String,
        source: catalog::Error,
    },

    #[snafu(display("Schema not exists, name:{}.\nBacktrace:\n{}", name, backtrace))]
    SchemaNotExists { name: String, backtrace: Backtrace },

    #[snafu(display("Failed to create table, name:{}, err:{}", table, source))]
    SchemaCreateTable {
        table: String,
        source: catalog::schema::Error,
    },

    #[snafu(display("Failed to drop table in schema, name:{}, err:{}", table, source))]
    SchemaDropTable {
        table: String,
        source: catalog::schema::Error,
    },

    #[snafu(display("Failed to drop table, name:{}, err:{}", table, source))]
    DropTable {
        table: String,
        source: table_engine::engine::Error,
    },

    #[snafu(display("Failed to create table, msg:{}, err:{}", msg, source))]
    CreateWithCause { msg: String, source: GenericError },

    #[snafu(display("Failed to drop table, msg:{}, err:{}", msg, source))]
    DropWithCause { msg: String, source: GenericError },

    #[snafu(display("Failed to create partition table without ceresmeta, table:{}", table))]
    PartitionTableNotSupported { table: String },

    #[snafu(display("Failed to operate table, err:{}", source))]
    TableOperator { source: catalog::Error },
}

define_result!(Error);

#[async_trait]
pub trait TableManipulator {
    async fn create_table(
        &self,
        ctx: Context,
        plan: CreateTablePlan,
        table_engine: TableEngineRef,
    ) -> Result<Output>;

    async fn drop_table(
        &self,
        ctx: Context,
        plan: DropTablePlan,
        table_engine: TableEngineRef,
    ) -> Result<Output>;
}
