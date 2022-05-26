// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

//! Interpreter for drop statements

use async_trait::async_trait;
use catalog::{manager::Manager, schema::DropOptions};
use snafu::{Backtrace, OptionExt, ResultExt, Snafu};
use sql::plan::DropTablePlan;
use table_engine::engine::{DropTableRequest, TableEngineRef};

use crate::{
    context::Context,
    interpreter::{Drop, Interpreter, InterpreterPtr, Output, Result as InterpreterResult},
};

#[derive(Debug, Snafu)]
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
}

define_result!(Error);

/// Drop interpreter
pub struct DropInterpreter<C> {
    ctx: Context,
    plan: DropTablePlan,
    catalog_manager: C,
    table_engine: TableEngineRef,
}

impl<C: Manager + 'static> DropInterpreter<C> {
    pub fn create(
        ctx: Context,
        plan: DropTablePlan,
        catalog_manager: C,
        table_engine: TableEngineRef,
    ) -> InterpreterPtr {
        Box::new(Self {
            ctx,
            plan,
            catalog_manager,
            table_engine,
        })
    }
}

impl<C: Manager> DropInterpreter<C> {
    async fn execute_drop(self: Box<Self>) -> Result<Output> {
        let default_catalog = self.ctx.default_catalog();
        let catalog = self
            .catalog_manager
            .catalog_by_name(default_catalog)
            .context(FindCatalog {
                name: default_catalog,
            })?
            .context(CatalogNotExists {
                name: default_catalog,
            })?;

        let default_schema = self.ctx.default_schema();
        let schema = catalog
            .schema_by_name(default_schema)
            .context(FindSchema {
                name: default_schema,
            })?
            .context(SchemaNotExists {
                name: default_schema,
            })?;

        let table = self.plan.table;
        let request = DropTableRequest {
            catalog_name: catalog.name().to_string(),
            schema_name: schema.name().to_string(),
            table_name: table.clone(),
            engine: self.plan.engine,
        };

        let opts = DropOptions {
            table_engine: self.table_engine,
        };

        let dropped = schema
            .drop_table(request, opts)
            .await
            .context(SchemaDropTable { table: &table })?;

        Ok(Output::AffectedRows(if dropped { 1 } else { 0 }))
    }
}

// TODO(yingwen): Wrap a method that returns self::Result, simplify some code to
// converting self::Error to super::Error
#[async_trait]
impl<C: Manager> Interpreter for DropInterpreter<C> {
    async fn execute(self: Box<Self>) -> InterpreterResult<Output> {
        self.execute_drop().await.context(Drop)
    }
}
