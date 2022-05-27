// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

//! Interpreter for create statements

use async_trait::async_trait;
use catalog::{
    manager::Manager,
    schema::{CreateOptions, CreateTableRequest},
};
use snafu::{Backtrace, OptionExt, ResultExt, Snafu};
use sql::plan::CreateTablePlan;
use table_engine::engine::{TableEngineRef, TableState};

use crate::{
    context::Context,
    interpreter::{Create, Interpreter, InterpreterPtr, Output, Result as InterpreterResult},
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

    #[snafu(display("Failed to create table, name:{}, err:{}", table, source))]
    SchemaCreateTable {
        table: String,
        source: catalog::schema::Error,
    },

    #[snafu(display("Failed to allocate table id, err:{}", source))]
    AllocTableId { source: catalog::schema::Error },
}

define_result!(Error);

/// Create interpreter
pub struct CreateInterpreter<C> {
    ctx: Context,
    plan: CreateTablePlan,
    catalog_manager: C,
    table_engine: TableEngineRef,
}

impl<C: Manager + 'static> CreateInterpreter<C> {
    pub fn create(
        ctx: Context,
        plan: CreateTablePlan,
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

impl<C: Manager> CreateInterpreter<C> {
    async fn execute_create(self: Box<Self>) -> Result<Output> {
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

        let CreateTablePlan {
            engine,
            table,
            table_schema,
            if_not_exists,
            options,
        } = self.plan;

        let request = CreateTableRequest {
            catalog_name: catalog.name().to_string(),
            schema_name: schema.name().to_string(),
            schema_id: schema.id(),
            table_name: table.clone(),
            table_schema,
            engine,
            options,
            state: TableState::Stable,
        };

        let opts = CreateOptions {
            table_engine: self.table_engine,
            create_if_not_exists: if_not_exists,
        };

        schema
            .create_table(request, opts)
            .await
            .context(SchemaCreateTable { table })?;

        Ok(Output::AffectedRows(1))
    }
}

// TODO(yingwen): Wrap a method that returns self::Result, simplify some code to
// converting self::Error to super::Error
#[async_trait]
impl<C: Manager> Interpreter for CreateInterpreter<C> {
    async fn execute(self: Box<Self>) -> InterpreterResult<Output> {
        self.execute_create().await.context(Create)
    }
}
