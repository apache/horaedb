// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

//! Interpreter for create statements

use async_trait::async_trait;
use snafu::{Backtrace, ResultExt, Snafu};
use sql::plan::CreateTablePlan;
use table_engine::engine::TableEngineRef;

use crate::{
    context::Context,
    interpreter::{Create, Interpreter, InterpreterPtr, Output, Result as InterpreterResult},
    table_creator::TableCreatorRef,
};

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

    #[snafu(display("Failed to allocate table id, err:{}", source))]
    AllocTableId { source: catalog::schema::Error },
}

define_result!(Error);

/// Create interpreter
pub struct CreateInterpreter {
    ctx: Context,
    plan: CreateTablePlan,
    table_engine: TableEngineRef,
    table_creator: TableCreatorRef,
}

impl CreateInterpreter {
    pub fn create(
        ctx: Context,
        plan: CreateTablePlan,
        table_engine: TableEngineRef,
        table_creator: TableCreatorRef,
    ) -> InterpreterPtr {
        Box::new(Self {
            ctx,
            plan,
            table_engine,
            table_creator,
        })
    }
}

impl CreateInterpreter {
    async fn execute_create(self: Box<Self>) -> Result<Output> {
        self.table_creator
            .create_table(self.ctx, self.plan, self.table_engine)
            .await
    }
}

// TODO(yingwen): Wrap a method that returns self::Result, simplify some code to
// converting self::Error to super::Error
#[async_trait]
impl Interpreter for CreateInterpreter {
    async fn execute(self: Box<Self>) -> InterpreterResult<Output> {
        self.execute_create().await.context(Create)
    }
}
