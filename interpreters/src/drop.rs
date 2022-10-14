// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

//! Interpreter for drop statements

use async_trait::async_trait;
use snafu::{Backtrace, ResultExt, Snafu};
use sql::plan::DropTablePlan;
use table_engine::engine::TableEngineRef;

use crate::{
    context::Context,
    interpreter::{Drop, Interpreter, InterpreterPtr, Output, Result as InterpreterResult},
    table_dropper::TableDropperRef,
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
pub struct DropInterpreter {
    ctx: Context,
    plan: DropTablePlan,
    table_engine: TableEngineRef,
    table_dropper: TableDropperRef,
}

impl DropInterpreter {
    pub fn create(
        ctx: Context,
        plan: DropTablePlan,
        table_engine: TableEngineRef,
        table_dropper: TableDropperRef,
    ) -> InterpreterPtr {
        Box::new(Self {
            ctx,
            plan,
            table_engine,
            table_dropper,
        })
    }
}

impl DropInterpreter {
    async fn execute_drop(self: Box<Self>) -> Result<Output> {
        self.table_dropper
            .drop_table(self.ctx, self.plan, self.table_engine)
            .await
    }
}

// TODO(yingwen): Wrap a method that returns self::Result, simplify some code to
// converting self::Error to super::Error
#[async_trait]
impl Interpreter for DropInterpreter {
    async fn execute(self: Box<Self>) -> InterpreterResult<Output> {
        self.execute_drop().await.context(Drop)
    }
}
