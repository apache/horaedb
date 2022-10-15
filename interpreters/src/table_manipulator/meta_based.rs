// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

use async_trait::async_trait;
use sql::plan::{CreateTablePlan, DropTablePlan};
use table_engine::engine::TableEngineRef;

use crate::{
    context::Context,
    interpreter::Output,
    table_manipulator::{Result, TableManipulator},
};

pub struct TableManipulatorImpl {}

#[async_trait]
impl TableManipulator for TableManipulatorImpl {
    async fn create_table(
        &self,
        _ctx: Context,
        _plan: CreateTablePlan,
        _table_engine: TableEngineRef,
    ) -> Result<Output> {
        todo!()
    }

    async fn drop_table(
        &self,
        _ctx: Context,
        _plan: DropTablePlan,
        _table_engine: TableEngineRef,
    ) -> Result<Output> {
        todo!()
    }
}
