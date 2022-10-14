// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

use async_trait::async_trait;
use sql::plan::CreateTablePlan;
use table_engine::engine::TableEngineRef;

use crate::{context::Context, create::Result, interpreter::Output, table_creator::TableCreator};

pub struct TableCreatorImpl {}

#[async_trait]
impl TableCreator for TableCreatorImpl {
    async fn create_table(
        &self,
        _ctx: Context,
        _plan: CreateTablePlan,
        _table_engine: TableEngineRef,
    ) -> Result<Output> {
        todo!()
    }
}
