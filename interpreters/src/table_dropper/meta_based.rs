// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

use async_trait::async_trait;
use sql::plan::DropTablePlan;
use table_engine::engine::TableEngineRef;

use crate::{context::Context, drop::Result, interpreter::Output, table_dropper::TableDropper};

pub struct TableDropperImpl {}

#[async_trait]
impl TableDropper for TableDropperImpl {
    async fn drop_table(
        &self,
        _ctx: Context,
        _plan: DropTablePlan,
        _table_engine: TableEngineRef,
    ) -> Result<Output> {
        todo!()
    }
}
