// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

use std::sync::Arc;

use async_trait::async_trait;
use sql::plan::DropTablePlan;
use table_engine::engine::TableEngineRef;

use crate::{context::Context, drop::Result, interpreter::Output};

pub mod catalog_based;
pub mod meta_based;

pub type TableDropperRef = Arc<dyn TableDropper + Send + Sync>;

#[async_trait]
pub trait TableDropper {
    async fn drop_table(
        &self,
        ctx: Context,
        plan: DropTablePlan,
        table_engine: TableEngineRef,
    ) -> Result<Output>;
}
