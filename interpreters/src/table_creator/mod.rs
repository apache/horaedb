// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

use std::sync::Arc;

use async_trait::async_trait;
use sql::plan::CreateTablePlan;
use table_engine::engine::TableEngineRef;

use crate::{context::Context, create::Result, interpreter::Output};

pub mod catalog_based;
pub mod meta_based;

pub type TableCreatorRef = Arc<dyn TableCreator + Send + Sync>;

#[async_trait]
pub trait TableCreator {
    async fn create_table(
        &self,
        ctx: Context,
        plan: CreateTablePlan,
        table_engine: TableEngineRef,
    ) -> Result<Output>;
}
