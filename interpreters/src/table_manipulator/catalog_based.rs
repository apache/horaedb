// Copyright 2022-2023 CeresDB Project Authors. Licensed under Apache-2.0.

use async_trait::async_trait;
use catalog::{
    schema::{CreateOptions, CreateTableRequest, DropOptions, DropTableRequest},
    table_operator::TableOperator,
};
use common_types::table::DEFAULT_SHARD_ID;
use snafu::{ensure, ResultExt};
use sql::plan::{CreateTablePlan, DropTablePlan};
use table_engine::engine::{TableEngineRef, TableState};

use crate::{
    context::Context,
    interpreter::Output,
    table_manipulator::{
        PartitionTableNotSupported, Result, TableManipulator, TableOperator as TableOperatorErr,
    },
};

pub struct TableManipulatorImpl {
    table_operator: TableOperator,
}

impl TableManipulatorImpl {
    pub fn new(table_operator: TableOperator) -> Self {
        Self { table_operator }
    }
}

#[async_trait]
impl TableManipulator for TableManipulatorImpl {
    async fn create_table(
        &self,
        ctx: Context,
        plan: CreateTablePlan,
        table_engine: TableEngineRef,
    ) -> Result<Output> {
        ensure!(
            plan.partition_info.is_none(),
            PartitionTableNotSupported { table: plan.table }
        );
        let default_catalog = ctx.default_catalog();
        let default_schema = ctx.default_schema();

        let CreateTablePlan {
            engine,
            table,
            table_schema,
            if_not_exists,
            options,
            ..
        } = plan;

        let request = CreateTableRequest {
            catalog_name: default_catalog.to_string(),
            schema_name: default_schema.to_string(),
            table_name: table.clone(),
            table_id: None,
            table_schema,
            engine,
            options,
            state: TableState::Stable,
            shard_id: DEFAULT_SHARD_ID,
            partition_info: None,
        };

        let opts = CreateOptions {
            table_engine,
            create_if_not_exists: if_not_exists,
        };

        let _ = self
            .table_operator
            .create_table_on_shard(request, opts)
            .await
            .context(TableOperatorErr)?;

        Ok(Output::AffectedRows(0))
    }

    async fn drop_table(
        &self,
        ctx: Context,
        plan: DropTablePlan,
        table_engine: TableEngineRef,
    ) -> Result<Output> {
        let default_catalog = ctx.default_catalog();
        let default_schema = ctx.default_schema();

        let table = plan.table;
        let request = DropTableRequest {
            catalog_name: default_catalog.to_string(),
            schema_name: default_schema.to_string(),
            table_name: table.clone(),
            engine: plan.engine,
        };

        let opts = DropOptions { table_engine };

        self.table_operator
            .drop_table_on_shard(request, opts)
            .await
            .context(TableOperatorErr)?;

        Ok(Output::AffectedRows(0))
    }
}
