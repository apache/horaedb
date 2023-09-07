// Copyright 2023 The CeresDB Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use async_trait::async_trait;
use catalog::{
    schema::{CreateOptions, CreateTableRequest, DropOptions, DropTableRequest},
    table_operator::TableOperator,
};
use common_types::table::DEFAULT_SHARD_ID;
use query_frontend::plan::{CreateTablePlan, DropTablePlan};
use snafu::{ensure, ResultExt};
use table_engine::engine::{CreateTableParams, TableEngineRef, TableState};

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

        let params = CreateTableParams {
            catalog_name: default_catalog.to_string(),
            schema_name: default_schema.to_string(),
            table_name: table.clone(),
            table_schema,
            engine,
            table_options: options,
            partition_info: None,
        };
        let request = CreateTableRequest {
            params,
            table_id: None,
            state: TableState::Stable,
            shard_id: DEFAULT_SHARD_ID,
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
