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

use std::sync::Arc;

use async_trait::async_trait;
use generic_error::BoxError;
use query_frontend::{plan::QueryPlan, provider::CatalogProviderAdapter};
use snafu::ResultExt;

use crate::{
    context::Context,
    datafusion_impl::{
        physical_plan::{DataFusionPhysicalPlanAdapter, TypedPlan},
        DfContextBuilder,
    },
    error::*,
    physical_planner::{PhysicalPlanPtr, PhysicalPlanner},
};

/// Physical planner based on datafusion
#[derive(Debug, Clone)]
pub struct DatafusionPhysicalPlannerImpl {
    df_ctx_builder: Arc<DfContextBuilder>,
}

impl DatafusionPhysicalPlannerImpl {
    pub fn new(df_ctx_builder: Arc<DfContextBuilder>) -> Self {
        Self { df_ctx_builder }
    }

    fn has_partitioned_table(logical_plan: &QueryPlan) -> bool {
        let mut has_partitioned_table = false;
        let _ = logical_plan
            .tables
            .visit(|_, table| -> std::result::Result<(), ()> {
                if table.partition_info().is_some() {
                    has_partitioned_table = true;
                }
                Ok(())
            });

        has_partitioned_table
    }
}

#[async_trait]
impl PhysicalPlanner for DatafusionPhysicalPlannerImpl {
    // TODO: we should modify `QueryPlan` to support create remote plan here.
    async fn plan(&self, ctx: &Context, logical_plan: QueryPlan) -> Result<PhysicalPlanPtr> {
        // Register catalogs to datafusion execution context.
        let catalogs = CatalogProviderAdapter::new_adapters(logical_plan.tables.clone());
        // TODO: maybe we should not build `SessionContext` in each physical plan's
        // building. We need to do so because we place some dynamic
        // information(such as `timeout`) in `SessionConfig`, maybe it is better
        // to remove it to `TaskContext`.
        let df_ctx = self.df_ctx_builder.build(ctx);
        for (name, catalog) in catalogs {
            df_ctx.register_catalog(&name, Arc::new(catalog));
        }

        // Generate physical plan.
        let exec_plan = df_ctx
            .state()
            .create_physical_plan(&logical_plan.df_plan)
            .await
            .box_err()
            .context(PhysicalPlannerWithCause { msg: None })?;

        // Decide if partitioned table exists.
        let has_partitioned_table =
            DatafusionPhysicalPlannerImpl::has_partitioned_table(&logical_plan);
        let typed_plan = if has_partitioned_table {
            TypedPlan::Partitioned(exec_plan)
        } else {
            TypedPlan::Normal(exec_plan)
        };
        let physical_plan = DataFusionPhysicalPlanAdapter::new(typed_plan);

        Ok(Box::new(physical_plan))
    }
}
