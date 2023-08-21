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

use std::{sync::Arc, time::Instant};

use async_trait::async_trait;
use datafusion::{
    execution::{context::SessionState, runtime_env::RuntimeEnv},
    optimizer::analyzer::Analyzer,
    physical_optimizer::PhysicalOptimizerRule,
    prelude::{SessionConfig, SessionContext},
};
use generic_error::BoxError;
use query_frontend::{plan::QueryPlan, provider::CatalogProviderAdapter};
use snafu::ResultExt;
use table_engine::provider::CeresdbOptions;

use crate::{
    context::Context,
    datafusion_impl::{
        logical_optimizer::type_conversion::TypeConversion, physical_optimizer,
        physical_plan::DataFusionPhysicalPlanImpl, physical_planner_extension::QueryPlannerAdapter,
        DfContextBuilder,
    },
    error::*,
    physical_planner::{PhysicalPlanPtr, PhysicalPlanner},
    Config,
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
}

#[async_trait]
impl PhysicalPlanner for DatafusionPhysicalPlannerImpl {
    async fn plan(&self, ctx: &Context, logical_plan: QueryPlan) -> Result<PhysicalPlanPtr> {
        // Register catalogs to datafusion execution context.
        let catalogs = CatalogProviderAdapter::new_adapters(logical_plan.tables.clone());
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
        let physical_plan = DataFusionPhysicalPlanImpl::new(exec_plan);

        Ok(Box::new(physical_plan))
    }
}
