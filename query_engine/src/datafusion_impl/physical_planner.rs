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
    },
    error::*,
    physical_planner::{PhysicalPlanPtr, PhysicalPlanner},
    Config,
};

/// Physical planner based on datafusion
#[derive(Clone)]
pub struct DatafusionPhysicalPlannerImpl {
    config: Config,
}

impl DatafusionPhysicalPlannerImpl {
    pub fn new(config: Config) -> Self {
        Self { config }
    }

    pub fn build_df_session_ctx(&self, config: &Config, ctx: &Context) -> SessionContext {
        let timeout = ctx
            .deadline
            .map(|deadline| deadline.duration_since(Instant::now()).as_millis() as u64);
        let ceresdb_options = CeresdbOptions {
            request_id: ctx.request_id.as_u64(),
            request_timeout: timeout,
        };
        let mut df_session_config = SessionConfig::new()
            .with_default_catalog_and_schema(
                ctx.default_catalog.clone(),
                ctx.default_schema.clone(),
            )
            .with_target_partitions(config.read_parallelism);

        df_session_config
            .options_mut()
            .extensions
            .insert(ceresdb_options);

        // Using default logcial optimizer, if want to add more custom rule, using
        // `add_optimizer_rule` to add.
        let state =
            SessionState::with_config_rt(df_session_config, Arc::new(RuntimeEnv::default()))
                .with_query_planner(Arc::new(QueryPlannerAdapter));

        // Register analyzer rules
        let state = Self::register_analyzer_rules(state);

        // Register iox optimizers, used by influxql.
        let state = influxql_query::logical_optimizer::register_iox_logical_optimizers(state);

        SessionContext::with_state(state)
    }

    // TODO: this is not used now, bug of RepartitionAdapter is already fixed in
    // datafusion itself. Remove this code in future.
    #[allow(dead_code)]
    fn apply_adapters_for_physical_optimize_rules(
        default_rules: &[Arc<dyn PhysicalOptimizerRule + Send + Sync>],
    ) -> Vec<Arc<dyn PhysicalOptimizerRule + Send + Sync>> {
        let mut new_rules = Vec::with_capacity(default_rules.len());
        for rule in default_rules {
            new_rules.push(physical_optimizer::may_adapt_optimize_rule(rule.clone()))
        }

        new_rules
    }

    fn register_analyzer_rules(mut state: SessionState) -> SessionState {
        // Our analyzer has high priority, so first add we custom rules, then add the
        // default ones.
        state = state.with_analyzer_rules(vec![Arc::new(TypeConversion)]);
        for rule in Analyzer::new().rules {
            state = state.add_analyzer_rule(rule);
        }

        state
    }
}

#[async_trait]
impl PhysicalPlanner for DatafusionPhysicalPlannerImpl {
    async fn plan(&self, ctx: &Context, logical_plan: QueryPlan) -> Result<PhysicalPlanPtr> {
        // Register catalogs to datafusion execution context.
        let catalogs = CatalogProviderAdapter::new_adapters(logical_plan.tables.clone());
        let df_ctx = self.build_df_session_ctx(&self.config, ctx);
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
        let physical_plan = DataFusionPhysicalPlanImpl::with_plan(df_ctx.clone(), exec_plan);

        Ok(Box::new(physical_plan))
    }
}
