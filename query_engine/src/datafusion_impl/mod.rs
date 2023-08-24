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

use std::{fmt, sync::Arc, time::Instant};

use catalog::manager::ManagerRef as CatalogManager;
use datafusion::{
    execution::{
        context::{QueryPlanner, SessionState},
        runtime_env::{RuntimeConfig, RuntimeEnv},
        FunctionRegistry,
    },
    optimizer::analyzer::Analyzer,
    physical_optimizer::PhysicalOptimizerRule,
    prelude::{SessionConfig, SessionContext},
};
use table_engine::{provider::CeresdbOptions, remote::RemoteEngineRef};

use crate::{
    context::Context,
    datafusion_impl::{
        executor::DatafusionExecutorImpl, logical_optimizer::type_conversion::TypeConversion,
        physical_planner::DatafusionPhysicalPlannerImpl,
        physical_planner_extension::QueryPlannerAdapter, task_context::Preprocessor,
    },
    executor::ExecutorRef,
    physical_planner::PhysicalPlannerRef,
    Config, QueryEngine,
};

pub mod executor;
pub mod logical_optimizer;
pub mod physical_optimizer;
pub mod physical_plan;
pub mod physical_plan_extension;
pub mod physical_planner;
pub mod physical_planner_extension;
pub mod task_context;

use crate::error::*;

#[derive(Debug)]
pub struct DatafusionQueryEngineImpl {
    physical_planner: PhysicalPlannerRef,
    executor: ExecutorRef,
}

impl DatafusionQueryEngineImpl {
    pub fn new(
        config: Config,
        runtime_config: RuntimeConfig,
        function_registry: Arc<dyn FunctionRegistry + Send + Sync>,
        remote_engine: RemoteEngineRef,
        catalog_manager: CatalogManager,
    ) -> Result<Self> {
        let runtime_env = Arc::new(RuntimeEnv::new(runtime_config).unwrap());
        let df_physical_planner = Arc::new(QueryPlannerAdapter);
        let df_ctx_builder = Arc::new(DfContextBuilder::new(
            config,
            runtime_env.clone(),
            df_physical_planner,
        ));

        // Physical planner
        let physical_planner = Arc::new(DatafusionPhysicalPlannerImpl::new(df_ctx_builder.clone()));

        // Executor
        let preprocessor = Arc::new(Preprocessor::new(
            remote_engine,
            catalog_manager,
            runtime_env.clone(),
            function_registry.clone(),
        ));
        let executor = Arc::new(DatafusionExecutorImpl::new(df_ctx_builder, preprocessor));

        Ok(Self {
            physical_planner,
            executor,
        })
    }
}

impl QueryEngine for DatafusionQueryEngineImpl {
    fn physical_planner(&self) -> PhysicalPlannerRef {
        self.physical_planner.clone()
    }

    fn executor(&self) -> ExecutorRef {
        self.executor.clone()
    }
}

/// Datafusion context builder
#[derive(Clone)]
pub struct DfContextBuilder {
    config: Config,
    runtime_env: Arc<RuntimeEnv>,
    physical_planner: Arc<dyn QueryPlanner + Send + Sync>,
}

impl fmt::Debug for DfContextBuilder {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DfContextBuilder")
            .field("config", &self.config)
            .field("runtime_env", &self.runtime_env)
            .field("physical_planner", &"QueryPlannerAdapter")
            .finish()
    }
}

impl DfContextBuilder {
    pub fn new(
        config: Config,
        runtime_env: Arc<RuntimeEnv>,
        physical_planner: Arc<dyn QueryPlanner + Send + Sync>,
    ) -> Self {
        Self {
            config,
            runtime_env,
            physical_planner,
        }
    }

    pub fn build(&self, ctx: &Context) -> SessionContext {
        let timeout = ctx
            .deadline
            .map(|deadline| deadline.duration_since(Instant::now()).as_millis() as u64);
        let ceresdb_options = CeresdbOptions {
            request_id: ctx.request_id.as_u64(),
            request_timeout: timeout,
            default_catalog: ctx.default_catalog.clone(),
            default_schema: ctx.default_schema.clone(),
        };
        let mut df_session_config = SessionConfig::new()
            .with_default_catalog_and_schema(
                ctx.default_catalog.clone(),
                ctx.default_schema.clone(),
            )
            .with_target_partitions(self.config.read_parallelism);

        df_session_config
            .options_mut()
            .extensions
            .insert(ceresdb_options);

        // Using default logcial optimizer, if want to add more custom rule, using
        // `add_optimizer_rule` to add.
        let state = SessionState::with_config_rt(df_session_config, self.runtime_env.clone())
            .with_query_planner(self.physical_planner.clone());

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
