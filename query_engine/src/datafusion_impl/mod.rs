// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use std::{sync::Arc, time::Instant};

use catalog::manager::ManagerRef as CatalogManager;
use datafusion::{
    execution::{
        context::SessionState,
        runtime_env::{RuntimeConfig, RuntimeEnv},
        FunctionRegistry,
    },
    prelude::{SessionConfig, SessionContext},
};
use df_engine_extensions::codec::PhysicalExtensionCodecImpl;
use table_engine::{provider::HoraeDBOptions, remote::RemoteEngineRef};

use crate::{
    context::Context,
    datafusion_impl::{
        executor::DatafusionExecutorImpl, physical_planner::DatafusionPhysicalPlannerImpl,
        physical_planner_extension::QueryPlannerAdapter, task_context::Preprocessor,
    },
    executor::ExecutorRef,
    physical_planner::PhysicalPlannerRef,
    Config, QueryEngine,
};

pub mod executor;
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
        let df_ctx_builder = Arc::new(DfContextBuilder::new(config, runtime_env.clone()));
        let physical_planner = Arc::new(DatafusionPhysicalPlannerImpl::new(
            df_ctx_builder.clone(),
            df_physical_planner,
        ));

        // Executor
        let extension_codec = Arc::new(PhysicalExtensionCodecImpl::new());
        let preprocessor = Arc::new(Preprocessor::new(
            remote_engine,
            catalog_manager,
            runtime_env.clone(),
            function_registry.clone(),
            extension_codec,
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
#[derive(Debug, Clone)]
pub struct DfContextBuilder {
    config: Config,
    runtime_env: Arc<RuntimeEnv>,
}

impl DfContextBuilder {
    pub fn new(config: Config, runtime_env: Arc<RuntimeEnv>) -> Self {
        Self {
            config,
            runtime_env,
        }
    }

    pub fn build(&self, ctx: &Context) -> SessionContext {
        let timeout = ctx
            .deadline
            .map(|deadline| deadline.duration_since(Instant::now()).as_millis() as u64);
        let options = HoraeDBOptions {
            request_id: ctx.request_id.to_string(),
            request_timeout: timeout,
            default_catalog: ctx.default_catalog.clone(),
            default_schema: ctx.default_schema.clone(),
            priority: ctx.priority,
        };
        let mut df_session_config = SessionConfig::new()
            .with_default_catalog_and_schema(
                ctx.default_catalog.clone(),
                ctx.default_schema.clone(),
            )
            .with_target_partitions(self.config.read_parallelism);

        df_session_config.options_mut().extensions.insert(options);

        // Using default logcial optimizer, if want to add more custom rule, using
        // `add_optimizer_rule` to add.
        let state = SessionState::with_config_rt(df_session_config, self.runtime_env.clone());
        SessionContext::with_state(state)
    }
}
