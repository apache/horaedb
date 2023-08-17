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

use datafusion::execution::{
    runtime_env::{RuntimeConfig, RuntimeEnv},
    FunctionRegistry,
};

use crate::{
    datafusion_impl::{
        encoding::DataFusionPhysicalPlanEncoderImpl,
        physical_planner::DatafusionPhysicalPlannerImpl,
    },
    encoding::PhysicalPlanEncoderRef,
    executor::{ExecutorImpl, ExecutorRef},
    physical_planner::PhysicalPlannerRef,
    Config, QueryEngine,
};

pub mod encoding;
pub mod logical_optimizer;
pub mod physical_optimizer;
pub mod physical_plan;
pub mod physical_plan_extension;
pub mod physical_planner;
pub mod physical_planner_extension;

use crate::error::*;

#[derive(Debug)]
pub struct DatafusionQueryEngineImpl {
    physical_planner: PhysicalPlannerRef,
    executor: ExecutorRef,
    physical_plan_encoder: PhysicalPlanEncoderRef,
}

impl DatafusionQueryEngineImpl {
    pub fn new(
        config: Config,
        runtime_config: RuntimeConfig,
        function_registry: Arc<dyn FunctionRegistry + Send + Sync>,
    ) -> Result<Self> {
        let runtime_env = Arc::new(RuntimeEnv::new(runtime_config).unwrap());

        let physical_planner = Arc::new(DatafusionPhysicalPlannerImpl::new(
            config,
            runtime_env.clone(),
        ));
        let executor = Arc::new(ExecutorImpl);
        let physical_plan_encoder = Arc::new(DataFusionPhysicalPlanEncoderImpl::new(
            runtime_env,
            function_registry,
        ));

        Ok(Self {
            physical_planner,
            executor,
            physical_plan_encoder,
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

    fn physical_plan_encoder(&self) -> PhysicalPlanEncoderRef {
        self.physical_plan_encoder.clone()
    }
}
