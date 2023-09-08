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

//! Query engine
//!
//! Optimizes and executes logical plan

pub mod config;
pub mod context;
pub mod datafusion_impl;
pub mod error;
pub mod executor;
pub mod physical_planner;
use std::{fmt, sync::Arc};

use catalog::manager::ManagerRef;
use datafusion::execution::{runtime_env::RuntimeConfig, FunctionRegistry};
use snafu::OptionExt;
use table_engine::remote::RemoteEngineRef;

use crate::{
    config::Config, datafusion_impl::DatafusionQueryEngineImpl, error::*, executor::ExecutorRef,
    physical_planner::PhysicalPlannerRef,
};

/// Query engine
pub trait QueryEngine: fmt::Debug + Send + Sync {
    fn physical_planner(&self) -> PhysicalPlannerRef;

    fn executor(&self) -> ExecutorRef;
}

pub type QueryEngineRef = Arc<dyn QueryEngine>;

/// Query engine builder
#[derive(Default)]
pub struct QueryEngineBuilder {
    config: Option<Config>,
    df_function_registry: Option<Arc<dyn FunctionRegistry + Send + Sync>>,
    df_runtime_config: Option<RuntimeConfig>,
    catalog_manager: Option<ManagerRef>,
    remote_engine: Option<RemoteEngineRef>,
}

#[derive(Debug)]
pub enum QueryEngineType {
    Datafusion,
}

impl QueryEngineBuilder {
    pub fn config(mut self, config: Config) -> Self {
        self.config = Some(config);
        self
    }

    pub fn df_function_registry(
        mut self,
        df_function_registry: Arc<dyn FunctionRegistry + Send + Sync>,
    ) -> Self {
        self.df_function_registry = Some(df_function_registry);
        self
    }

    pub fn df_runtime_config(mut self, df_runtime_config: RuntimeConfig) -> Self {
        self.df_runtime_config = Some(df_runtime_config);
        self
    }

    pub fn catalog_manager(mut self, catalog_manager: ManagerRef) -> Self {
        self.catalog_manager = Some(catalog_manager);
        self
    }

    pub fn remote_engine(mut self, remote_engine: RemoteEngineRef) -> Self {
        self.remote_engine = Some(remote_engine);
        self
    }

    fn build_datafusion_query_engine(self) -> Result<QueryEngineRef> {
        // Check if necessary component exists.
        let config = self.config.with_context(|| InitNoCause {
            msg: "config not found".to_string(),
        })?;

        let df_function_registry = self.df_function_registry.with_context(|| InitNoCause {
            msg: "df_function_registry not found".to_string(),
        })?;

        let df_runtime_config = self.df_runtime_config.with_context(|| InitNoCause {
            msg: "df_runtime_config not found".to_string(),
        })?;

        let catalog_manager = self.catalog_manager.with_context(|| InitNoCause {
            msg: "catalog_manager not found".to_string(),
        })?;

        let remote_engine = self.remote_engine.with_context(|| InitNoCause {
            msg: "remote_engine not found".to_string(),
        })?;

        // Build engine.
        let df_query_engine = DatafusionQueryEngineImpl::new(
            config,
            df_runtime_config,
            df_function_registry,
            remote_engine,
            catalog_manager,
        )?;

        Ok(Arc::new(df_query_engine))
    }

    pub fn build(self, engine_type: QueryEngineType) -> Result<QueryEngineRef> {
        match engine_type {
            QueryEngineType::Datafusion => self.build_datafusion_query_engine(),
        }
    }
}
