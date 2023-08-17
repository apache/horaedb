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

//! Function registry.

use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

use datafusion::{
    error::{DataFusionError, Result as DfResult},
    execution::FunctionRegistry as DfFunctionRegistry,
    logical_expr::{
        AggregateUDF as DfAggregateUDF, ScalarUDF as DfScalarUDF, WindowUDF as DfWindowUDF,
    },
};
use macros::define_result;
use snafu::{ensure, Backtrace, Snafu};

use crate::{scalar::ScalarUdf, udaf::AggregateUdf, udfs};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Udf already exists, name:{}.\nBacktrace:\n{}", name, backtrace))]
    UdfExists { name: String, backtrace: Backtrace },
}

define_result!(Error);

/// A registry knows how to build logical expressions out of user-defined
/// function' names
// TODO: maybe unnecessary to define inner trait rather than using datafusion's?
pub trait FunctionRegistry {
    fn register_udf(&mut self, udf: ScalarUdf) -> Result<()>;

    fn register_udaf(&mut self, udaf: AggregateUdf) -> Result<()>;

    fn find_udf(&self, name: &str) -> Result<Option<ScalarUdf>>;

    fn find_udaf(&self, name: &str) -> Result<Option<AggregateUdf>>;

    fn list_udfs(&self) -> HashSet<String>;

    // TODO: can we remove restriction about `Send` and `Sync`?
    fn to_df_function_registry(self: Arc<Self>) -> Arc<dyn DfFunctionRegistry + Send + Sync>;
}

/// Default function registry.
#[derive(Debug, Default)]
pub struct FunctionRegistryImpl {
    scalar_functions: HashMap<String, ScalarUdf>,
    aggregate_functions: HashMap<String, AggregateUdf>,
}

impl FunctionRegistryImpl {
    pub fn new() -> Self {
        Self::default()
    }

    /// Load all provided udfs.
    pub fn load_functions(&mut self) -> Result<()> {
        udfs::register_all_udfs(self)
    }
}

impl FunctionRegistry for FunctionRegistryImpl {
    fn register_udf(&mut self, udf: ScalarUdf) -> Result<()> {
        ensure!(
            !self.scalar_functions.contains_key(udf.name()),
            UdfExists { name: udf.name() }
        );

        self.scalar_functions.insert(udf.name().to_string(), udf);

        Ok(())
    }

    fn register_udaf(&mut self, udaf: AggregateUdf) -> Result<()> {
        ensure!(
            !self.aggregate_functions.contains_key(udaf.name()),
            UdfExists { name: udaf.name() }
        );

        self.aggregate_functions
            .insert(udaf.name().to_string(), udaf);

        Ok(())
    }

    fn find_udf(&self, name: &str) -> Result<Option<ScalarUdf>> {
        let udf = self.scalar_functions.get(name).cloned();
        Ok(udf)
    }

    fn find_udaf(&self, name: &str) -> Result<Option<AggregateUdf>> {
        let udaf = self.aggregate_functions.get(name).cloned();
        Ok(udaf)
    }

    fn list_udfs(&self) -> HashSet<String> {
        self.scalar_functions.keys().cloned().collect()
    }

    fn to_df_function_registry(self: Arc<Self>) -> Arc<dyn DfFunctionRegistry + Send + Sync> {
        Arc::new(DfFunctionRegistryAdapter(self))
    }
}

struct DfFunctionRegistryAdapter(FunctionRegistryRef);

impl DfFunctionRegistry for DfFunctionRegistryAdapter {
    fn udfs(&self) -> HashSet<String> {
        self.0.list_udfs()
    }

    fn udf(&self, name: &str) -> DfResult<Arc<DfScalarUDF>> {
        self.0
            .find_udf(name)
            .map_err(|e| DataFusionError::Internal(format!("failed to find udf, err:{e}")))?
            .ok_or(DataFusionError::Internal(format!(
                "udf not found, name:{name}"
            )))
            .map(|f| f.to_datafusion_udf())
    }

    fn udaf(&self, name: &str) -> DfResult<Arc<DfAggregateUDF>> {
        self.0
            .find_udaf(name)
            .map_err(|e| DataFusionError::Internal(format!("failed to find udaf, err:{e}")))?
            .ok_or(DataFusionError::Internal(format!(
                "udaf not found, name:{name}"
            )))
            .map(|f| f.to_datafusion_udaf())
    }

    fn udwf(&self, _name: &str) -> DfResult<Arc<DfWindowUDF>> {
        Err(DataFusionError::Internal(
            "no udwfs defined now".to_string(),
        ))
    }
}

pub type FunctionRegistryRef = Arc<dyn FunctionRegistry + Send + Sync>;
