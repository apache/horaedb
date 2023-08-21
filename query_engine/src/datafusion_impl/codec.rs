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

use std::{fmt, sync::Arc};

use bytes_ext::Bytes;
use datafusion::execution::{runtime_env::RuntimeEnv, FunctionRegistry};

use crate::{codec::PhysicalPlanCodec, error::*, physical_planner::PhysicalPlanPtr};

/// Datafusion encoder powered by `datafusion-proto`
// TODO: replace `datafusion-proto` with `substrait`?
// TODO: make use of it.
pub struct DataFusionPhysicalPlanEncoderImpl {
    _runtime_env: Arc<RuntimeEnv>,
    _function_registry: Arc<dyn FunctionRegistry + Send + Sync>,
}

impl DataFusionPhysicalPlanEncoderImpl {
    // TODO: use `SessionContext` to init it.
    pub fn new(
        runtime_env: Arc<RuntimeEnv>,
        function_registry: Arc<dyn FunctionRegistry + Send + Sync>,
    ) -> Self {
        Self {
            _runtime_env: runtime_env,
            _function_registry: function_registry,
        }
    }
}

impl fmt::Debug for DataFusionPhysicalPlanEncoderImpl {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "DataFusionPhysicalPlanEncoderImpl")
    }
}

impl PhysicalPlanCodec for DataFusionPhysicalPlanEncoderImpl {
    fn encode(&self, _plan: &crate::physical_planner::PhysicalPlanPtr) -> Result<Bytes> {
        todo!()
    }

    fn decode(&self, _bytes: &[u8]) -> Result<PhysicalPlanPtr> {
        todo!()
    }
}
