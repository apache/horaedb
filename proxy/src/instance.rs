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

//! Instance contains shared states of service

use std::sync::Arc;

use catalog::manager::ManagerRef;
use df_operator::registry::FunctionRegistryRef;
use interpreters::table_manipulator::TableManipulatorRef;
use table_engine::{engine::TableEngineRef, remote::RemoteEngineRef};

use crate::limiter::Limiter;

/// A cluster instance. Usually there is only one instance per cluster
///
/// Q: query_engine::executor::Executor
pub struct Instance<Q> {
    pub catalog_manager: ManagerRef,
    pub query_executor: Q,
    pub table_engine: TableEngineRef,
    pub partition_table_engine: TableEngineRef,
    // User defined functions registry.
    pub function_registry: FunctionRegistryRef,
    pub limiter: Limiter,
    pub table_manipulator: TableManipulatorRef,
    pub remote_engine_ref: RemoteEngineRef,
}

/// A reference counted instance pointer
pub type InstanceRef<Q> = Arc<Instance<Q>>;
