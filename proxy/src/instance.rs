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

use std::sync::{atomic::AtomicU64, Arc};

use catalog::manager::ManagerRef;
use df_operator::registry::FunctionRegistryRef;
use interpreters::table_manipulator::TableManipulatorRef;
use query_engine::QueryEngineRef;
use table_engine::{engine::TableEngineRef, remote::RemoteEngineRef};

use crate::{limiter::Limiter, DynamicConfig};

/// A cluster instance. Usually there is only one instance per cluster
pub struct Instance {
    pub catalog_manager: ManagerRef,
    pub query_engine: QueryEngineRef,
    pub table_engine: TableEngineRef,
    pub partition_table_engine: TableEngineRef,
    // User defined functions registry.
    // TODO: remove it, it should be part of query engine...
    pub function_registry: FunctionRegistryRef,
    pub limiter: Limiter,
    pub table_manipulator: TableManipulatorRef,
    pub remote_engine_ref: RemoteEngineRef,
    pub dyn_config: Arc<DynamicConfig>,
}

/// A reference counted instance pointer
pub type InstanceRef = Arc<Instance>;
