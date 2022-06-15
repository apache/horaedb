// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

//! Instance contains shared states of service

use std::sync::Arc;

use table_engine::engine::TableEngineRef;
use df_operator::registry::FunctionRegistryRef;

use crate::limiter::Limiter;

/// A cluster instance. Usually there is only one instance per cluster
///
/// C: catalog::manager::Manager
/// Q: query_engine::executor::Executor
pub struct Instance<C, Q> {
    pub catalog_manager: C,
    pub query_executor: Q,
    pub table_engine: TableEngineRef,
    // User defined functions registry.
    pub function_registry: FunctionRegistryRef,
    pub limiter: Limiter,
}

/// A reference counted instance pointer
pub type InstanceRef<C, Q> = Arc<Instance<C, Q>>;
