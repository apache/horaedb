// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

//! Instance contains shared states of service

use std::sync::Arc;

use catalog::manager::ManagerRef;
use df_operator::registry::FunctionRegistryRef;
use interpreters::{table_creator::TableCreatorRef, table_dropper::TableDropperRef};
use table_engine::engine::TableEngineRef;

use crate::limiter::Limiter;

/// A cluster instance. Usually there is only one instance per cluster
///
/// Q: query_engine::executor::Executor
pub struct Instance<Q> {
    pub catalog_manager: ManagerRef,
    pub query_executor: Q,
    pub table_engine: TableEngineRef,
    // User defined functions registry.
    pub function_registry: FunctionRegistryRef,
    pub limiter: Limiter,
    pub table_creator: TableCreatorRef,
    pub table_dropper: TableDropperRef,
}

/// A reference counted instance pointer
pub type InstanceRef<Q> = Arc<Instance<Q>>;
