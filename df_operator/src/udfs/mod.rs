// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

//! UDFs

use crate::registry::{FunctionRegistry, Result};

mod thetasketch_distinct;
mod time_bucket;

pub fn register_all_udfs(registry: &mut dyn FunctionRegistry) -> Result<()> {
    // Register all udfs
    time_bucket::register_to_registry(registry)?;
    thetasketch_distinct::register_to_registry(registry)?;

    Ok(())
}
