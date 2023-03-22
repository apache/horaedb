// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

//! Table engine facade, provides read/write interfaces of table

#[macro_use]
extern crate common_util;

pub mod engine;
pub mod memory;
pub mod partition;
pub mod predicate;
pub mod provider;
pub mod proxy;
pub mod remote;
pub mod stream;
pub mod table;

/// Enable ttl key
pub const OPTION_KEY_ENABLE_TTL: &str = "enable_ttl";

pub const MEMORY_ENGINE_TYPE: &str = "Memory";
pub const ANALYTIC_ENGINE_TYPE: &str = "Analytic";
