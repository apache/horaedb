// Copyright 2022-2023 CeresDB Project Authors. Licensed under Apache-2.0.

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

pub const MEMORY_ENGINE_TYPE: &str = "Memory";
pub const ANALYTIC_ENGINE_TYPE: &str = "Analytic";
pub const PARTITION_TABLE_ENGINE_TYPE: &str = "PartitionTable";
