// Copyright 2022-2023 CeresDB Project Authors. Licensed under Apache-2.0.

//! Utilities for benchmarks.

use common_types::SequenceNumber;

pub mod config;
pub mod merge_memtable_bench;
pub mod merge_sst_bench;
pub mod parquet_bench;
pub mod scan_memtable_bench;
pub mod sst_bench;
pub mod sst_tools;
pub mod util;
pub mod wal_write_bench;

pub(crate) const INIT_SEQUENCE: SequenceNumber = 1;
