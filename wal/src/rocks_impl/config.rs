// Copyright 2023 CeresDB Project Authors. Licensed under Apache-2.0.

//! RocksDB Config

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct Config {
    pub max_subcompactions: u32,
    pub max_background_jobs: i32,
    pub enable_statistics: bool,
    pub write_buffer_size: u64,
    pub max_write_buffer_number: i32,
    pub level_zero_file_num_compaction_trigger: i32,
    pub level_zero_slowdown_writes_trigger: i32,
    pub level_zero_stop_writes_trigger: i32,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            // Same with rocksdb
            // https://github.com/facebook/rocksdb/blob/v6.4.6/include/rocksdb/options.h#L537
            max_subcompactions: 1,
            max_background_jobs: 2,
            enable_statistics: false,
            write_buffer_size: 64 << 20,
            max_write_buffer_number: 2,
            level_zero_file_num_compaction_trigger: 4,
            level_zero_slowdown_writes_trigger: 20,
            level_zero_stop_writes_trigger: 36,
        }
    }
}
