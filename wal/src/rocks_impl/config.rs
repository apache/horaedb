// Copyright 2023 CeresDB Project Authors. Licensed under Apache-2.0.

//! RocksDB Config

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct Config {
    pub max_background_jobs: i32,
    pub enable_statistics: bool,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            // Same with rocksdb
            // https://github.com/facebook/rocksdb/blob/v6.4.6/include/rocksdb/options.h#L537
            max_background_jobs: 2,
            enable_statistics: false,
        }
    }
}
