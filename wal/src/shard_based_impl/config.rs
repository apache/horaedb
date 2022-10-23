// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

//! Kafka implementation's config

use serde_derive::{Deserialize, Serialize};

/// Generic client config that is used for consumers, producers as well as admin
/// operations (like "create topic").
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct Config {
    pub channel_size: usize,
    pub splitter_config: SplitterConfig,
    pub fetcher_config: FetcherConfig,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            channel_size: 500,
            splitter_config: SplitterConfig::default(),
            fetcher_config: FetcherConfig::default(),
        }
    }
}

/// Config for log splitter
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct SplitterConfig {
    pub read_log_batch_size: usize,
    pub max_per_table_wait_ms: u64,
}

impl Default for SplitterConfig {
    fn default() -> Self {
        Self {
            read_log_batch_size: 500,
            max_per_table_wait_ms: 5000,
        }
    }
}

/// Config for log fetcher
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct FetcherConfig {
    pub fetch_batch_size: usize,
}

impl Default for FetcherConfig {
    fn default() -> Self {
        Self {
            fetch_batch_size: 500,
        }
    }
}
