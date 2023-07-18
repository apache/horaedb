// Copyright 2022-2023 CeresDB Project Authors. Licensed under Apache-2.0.

//! Config for [Client]

use std::str::FromStr;

use arrow_ext::ipc::CompressOptions;
use serde::{Deserialize, Serialize};
use time_ext::ReadableDuration;

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(default)]
pub struct Config {
    pub connect_timeout: ReadableDuration,
    pub channel_pool_max_size_per_partition: usize,
    pub channel_pool_partition_num: usize,
    pub channel_keep_alive_while_idle: bool,
    pub channel_keep_alive_timeout: ReadableDuration,
    pub channel_keep_alive_interval: ReadableDuration,
    pub route_cache_max_size_per_partition: usize,
    pub route_cache_partition_num: usize,
    pub compression: CompressOptions,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            connect_timeout: ReadableDuration::from_str("3s").unwrap(),
            channel_pool_max_size_per_partition: 16,
            channel_pool_partition_num: 16,
            channel_keep_alive_interval: ReadableDuration::from_str("600s").unwrap(),
            channel_keep_alive_timeout: ReadableDuration::from_str("3s").unwrap(),
            channel_keep_alive_while_idle: true,
            route_cache_max_size_per_partition: 16,
            route_cache_partition_num: 16,
            compression: CompressOptions::default(),
        }
    }
}
