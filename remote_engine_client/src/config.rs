// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

//! Config for [Client]

use common_util::config::ReadableDuration;

pub struct Config {
    pub connect_timeout: ReadableDuration,
    pub channel_pool_max_size: usize,
    pub channel_keep_alive_while_idle: bool,
    pub channel_keep_alive_timeout: ReadableDuration,
    pub channel_keep_alive_interval: ReadableDuration,
}
