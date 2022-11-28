// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

//! Config for wal on message queue

use common_util::config::ReadableDuration;
use serde_derive::{Deserialize, Serialize};

// TODO: add more needed config items.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct Config {
    pub clean_period: ReadableDuration,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            clean_period: ReadableDuration::millis(3600 * 1000),
        }
    }
}
