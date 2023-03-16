// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

use serde::{Deserialize, Serialize};

// FIXME: Use cpu number as the default parallelism
const DEFAULT_READ_PARALLELISM: usize = 8;

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(default)]
pub struct Config {
    pub read_parallelism: usize,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            read_parallelism: DEFAULT_READ_PARALLELISM,
        }
    }
}
