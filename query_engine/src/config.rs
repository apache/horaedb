// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

// FIXME: Use cpu number as the default parallelism
const DEFAULT_READ_PARALLELISM: usize = 8;

#[derive(Debug, Clone)]
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
