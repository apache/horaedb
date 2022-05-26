// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

//! Context for instance

use std::{fmt, sync::Arc};

use parquet::{DataCacheRef, MetaCacheRef};
use table_engine::engine::EngineRuntimes;

use crate::Config;

/// Common context for instance
pub struct CommonContext {
    pub db_write_buffer_size: usize,
    pub space_write_buffer_size: usize,
}

/// Context for instance open
pub struct OpenContext {
    /// Engine config
    pub config: Config,

    /// Background job runtime
    pub runtimes: Arc<EngineRuntimes>,

    /// Sst meta data cache.
    pub meta_cache: Option<MetaCacheRef>,
    /// Sst page cache.
    pub data_cache: Option<DataCacheRef>,
}

impl fmt::Debug for OpenContext {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("OpenContext")
            .field("config", &self.config)
            .finish()
    }
}
