// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

pub mod cache;
#[allow(deprecated)]
pub mod reverse_reader;
#[cfg(test)]
pub mod tests;

// use cache::Cache;
use std::sync::Arc;

use crate::cache::{DataCache, MetaCache};

pub type MetaCacheRef = Arc<dyn MetaCache + Send + Sync>;
pub type DataCacheRef = Arc<dyn DataCache + Send + Sync>;
