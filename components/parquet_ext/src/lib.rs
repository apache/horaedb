// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

pub mod cache;
pub mod prune;
pub mod reverse_reader;
mod serialized_reader;
#[cfg(test)]
pub mod tests;

use std::sync::Arc;

pub use serialized_reader::CacheableSerializedFileReader;

use crate::cache::{DataCache, MetaCache};

pub type MetaCacheRef = Arc<dyn MetaCache + Send + Sync>;
pub type DataCacheRef = Arc<dyn DataCache + Send + Sync>;
