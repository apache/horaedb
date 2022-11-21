// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

pub mod cache;
pub mod prune;
pub mod reverse_reader;
#[cfg(test)]
pub mod tests;

use std::sync::Arc;

use parquet::file::metadata::ParquetMetaData;

use crate::cache::DataCache;

pub type DataCacheRef = Arc<dyn DataCache + Send + Sync>;

pub type ParquetMetaDataRef = Arc<ParquetMetaData>;
