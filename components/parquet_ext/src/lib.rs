// Copyright 2022-2023 CeresDB Project Authors. Licensed under Apache-2.0.

pub mod meta_data;
pub mod prune;
pub mod reader;
#[cfg(test)]
pub mod tests;

use std::sync::Arc;

pub use parquet::file::metadata::ParquetMetaData;
pub type ParquetMetaDataRef = Arc<ParquetMetaData>;
