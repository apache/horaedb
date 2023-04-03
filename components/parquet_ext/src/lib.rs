// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

pub mod async_arrow_writer;
pub mod meta_data;
pub mod prune;
pub mod reverse_reader;
#[cfg(test)]
pub mod tests;

use std::sync::Arc;

pub use parquet::file::metadata::ParquetMetaData;
pub type ParquetMetaDataRef = Arc<ParquetMetaData>;
