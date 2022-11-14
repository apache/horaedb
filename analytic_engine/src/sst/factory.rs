// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

//! Factory for different kinds sst builder and reader.

use std::{fmt::Debug, sync::Arc};

use common_types::projected_schema::ProjectedSchema;
use common_util::runtime::Runtime;
use object_store::{ObjectStoreRef, Path};
use parquet_ext::DataCacheRef;
use table_engine::predicate::PredicateRef;

use crate::{
    sst::{
        builder::SstBuilder,
        meta_cache::MetaCacheRef,
        parquet::{
            builder::ParquetSstBuilder, reader::ParquetSstReader, AsyncParquetReader,
            ThreadedReader,
        },
        reader::SstReader,
    },
    table_options::Compression,
};

pub trait Factory: Send + Sync + Debug {
    fn new_sst_reader<'a>(
        &self,
        options: &SstReaderOptions,
        path: &'a Path,
        storage: &'a ObjectStoreRef,
    ) -> Option<Box<dyn SstReader + Send + 'a>>;

    fn new_sst_builder<'a>(
        &self,
        options: &SstBuilderOptions,
        path: &'a Path,
        storage: &'a ObjectStoreRef,
    ) -> Option<Box<dyn SstBuilder + Send + 'a>>;
}

#[derive(Debug, Copy, Clone)]
pub enum SstType {
    Parquet,
}

#[derive(Debug, Clone)]
pub struct SstReaderOptions {
    pub sst_type: SstType,
    pub read_batch_row_num: usize,
    pub reverse: bool,
    pub projected_schema: ProjectedSchema,
    pub predicate: PredicateRef,
    pub meta_cache: Option<MetaCacheRef>,
    pub data_cache: Option<DataCacheRef>,
    pub runtime: Arc<Runtime>,
}

#[derive(Debug, Clone)]
pub struct SstBuilderOptions {
    pub sst_type: SstType,
    pub num_rows_per_row_group: usize,
    pub compression: Compression,
}

#[derive(Debug, Default)]
pub struct FactoryImpl;

impl Factory for FactoryImpl {
    fn new_sst_reader<'a>(
        &self,
        options: &SstReaderOptions,
        path: &'a Path,
        storage: &'a ObjectStoreRef,
    ) -> Option<Box<dyn SstReader + Send + 'a>> {
        match options.sst_type {
            SstType::Parquet => {
                // FIXME: pass from reader options
                if std::env::var("ASYNC_PARQUET_READER").unwrap_or_default() == "true" {
                    let reader = AsyncParquetReader::new(path, storage, options);
                    let reader = ThreadedReader::new(reader, options.runtime.clone());
                    Some(Box::new(reader))
                } else {
                    Some(Box::new(ParquetSstReader::new(path, storage, options)))
                }
            }
        }
    }

    fn new_sst_builder<'a>(
        &self,
        options: &SstBuilderOptions,
        path: &'a Path,
        storage: &'a ObjectStoreRef,
    ) -> Option<Box<dyn SstBuilder + Send + 'a>> {
        match options.sst_type {
            SstType::Parquet => Some(Box::new(ParquetSstBuilder::new(path, storage, options))),
        }
    }
}

/// Sst factory reference
pub type FactoryRef = Arc<dyn Factory>;
