// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

//! Factory for different kinds sst builder and reader.

use std::{fmt::Debug, sync::Arc};

use common_types::projected_schema::ProjectedSchema;
use common_util::runtime::Runtime;
use object_store::{ObjectStoreRef, Path};
use table_engine::predicate::PredicateRef;

use crate::{
    sst::{
        builder::SstBuilder,
        meta_cache::MetaCacheRef,
        parquet::{builder::ParquetSstBuilder, AsyncParquetReader, ThreadedReader},
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

/// The frequency of query execution may decide some behavior in the sst reader,
/// e.g. cache policy.
#[derive(Debug, Copy, Clone)]
pub enum ReadFrequency {
    Once,
    Frequent,
}

#[derive(Debug, Clone)]
pub struct SstReaderOptions {
    pub sst_type: SstType,
    pub read_batch_row_num: usize,
    pub reverse: bool,
    pub frequency: ReadFrequency,
    pub projected_schema: ProjectedSchema,
    pub predicate: PredicateRef,
    pub meta_cache: Option<MetaCacheRef>,
    pub runtime: Arc<Runtime>,

    /// The max number of rows in one row group
    pub num_rows_per_row_group: usize,

    /// The suggested parallelism while reading sst
    pub background_read_parallelism: usize,
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
                let reader = AsyncParquetReader::new(path, storage, options);
                let reader = ThreadedReader::new(
                    reader,
                    options.runtime.clone(),
                    options.background_read_parallelism,
                );
                Some(Box::new(reader))
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
