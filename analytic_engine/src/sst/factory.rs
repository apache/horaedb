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

/// Pick suitable object store for different scenes.
pub trait ObjectStorePicker: Send + Sync + Debug {
    /// Just provide default object store for the scenes where user don't care
    /// about it.
    fn default_store(&self) -> &ObjectStoreRef;

    /// Pick an object store according to the read frequency.
    fn pick_by_freq(&self, freq: ReadFrequency) -> &ObjectStoreRef;
}

pub type ObjectStorePickerRef = Arc<dyn ObjectStorePicker>;

/// For any [`ObjectStoreRef`], it can be used as an [`ObjectStorePicker`].
impl ObjectStorePicker for ObjectStoreRef {
    fn default_store(&self) -> &ObjectStoreRef {
        self
    }

    fn pick_by_freq(&self, _freq: ReadFrequency) -> &ObjectStoreRef {
        self
    }
}

pub trait Factory: Send + Sync + Debug {
    fn new_sst_reader<'a>(
        &self,
        options: &SstReaderOptions,
        path: &'a Path,
        store_picker: &'a ObjectStorePickerRef,
    ) -> Option<Box<dyn SstReader + Send + 'a>>;

    fn new_sst_builder<'a>(
        &self,
        options: &SstBuilderOptions,
        path: &'a Path,
        store_picker: &'a ObjectStorePickerRef,
    ) -> Option<Box<dyn SstBuilder + Send + 'a>>;
}

#[derive(Debug, Copy, Clone)]
pub enum SstType {
    Auto,
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
        store_picker: &'a ObjectStorePickerRef,
    ) -> Option<Box<dyn SstReader + Send + 'a>> {
        // TODO: Currently, we only have one sst format, and we have to choose right
        // reader for sst according to its real format in the future.
        let reader = AsyncParquetReader::new(path, store_picker, options);
        let reader = ThreadedReader::new(
            reader,
            options.runtime.clone(),
            options.background_read_parallelism,
        );
        Some(Box::new(reader))
    }

    fn new_sst_builder<'a>(
        &self,
        options: &SstBuilderOptions,
        path: &'a Path,
        store_picker: &'a ObjectStorePickerRef,
    ) -> Option<Box<dyn SstBuilder + Send + 'a>> {
        match options.sst_type {
            SstType::Parquet | SstType::Auto => Some(Box::new(ParquetSstBuilder::new(
                path,
                store_picker,
                options,
            ))),
        }
    }
}

/// Sst factory reference
pub type FactoryRef = Arc<dyn Factory>;
