// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

//! Factory for different kinds sst builder and reader.

use std::{
    fmt::Debug,
    ops::Range,
    sync::{Arc, RwLock},
};

use async_trait::async_trait;
use bytes::Bytes;
use common_types::projected_schema::ProjectedSchema;
use common_util::{
    error::{GenericError, GenericResult},
    runtime::Runtime,
};
use object_store::{ObjectStoreRef, Path};
use table_engine::predicate::PredicateRef;

use crate::{
    sst::{
        builder::SstBuilder,
        meta_data::cache::MetaCacheRef,
        parquet::{
            async_reader::AsyncFileChunkReader, builder::ParquetSstBuilder, AsyncParquetReader,
            ThreadedReader,
        },
        reader::SstReader,
    },
    table_options::{Compression, StorageFormat, StorageFormatHint},
};

pub struct FileReaderOnObjectStore {
    path: Path,
    store: ObjectStoreRef,
    cached_file_size: RwLock<Option<usize>>,
}

impl FileReaderOnObjectStore {
    pub fn new(path: Path, store: ObjectStoreRef) -> Self {
        Self {
            path,
            store,
            cached_file_size: RwLock::new(None),
        }
    }
}

#[async_trait]
impl AsyncFileChunkReader for FileReaderOnObjectStore {
    async fn file_size(&self) -> GenericResult<usize> {
        // check cached filed_size first
        {
            let file_size = self.cached_file_size.read().unwrap();
            if let Some(s) = file_size.as_ref() {
                return Ok(*s);
            }
        }

        // fetch the size from the underlying store
        let head = self
            .store
            .head(&self.path)
            .await
            .map_err(|e| Box::new(e) as GenericError)?;
        *self.cached_file_size.write().unwrap() = Some(head.size);
        Ok(head.size)
    }

    async fn get_byte_range(&self, range: Range<usize>) -> GenericResult<Bytes> {
        self.store
            .get_range(&self.path, range)
            .await
            .map_err(|e| Box::new(e) as _)
    }

    async fn get_byte_ranges(&self, ranges: &[Range<usize>]) -> GenericResult<Vec<Bytes>> {
        self.store
            .get_ranges(&self.path, ranges)
            .await
            .map_err(|e| Box::new(e) as _)
    }
}

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
        storage_format: StorageFormat,
        store_picker: &'a ObjectStorePickerRef,
    ) -> Option<Box<dyn SstReader + Send + 'a>>;

    fn new_sst_builder<'a>(
        &self,
        options: &SstBuilderOptions,
        path: &'a Path,
        store_picker: &'a ObjectStorePickerRef,
    ) -> Option<Box<dyn SstBuilder + Send + 'a>>;
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
    pub storage_format_hint: StorageFormatHint,
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
        storage_format: StorageFormat,
        store_picker: &'a ObjectStorePickerRef,
    ) -> Option<Box<dyn SstReader + Send + 'a>> {
        // TODO: Currently, we only have one sst format, and we have to choose right
        // reader for sst according to its real format in the future.
        let hybrid_encoding = matches!(storage_format, StorageFormat::Hybrid);
        let store = store_picker.pick_by_freq(options.frequency).clone();
        let file_reader = FileReaderOnObjectStore::new(path.clone(), store);
        let parquet_reader =
            AsyncParquetReader::new(path, hybrid_encoding, Arc::new(file_reader), options);
        let reader = ThreadedReader::new(
            parquet_reader,
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
        let hybrid_encoding = match options.storage_format_hint {
            StorageFormatHint::Specific(format) => matches!(format, StorageFormat::Hybrid),
            // `Auto` is mapped to columnar parquet format now, may change in future.
            StorageFormatHint::Auto => false,
        };

        Some(Box::new(ParquetSstBuilder::new(
            path,
            hybrid_encoding,
            store_picker,
            options,
        )))
    }
}

/// Sst factory reference
pub type FactoryRef = Arc<dyn Factory>;
