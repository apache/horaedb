// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

//! Factory for different kinds sst writer and reader.

use std::{fmt::Debug, sync::Arc};

use async_trait::async_trait;
use common_types::projected_schema::ProjectedSchema;
use common_util::{define_result, runtime::Runtime};
use object_store::{ObjectStoreRef, Path};
use snafu::{ResultExt, Snafu};
use table_engine::predicate::PredicateRef;
use trace_metric::Collector;

use crate::{
    sst::{
        header,
        header::HeaderParser,
        meta_data::cache::MetaCacheRef,
        parquet::{writer::ParquetSstWriter, AsyncParquetReader, ThreadedReader},
        reader::SstReader,
        writer::SstWriter,
    },
    table_options::{Compression, StorageFormat, StorageFormatHint},
};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Failed to parse sst header, err:{}", source,))]
    ParseHeader { source: header::Error },
}

define_result!(Error);

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

/// Sst factory reference
pub type FactoryRef = Arc<dyn Factory>;

#[async_trait]
pub trait Factory: Send + Sync + Debug {
    async fn create_reader<'a>(
        &self,
        path: &'a Path,
        options: &SstReadOptions,
        hint: SstReadHint,
        store_picker: &'a ObjectStorePickerRef,
        metrics_collector: Option<Collector>,
    ) -> Result<Box<dyn SstReader + Send + 'a>>;

    async fn create_writer<'a>(
        &self,
        options: &SstWriteOptions,
        path: &'a Path,
        store_picker: &'a ObjectStorePickerRef,
    ) -> Result<Box<dyn SstWriter + Send + 'a>>;
}

/// The frequency of query execution may decide some behavior in the sst reader,
/// e.g. cache policy.
#[derive(Debug, Copy, Clone)]
pub enum ReadFrequency {
    Once,
    Frequent,
}

#[derive(Clone, Copy, Debug, Default)]
pub struct SstReadHint {
    /// Hint for the size of the sst file. It may avoid some io if provided.
    pub file_size: Option<usize>,
    /// Hint for the storage format of the sst file. It may avoid some io if
    /// provided.
    pub file_format: Option<StorageFormat>,
}

#[derive(Debug, Clone)]
pub struct SstReadOptions {
    pub read_batch_row_num: usize,
    pub reverse: bool,
    pub frequency: ReadFrequency,
    pub projected_schema: ProjectedSchema,
    pub predicate: PredicateRef,
    pub meta_cache: Option<MetaCacheRef>,

    /// The max number of rows in one row group
    pub num_rows_per_row_group: usize,
    /// The suggested parallelism while reading sst
    pub background_read_parallelism: usize,

    pub runtime: Arc<Runtime>,
}

#[derive(Debug, Clone)]
pub struct SstWriteOptions {
    pub storage_format_hint: StorageFormatHint,
    pub num_rows_per_row_group: usize,
    pub compression: Compression,
}

#[derive(Debug, Default)]
pub struct FactoryImpl;

#[async_trait]
impl Factory for FactoryImpl {
    async fn create_reader<'a>(
        &self,
        path: &'a Path,
        options: &SstReadOptions,
        hint: SstReadHint,
        store_picker: &'a ObjectStorePickerRef,
        metrics_collector: Option<Collector>,
    ) -> Result<Box<dyn SstReader + Send + 'a>> {
        let storage_format = match hint.file_format {
            Some(v) => v,
            None => {
                let header_parser = HeaderParser::new(path, store_picker.default_store());
                header_parser.parse().await.context(ParseHeader)?
            }
        };

        match storage_format {
            StorageFormat::Columnar | StorageFormat::Hybrid => {
                let reader = AsyncParquetReader::new(
                    path,
                    options,
                    hint.file_size,
                    store_picker,
                    metrics_collector,
                );
                let reader = ThreadedReader::new(
                    reader,
                    options.runtime.clone(),
                    options.background_read_parallelism,
                );
                Ok(Box::new(reader))
            }
        }
    }

    async fn create_writer<'a>(
        &self,
        options: &SstWriteOptions,
        path: &'a Path,
        store_picker: &'a ObjectStorePickerRef,
    ) -> Result<Box<dyn SstWriter + Send + 'a>> {
        let hybrid_encoding = match options.storage_format_hint {
            StorageFormatHint::Specific(format) => matches!(format, StorageFormat::Hybrid),
            // `Auto` is mapped to columnar parquet format now, may change in future.
            StorageFormatHint::Auto => false,
        };

        Ok(Box::new(ParquetSstWriter::new(
            path,
            hybrid_encoding,
            store_picker,
            options,
        )))
    }
}
