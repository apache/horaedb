// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Factory for different kinds sst writer and reader.

use std::{collections::HashMap, fmt::Debug, sync::Arc};

use async_trait::async_trait;
use common_types::projected_schema::RowProjectorBuilder;
use generic_error::{BoxError, GenericError};
use macros::define_result;
use object_store::{ObjectStoreRef, Path};
use runtime::Runtime;
use snafu::{Backtrace, OptionExt, ResultExt, Snafu};
use table_engine::predicate::PredicateRef;
use trace_metric::MetricsCollector;

use super::parquet::encoding::ColumnEncoding;
use crate::{
    sst::{
        file::Level,
        header,
        header::HeaderParser,
        meta_data::cache::MetaCacheRef,
        metrics::MaybeTableLevelMetrics as SstMaybeTableLevelMetrics,
        parquet::{
            writer::{ParquetSstWriter, WriteOptions},
            AsyncParquetReader, ThreadedReader,
        },
        reader::SstReader,
        writer::SstWriter,
    },
    table_options::{Compression, StorageFormat, StorageFormatHint},
};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Failed to parse sst header, err:{}", source,))]
    ParseHeader { source: header::Error },

    #[snafu(display("Empty storage format hint.\nBacktrace:\n{}", backtrace))]
    EmptyStorageFormatHint { backtrace: Backtrace },

    #[snafu(display("Failed to convert storage format hint, err:{}", source))]
    ConvertStorageFormatHint { source: GenericError },

    #[snafu(display("Failed to convert compression, err:{}", source))]
    ConvertCompression { source: GenericError },
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
        metrics_collector: Option<MetricsCollector>,
    ) -> Result<Box<dyn SstReader + Send + 'a>>;

    async fn create_writer<'a>(
        &self,
        options: &SstWriteOptions,
        path: &'a Path,
        store_picker: &'a ObjectStorePickerRef,
        level: Level,
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
pub struct ScanOptions {
    /// The suggested parallelism while reading sst
    pub background_read_parallelism: usize,
    /// The max record batches in flight
    pub max_record_batches_in_flight: usize,
    /// The number of streams to prefetch when scan
    pub num_streams_to_prefetch: usize,
}

impl Default for ScanOptions {
    fn default() -> Self {
        Self {
            background_read_parallelism: 1,
            max_record_batches_in_flight: 64,
            num_streams_to_prefetch: 2,
        }
    }
}

#[derive(Debug, Clone)]
pub struct SstReadOptions {
    pub maybe_table_level_metrics: Option<Arc<SstMaybeTableLevelMetrics>>,

    pub frequency: ReadFrequency,
    pub num_rows_per_row_group: usize,
    pub row_projector_builder: RowProjectorBuilder,
    pub predicate: PredicateRef,
    pub meta_cache: Option<MetaCacheRef>,
    pub scan_options: ScanOptions,

    pub runtime: Arc<Runtime>,
}
#[derive(Clone, Debug)]
pub struct ColumnStats {
    pub low_cardinality: bool,
}

#[derive(Debug, Clone)]
pub struct SstWriteOptions {
    pub storage_format_hint: StorageFormatHint,
    pub num_rows_per_row_group: usize,
    pub compression: Compression,
    pub max_buffer_size: usize,
    pub column_stats: HashMap<String, ColumnStats>,
}

impl TryFrom<horaedbproto::compaction_service::SstWriteOptions> for SstWriteOptions {
    type Error = Error;

    fn try_from(value: horaedbproto::compaction_service::SstWriteOptions) -> Result<Self> {
        let storage_format_hint: StorageFormatHint = value
            .storage_format_hint
            .context(EmptyStorageFormatHint)?
            .try_into()
            .box_err()
            .context(ConvertStorageFormatHint)?;

        let num_rows_per_row_group = value.num_rows_per_row_group as usize;
        let compression: Compression = value
            .compression
            .try_into()
            .box_err()
            .context(ConvertCompression)?;
        let max_buffer_size = value.max_buffer_size as usize;

        let column_stats: HashMap<String, ColumnStats> = value
            .column_stats
            .into_iter()
            .map(|(k, v)| (k, ColumnStats { low_cardinality: v }))
            .collect();

        Ok(SstWriteOptions {
            storage_format_hint,
            num_rows_per_row_group,
            compression,
            max_buffer_size,
            column_stats,
        })
    }
}

impl From<SstWriteOptions> for horaedbproto::compaction_service::SstWriteOptions {
    fn from(value: SstWriteOptions) -> Self {
        let column_stats = value
            .column_stats
            .into_iter()
            .map(|(k, v)| (k, v.low_cardinality))
            .collect();

        Self {
            storage_format_hint: Some(value.storage_format_hint.into()),
            num_rows_per_row_group: value.num_rows_per_row_group as u64,
            compression: value.compression.into(),
            max_buffer_size: value.max_buffer_size as u64,
            column_stats,
        }
    }
}

impl From<&ColumnStats> for ColumnEncoding {
    fn from(value: &ColumnStats) -> Self {
        ColumnEncoding {
            enable_dict: value.low_cardinality,
        }
    }
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
        metrics_collector: Option<MetricsCollector>,
    ) -> Result<Box<dyn SstReader + Send + 'a>> {
        let storage_format = match hint.file_format {
            Some(v) => v,
            None => {
                let header_parser = HeaderParser::new(path, store_picker.default_store());
                header_parser.parse().await.context(ParseHeader)?
            }
        };

        match storage_format {
            StorageFormat::Columnar => {
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
                    options.scan_options.background_read_parallelism,
                    options.scan_options.max_record_batches_in_flight,
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
        level: Level,
    ) -> Result<Box<dyn SstWriter + Send + 'a>> {
        let column_encodings =
            HashMap::from_iter(options.column_stats.iter().map(|(col_name, col_stats)| {
                (col_name.to_owned(), ColumnEncoding::from(col_stats))
            }));
        let write_options = WriteOptions {
            num_rows_per_row_group: options.num_rows_per_row_group,
            max_buffer_size: options.max_buffer_size,
            compression: options.compression.into(),
            sst_level: level,
            column_encodings,
        };
        Ok(Box::new(ParquetSstWriter::new(
            path,
            write_options,
            store_picker,
        )))
    }
}
