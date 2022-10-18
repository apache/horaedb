// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

//! Sst reader implementation based on parquet.

use std::{
    ops::Range,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use async_trait::async_trait;
use bytes::Bytes;
use common_types::{
    projected_schema::{ProjectedSchema, RowProjector},
    record_batch::{ArrowRecordBatchProjector, RecordBatchWithKey},
};
use datafusion::{
    datasource::{file_format, object_store::ObjectStoreUrl},
    execution::context::TaskContext,
    physical_plan::{
        execute_stream,
        file_format::{
            FileMeta, FileScanConfig, ParquetExec, ParquetFileMetrics, ParquetFileReaderFactory,
        },
        metrics::ExecutionPlanMetricsSet,
        SendableRecordBatchStream, Statistics,
    },
    prelude::SessionContext,
};
use futures::{
    future::{self, BoxFuture},
    FutureExt, Stream, StreamExt, TryFutureExt,
};
use log::{debug, info};
use object_store::{ObjectMeta, ObjectStoreRef, Path};
use parquet::arrow::async_reader::AsyncFileReader;
use parquet_ext::{DataCacheRef, MetaCacheRef};
use snafu::{ensure, OptionExt, ResultExt};
use table_engine::predicate::PredicateRef;

use super::encoding::{self, ParquetDecoder};
use crate::{
    sst::{
        factory::SstReaderOptions,
        file::SstMetaData,
        reader::{error::*, Result, SstReader},
    },
    table_options::StorageFormatOptions,
};

pub struct ParquetSstReader<'a> {
    /// The path where the data is persisted.
    path: &'a Path,
    /// The storage where the data is persist.
    storage: &'a ObjectStoreRef,
    projected_schema: ProjectedSchema,
    reader_factory: Arc<dyn ParquetFileReaderFactory>,
    meta_cache: Option<MetaCacheRef>,
    predicate: PredicateRef,

    /// init those fields in `init_if_necessary`
    meta_data: Option<SstMetaData>,
    object_meta: Option<ObjectMeta>,
}

impl<'a> ParquetSstReader<'a> {
    pub fn new(path: &'a Path, storage: &'a ObjectStoreRef, options: &SstReaderOptions) -> Self {
        let reader_factory = Arc::new(CachableParquetFileReaderFactory {
            storage: storage.clone(),
            data_cache: options.data_cache.clone(),
        });
        Self {
            path,
            storage,
            reader_factory,
            projected_schema: options.projected_schema.clone(),
            meta_cache: options.meta_cache.clone(),
            predicate: options.predicate.clone(),

            meta_data: None,
            object_meta: None,
        }
    }

    async fn fetch_record_batch_stream(&mut self) -> Result<SendableRecordBatchStream> {
        assert!(self.meta_data.is_some());

        let meta_data = self.meta_data.as_ref().unwrap();
        let object_meta = self.object_meta.as_ref().unwrap();

        let schema = meta_data.schema.clone();
        let arrow_schema = schema.to_arrow_schema_ref();
        let row_projector = self
            .projected_schema
            .try_project_with_key(&meta_data.schema)
            .map_err(|e| Box::new(e) as _)
            .context(Projection)?;
        let scan_config = FileScanConfig {
            object_store_url: ObjectStoreUrl::parse("ceresdb://ceresdb/")
                .expect("valid object store URL"),
            file_schema: arrow_schema,
            file_groups: vec![vec![object_meta.clone().into()]],
            statistics: Statistics::default(),
            projection: Some(row_projector.existed_source_projection()),
            limit: None,
            table_partition_cols: vec![],
        };
        let filter_expr = self.predicate.filter_expr(schema.timestamp_name());
        debug!(
            "send record_batch, object_meta:{:?}, filter:{:?}, scan_config:{:?}",
            object_meta, filter_expr, scan_config
        );

        let exec = ParquetExec::new(scan_config, Some(filter_expr), Some(object_meta.size))
            .with_parquet_file_reader_factory(self.reader_factory.clone());

        // set up "fake" DataFusion session
        let session_ctx = SessionContext::new();
        let task_ctx = Arc::new(TaskContext::from(&session_ctx));
        task_ctx
            .runtime_env()
            .register_object_store("ceresdb", "ceresdb", self.storage.clone());

        execute_stream(Arc::new(exec), task_ctx)
            .await
            .context(DataFusionError {})
    }

    async fn init_if_necessary(&mut self) -> Result<()> {
        if self.meta_data.is_some() {
            return Ok(());
        }

        let object_meta = self
            .storage
            .head(self.path)
            .await
            .context(ObjectStoreError {})?;
        self.object_meta = Some(object_meta.clone());

        let sst_meta = Self::read_sst_meta(
            self.storage,
            self.path,
            Some(object_meta),
            &self.meta_cache,
            &self.reader_factory,
        )
        .await?;
        self.meta_data = Some(sst_meta);

        Ok(())
    }

    pub async fn read_sst_meta(
        storage: &ObjectStoreRef,
        path: &Path,
        object_meta: Option<ObjectMeta>,
        meta_cache: &Option<MetaCacheRef>,
        reader_factory: &Arc<dyn ParquetFileReaderFactory>,
    ) -> Result<SstMetaData> {
        let get_metadata_from_storage = || async {
            let object_meta = if let Some(v) = object_meta {
                v
            } else {
                storage.head(path).await.context(ObjectStoreError {})?
            };

            let mut reader = reader_factory
                // partition_index set to dummy 0
                .create_reader(0, object_meta.into(), None, &ExecutionPlanMetricsSet::new())
                .context(DataFusionError {})?;
            let metadata = reader.get_metadata().await.context(ParquetError {})?;

            debug!("read sst_meta, path:{}, meta:{:?}", &path, metadata);

            if let Some(cache) = meta_cache {
                cache.put(path.to_string(), metadata.clone());
            }
            Ok(metadata)
        };

        let metadata = if let Some(cache) = meta_cache {
            if let Some(cached_data) = cache.get(path.as_ref()) {
                cached_data
            } else {
                get_metadata_from_storage().await?
            }
        } else {
            get_metadata_from_storage().await?
        };

        let kv_metas = metadata
            .file_metadata()
            .key_value_metadata()
            .context(SstMetaNotFound)?;
        ensure!(!kv_metas.is_empty(), EmptySstMeta);

        encoding::decode_sst_meta_data(&kv_metas[0])
            .map_err(|e| Box::new(e) as _)
            .context(DecodeSstMeta)
    }

    #[cfg(test)]
    pub(crate) async fn row_groups(&mut self) -> Vec<parquet::file::metadata::RowGroupMetaData> {
        let object_meta = self.storage.head(self.path).await.unwrap();
        let mut reader = self
            .reader_factory
            .create_reader(0, object_meta.into(), None, &ExecutionPlanMetricsSet::new())
            .unwrap();

        let metadata = reader.get_metadata().await.unwrap();
        metadata.row_groups().to_vec()
    }
}

#[derive(Debug)]
pub struct CachableParquetFileReaderFactory {
    storage: ObjectStoreRef,
    data_cache: Option<DataCacheRef>,
}

impl CachableParquetFileReaderFactory {
    pub fn new(storage: ObjectStoreRef, data_cache: Option<DataCacheRef>) -> Self {
        Self {
            storage,
            data_cache,
        }
    }
}

impl ParquetFileReaderFactory for CachableParquetFileReaderFactory {
    fn create_reader(
        &self,
        partition_index: usize,
        file_meta: FileMeta,
        metadata_size_hint: Option<usize>,
        metrics: &ExecutionPlanMetricsSet,
    ) -> datafusion::error::Result<Box<dyn AsyncFileReader + Send>> {
        let parquet_file_metrics =
            ParquetFileMetrics::new(partition_index, file_meta.location().as_ref(), metrics);

        Ok(Box::new(CachableParquetFileReader {
            storage: self.storage.clone(),
            data_cache: self.data_cache.clone(),
            meta: file_meta.object_meta,
            metadata_size_hint,
            metrics: parquet_file_metrics,
        }))
    }
}

struct CachableParquetFileReader {
    storage: ObjectStoreRef,
    data_cache: Option<DataCacheRef>,
    meta: ObjectMeta,
    metrics: ParquetFileMetrics,
    metadata_size_hint: Option<usize>,
}

impl CachableParquetFileReader {
    fn cache_key(name: &str, start: usize, end: usize) -> String {
        format!("{}_{}_{}", name, start, end)
    }
}

impl AsyncFileReader for CachableParquetFileReader {
    fn get_bytes(&mut self, range: Range<usize>) -> BoxFuture<'_, parquet::errors::Result<Bytes>> {
        self.metrics.bytes_scanned.add(range.end - range.start);

        let key = Self::cache_key(self.meta.location.as_ref(), range.start, range.end);
        if let Some(cache) = &self.data_cache {
            if let Some(cached_bytes) = cache.get(&key) {
                return Box::pin(future::ok(Bytes::from(cached_bytes.to_vec())));
            };
        }

        self.storage
            .get_range(&self.meta.location, range)
            .map_ok(|bytes| {
                if let Some(cache) = &self.data_cache {
                    cache.put(key, Arc::new(bytes.to_vec()));
                }
                bytes
            })
            .map_err(|e| {
                parquet::errors::ParquetError::General(format!(
                    "CachableParquetFileReader::get_bytes error: {}",
                    e
                ))
            })
            .boxed()
    }

    fn get_metadata(
        &mut self,
    ) -> BoxFuture<'_, parquet::errors::Result<Arc<parquet::file::metadata::ParquetMetaData>>> {
        Box::pin(async move {
            let metadata = file_format::parquet::fetch_parquet_metadata(
                self.storage.as_ref(),
                &self.meta,
                self.metadata_size_hint,
            )
            .await
            .map_err(|e| {
                parquet::errors::ParquetError::General(format!(
                    "CachableParquetFileReader::get_metadata error: {}",
                    e
                ))
            })?;
            Ok(Arc::new(metadata))
        })
    }
}

struct RecordBatchProjector {
    stream: SendableRecordBatchStream,
    row_projector: RowProjector,
    storage_format_opts: StorageFormatOptions,
    row_num: usize,
}

impl Drop for RecordBatchProjector {
    fn drop(&mut self) {
        info!("RecordBatchProjector read {} rows", self.row_num);
    }
}

impl Stream for RecordBatchProjector {
    type Item = Result<RecordBatchWithKey>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let projector = self.get_mut();

        match projector.stream.poll_next_unpin(cx) {
            Poll::Ready(Some(record_batch)) => {
                match record_batch
                    .map_err(|e| Box::new(e) as _)
                    .context(DecodeRecordBatch {})
                {
                    Err(e) => Poll::Ready(Some(Err(e))),
                    Ok(record_batch) => {
                        let arrow_record_batch_projector =
                            ArrowRecordBatchProjector::from(projector.row_projector.clone());
                        let parquet_decoder =
                            ParquetDecoder::new(projector.storage_format_opts.clone());
                        let record_batch = parquet_decoder
                            .decode_record_batch(record_batch)
                            .map_err(|e| Box::new(e) as _)
                            .context(DecodeRecordBatch)?;

                        projector.row_num += record_batch.num_rows();

                        let projected_batch = arrow_record_batch_projector
                            .project_to_record_batch_with_key(record_batch)
                            .map_err(|e| Box::new(e) as _)
                            .context(DecodeRecordBatch {});

                        Poll::Ready(Some(projected_batch))
                    }
                }
            }
            // expected struct `RecordBatchWithKey`, found struct `arrow::record_batch::RecordBatch`
            // other => other
            Poll::Pending => Poll::Pending,
            Poll::Ready(None) => Poll::Ready(None),
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (0, None)
    }
}

#[async_trait]
impl<'a> SstReader for ParquetSstReader<'a> {
    async fn meta_data(&mut self) -> Result<&SstMetaData> {
        self.init_if_necessary().await?;

        Ok(self.meta_data.as_ref().unwrap())
    }

    async fn read(
        &mut self,
    ) -> Result<Box<dyn Stream<Item = Result<RecordBatchWithKey>> + Send + Unpin>> {
        self.init_if_necessary().await?;

        let stream = self.fetch_record_batch_stream().await?;
        let metadata = &self.meta_data.as_ref().unwrap();
        let row_projector = self
            .projected_schema
            .try_project_with_key(&metadata.schema)
            .map_err(|e| Box::new(e) as _)
            .context(Projection)?;
        let storage_format_opts = metadata.storage_format_opts.clone();

        Ok(Box::new(RecordBatchProjector {
            stream,
            row_projector,
            storage_format_opts,
            row_num: 0,
        }))
    }
}
