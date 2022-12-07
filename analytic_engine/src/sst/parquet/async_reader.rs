// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

//! Sst reader implementation based on parquet.

use std::{
    ops::Range,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
    time::Instant,
};

use arrow::{datatypes::SchemaRef, record_batch::RecordBatch as ArrowRecordBatch};
use async_trait::async_trait;
use bytes::Bytes;
use common_types::{
    projected_schema::{ProjectedSchema, RowProjector},
    record_batch::{ArrowRecordBatchProjector, RecordBatchWithKey},
};
use common_util::{runtime::Runtime, time::InstantExt};
use datafusion::datasource::file_format;
use futures::{future::BoxFuture, FutureExt, Stream, StreamExt, TryFutureExt};
use log::{debug, error, info};
use object_store::{ObjectMeta, ObjectStoreRef, Path};
use parquet::{
    arrow::{async_reader::AsyncFileReader, ParquetRecordBatchStreamBuilder, ProjectionMask},
    file::metadata::RowGroupMetaData,
};
use parquet_ext::ParquetMetaDataRef;
use prometheus::local::LocalHistogram;
use snafu::ResultExt;
use table_engine::predicate::PredicateRef;
use tokio::sync::mpsc::{self, Receiver, Sender};

use crate::{
    sst::{
        factory::SstReaderOptions,
        file::{BloomFilter, SstMetaData},
        meta_cache::{MetaCacheRef, MetaData},
        metrics,
        parquet::{encoding::ParquetDecoder, row_group_filter::RowGroupFilter},
        reader::{error::*, Result, SstReader},
    },
    table_options::StorageFormatOptions,
};

type SendableRecordBatchStream = Pin<Box<dyn Stream<Item = Result<ArrowRecordBatch>> + Send>>;

pub struct Reader<'a> {
    /// The path where the data is persisted.
    path: &'a Path,
    /// The storage where the data is persist.
    storage: &'a ObjectStoreRef,
    projected_schema: ProjectedSchema,
    meta_cache: Option<MetaCacheRef>,
    predicate: PredicateRef,
    batch_size: usize,

    /// init those fields in `init_if_necessary`
    meta_data: Option<MetaData>,
    row_projector: Option<RowProjector>,
}

impl<'a> Reader<'a> {
    pub fn new(path: &'a Path, storage: &'a ObjectStoreRef, options: &SstReaderOptions) -> Self {
        let batch_size = options.read_batch_row_num;
        Self {
            path,
            storage,
            projected_schema: options.projected_schema.clone(),
            meta_cache: options.meta_cache.clone(),
            predicate: options.predicate.clone(),
            batch_size,
            meta_data: None,
            row_projector: None,
        }
    }

    fn filter_row_groups(
        &self,
        schema: SchemaRef,
        row_groups: &[RowGroupMetaData],
        bloom_filter: &BloomFilter,
    ) -> Result<Vec<usize>> {
        let filter = RowGroupFilter::try_new(
            &schema,
            row_groups,
            bloom_filter.filters(),
            self.predicate.exprs(),
        )?;

        Ok(filter.filter())
    }

    async fn fetch_record_batch_stream(&mut self) -> Result<SendableRecordBatchStream> {
        assert!(self.meta_data.is_some());

        let meta_data = self.meta_data.as_ref().unwrap();
        let row_projector = self.row_projector.as_ref().unwrap();
        let object_store_reader =
            ObjectStoreReader::new(self.storage.clone(), self.path.clone(), meta_data.clone());
        let filtered_row_groups = self.filter_row_groups(
            meta_data.custom().schema.to_arrow_schema_ref(),
            meta_data.parquet().row_groups(),
            &meta_data.custom().bloom_filter,
        )?;

        debug!(
            "fetch_record_batch row_groups total:{}, after filter:{}",
            meta_data.parquet().num_row_groups(),
            filtered_row_groups.len()
        );

        let proj_mask = ProjectionMask::leaves(
            meta_data.parquet().file_metadata().schema_descr(),
            row_projector.existed_source_projection().iter().copied(),
        );

        let builder = ParquetRecordBatchStreamBuilder::new(object_store_reader)
            .await
            .with_context(|| ParquetError)?;
        let stream = builder
            .with_batch_size(self.batch_size)
            .with_row_groups(filtered_row_groups)
            .with_projection(proj_mask)
            .build()
            .with_context(|| ParquetError)?
            .map(|batch| batch.with_context(|| ParquetError));

        Ok(Box::pin(stream))
    }

    async fn init_if_necessary(&mut self) -> Result<()> {
        if self.meta_data.is_some() {
            return Ok(());
        }

        let meta_data = Self::read_sst_meta(self.storage, self.path, &self.meta_cache).await?;

        let row_projector = self
            .projected_schema
            .try_project_with_key(&meta_data.custom().schema)
            .map_err(|e| Box::new(e) as _)
            .context(Projection)?;
        self.meta_data = Some(meta_data);
        self.row_projector = Some(row_projector);
        Ok(())
    }

    async fn load_meta_data_from_storage(
        storage: &ObjectStoreRef,
        object_meta: &ObjectMeta,
    ) -> Result<ParquetMetaDataRef> {
        let meta_data =
            file_format::parquet::fetch_parquet_metadata(storage.as_ref(), object_meta, None)
                .await
                .map_err(|e| Box::new(e) as _)
                .context(DecodeSstMeta)?;
        Ok(Arc::new(meta_data))
    }

    async fn read_sst_meta(
        storage: &ObjectStoreRef,
        path: &Path,
        meta_cache: &Option<MetaCacheRef>,
    ) -> Result<MetaData> {
        if let Some(cache) = meta_cache {
            if let Some(meta_data) = cache.get(path.as_ref()) {
                return Ok(meta_data);
            }
        }
        // The metadata can't be found in the cache, and let's fetch it from the
        // storage.

        let meta_data = {
            let object_meta = storage.head(path).await.context(ObjectStoreError {})?;
            let parquet_meta_data =
                Self::load_meta_data_from_storage(storage, &object_meta).await?;
            MetaData::try_new(&parquet_meta_data, object_meta.size)
                .map_err(|e| Box::new(e) as _)
                .context(DecodeSstMeta)?
        };

        // Update the cache if necessary.
        if let Some(cache) = meta_cache {
            cache.put(path.to_string(), meta_data.clone());
        }

        Ok(meta_data)
    }

    #[cfg(test)]
    pub(crate) async fn row_groups(&mut self) -> Vec<parquet::file::metadata::RowGroupMetaData> {
        let meta_data = Self::read_sst_meta(self.storage, self.path, &self.meta_cache)
            .await
            .unwrap();
        meta_data.parquet().row_groups().to_vec()
    }
}

#[derive(Debug)]
struct ReaderMetrics {
    bytes_scanned: usize,
    sst_get_range_length_histogram: LocalHistogram,
}

struct ObjectStoreReader {
    storage: ObjectStoreRef,
    path: Path,
    meta_data: MetaData,
    metrics: ReaderMetrics,
}

impl ObjectStoreReader {
    fn new(storage: ObjectStoreRef, path: Path, meta_data: MetaData) -> Self {
        Self {
            storage,
            path,
            meta_data,
            metrics: ReaderMetrics {
                bytes_scanned: 0,
                sst_get_range_length_histogram: metrics::SST_GET_RANGE_HISTOGRAM.local(),
            },
        }
    }
}

impl Drop for ObjectStoreReader {
    fn drop(&mut self) {
        info!("ObjectStoreReader dropped, metrics:{:?}", self.metrics);
    }
}

impl AsyncFileReader for ObjectStoreReader {
    fn get_bytes(&mut self, range: Range<usize>) -> BoxFuture<'_, parquet::errors::Result<Bytes>> {
        self.metrics.bytes_scanned += range.end - range.start;
        self.metrics
            .sst_get_range_length_histogram
            .observe((range.end - range.start) as f64);
        self.storage
            .get_range(&self.path, range)
            .map_err(|e| {
                parquet::errors::ParquetError::General(format!(
                    "Failed to fetch range from object store, err:{}",
                    e
                ))
            })
            .boxed()
    }

    fn get_byte_ranges(
        &mut self,
        ranges: Vec<Range<usize>>,
    ) -> BoxFuture<'_, parquet::errors::Result<Vec<Bytes>>> {
        for range in &ranges {
            self.metrics
                .sst_get_range_length_histogram
                .observe((range.end - range.start) as f64);
        }
        async move {
            self.storage
                .get_ranges(&self.path, &ranges)
                .map_err(|e| {
                    parquet::errors::ParquetError::General(format!(
                        "Failed to fetch ranges from object store, err:{}",
                        e
                    ))
                })
                .await
        }
        .boxed()
    }

    fn get_metadata(
        &mut self,
    ) -> BoxFuture<'_, parquet::errors::Result<Arc<parquet::file::metadata::ParquetMetaData>>> {
        Box::pin(async move { Ok(self.meta_data.parquet().clone()) })
    }
}

struct RecordBatchProjector {
    path: String,
    stream: SendableRecordBatchStream,
    row_projector: ArrowRecordBatchProjector,
    storage_format_opts: StorageFormatOptions,

    row_num: usize,
    start_time: Instant,
}

impl RecordBatchProjector {
    fn new(
        path: String,
        stream: SendableRecordBatchStream,
        row_projector: ArrowRecordBatchProjector,
        storage_format_opts: StorageFormatOptions,
    ) -> Self {
        Self {
            path,
            stream,
            row_projector,
            storage_format_opts,
            row_num: 0,
            start_time: Instant::now(),
        }
    }
}

impl Drop for RecordBatchProjector {
    fn drop(&mut self) {
        info!(
            "RecordBatchProjector {}, read {} rows, cost:{}ms",
            self.path,
            self.row_num,
            self.start_time.saturating_elapsed().as_millis(),
        );
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
                        let parquet_decoder =
                            ParquetDecoder::new(projector.storage_format_opts.clone());
                        let record_batch = parquet_decoder
                            .decode_record_batch(record_batch)
                            .map_err(|e| Box::new(e) as _)
                            .context(DecodeRecordBatch)?;

                        projector.row_num += record_batch.num_rows();

                        let projected_batch = projector
                            .row_projector
                            .project_to_record_batch_with_key(record_batch)
                            .map_err(|e| Box::new(e) as _)
                            .context(DecodeRecordBatch {});

                        Poll::Ready(Some(projected_batch))
                    }
                }
            }
            Poll::Pending => Poll::Pending,
            Poll::Ready(None) => Poll::Ready(None),
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.stream.size_hint()
    }
}

#[async_trait]
impl<'a> SstReader for Reader<'a> {
    async fn meta_data(&mut self) -> Result<&SstMetaData> {
        self.init_if_necessary().await?;

        Ok(self.meta_data.as_ref().unwrap().custom().as_ref())
    }

    async fn read(
        &mut self,
    ) -> Result<Box<dyn Stream<Item = Result<RecordBatchWithKey>> + Send + Unpin>> {
        self.init_if_necessary().await?;

        let stream = self.fetch_record_batch_stream().await?;
        let row_projector = self.row_projector.take().unwrap();
        let row_projector = ArrowRecordBatchProjector::from(row_projector);

        let storage_format_opts = self
            .meta_data
            .as_ref()
            // metadata must be inited after `init_if_necessary`.
            .unwrap()
            .custom()
            .storage_format_opts
            .clone();

        Ok(Box::new(RecordBatchProjector::new(
            self.path.to_string(),
            stream,
            row_projector,
            storage_format_opts,
        )))
    }
}

struct RecordBatchReceiver {
    rx: Receiver<Result<RecordBatchWithKey>>,
}

impl Stream for RecordBatchReceiver {
    type Item = Result<RecordBatchWithKey>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.as_mut().rx.poll_recv(cx)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (0, None)
    }
}

const DEFAULT_CHANNEL_CAP: usize = 1024;

/// Spawn a new thread to read record_batches
pub struct ThreadedReader<'a> {
    inner: Reader<'a>,
    runtime: Arc<Runtime>,

    channel_cap: usize,
}

impl<'a> ThreadedReader<'a> {
    pub fn new(reader: Reader<'a>, runtime: Arc<Runtime>) -> Self {
        Self {
            inner: reader,
            runtime,
            channel_cap: DEFAULT_CHANNEL_CAP,
        }
    }

    async fn read_record_batches(&mut self, tx: Sender<Result<RecordBatchWithKey>>) -> Result<()> {
        let mut stream = self.inner.read().await?;
        self.runtime.spawn(async move {
            while let Some(batch) = stream.next().await {
                if let Err(e) = tx.send(batch).await {
                    error!("fail to send the fetched record batch result, err:{}", e);
                }
            }
        });

        Ok(())
    }
}

#[async_trait]
impl<'a> SstReader for ThreadedReader<'a> {
    async fn meta_data(&mut self) -> Result<&SstMetaData> {
        self.inner.meta_data().await
    }

    async fn read(
        &mut self,
    ) -> Result<Box<dyn Stream<Item = Result<RecordBatchWithKey>> + Send + Unpin>> {
        let (tx, rx) = mpsc::channel::<Result<RecordBatchWithKey>>(self.channel_cap);
        self.read_record_batches(tx).await?;

        Ok(Box::new(RecordBatchReceiver { rx }))
    }
}
