// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

//! Sst reader implementation based on parquet.

use std::{
    ops::Range,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
    time::{Duration, Instant},
};

use arrow::{datatypes::SchemaRef, record_batch::RecordBatch as ArrowRecordBatch};
use async_trait::async_trait;
use bytes::Bytes;
use common_types::{
    projected_schema::{ProjectedSchema, RowProjector},
    record_batch::{ArrowRecordBatchProjector, RecordBatchWithKey},
};
use common_util::{
    error::{BoxError, GenericResult},
    runtime::{AbortOnDropMany, JoinHandle, Runtime},
    time::InstantExt,
};
use datafusion::{
    common::ToDFSchema,
    physical_expr::{create_physical_expr, execution_props::ExecutionProps},
    physical_plan::{
        file_format::{parquet::page_filter::PagePruningPredicate, ParquetFileMetrics},
        metrics::ExecutionPlanMetricsSet,
    },
};
use futures::{future::BoxFuture, FutureExt, Stream, StreamExt, TryFutureExt};
use log::{debug, error};
use object_store::{ObjectStoreRef, Path};
use parquet::{
    arrow::{
        arrow_reader::{ArrowReaderOptions, RowSelection},
        async_reader::AsyncFileReader,
        ParquetRecordBatchStreamBuilder, ProjectionMask,
    },
    file::metadata::RowGroupMetaData,
};
use parquet_ext::meta_data::ChunkReader;
use snafu::ResultExt;
use table_engine::predicate::PredicateRef;
use tokio::sync::mpsc::{self, Receiver, Sender};
use trace_metric::{MetricsCollector, TraceMetricWhenDrop};

use crate::sst::{
    factory::{ObjectStorePickerRef, ReadFrequency, SstReadOptions},
    meta_data::{
        cache::{MetaCacheRef, MetaData},
        SstMetaData,
    },
    parquet::{
        encoding::ParquetDecoder,
        meta_data::{ParquetFilter, ParquetMetaDataRef},
        row_group_pruner::RowGroupPruner,
    },
    reader::{error::*, Result, SstReader},
};

const PRUNE_ROW_GROUPS_METRICS_COLLECTOR_NAME: &str = "prune_row_groups";
type SendableRecordBatchStream = Pin<Box<dyn Stream<Item = Result<ArrowRecordBatch>> + Send>>;

pub struct Reader<'a> {
    /// The path where the data is persisted.
    path: &'a Path,
    /// The storage where the data is persist.
    store: &'a ObjectStoreRef,
    /// The hint for the sst file size.
    file_size_hint: Option<usize>,
    num_rows_per_row_group: usize,
    projected_schema: ProjectedSchema,
    meta_cache: Option<MetaCacheRef>,
    predicate: PredicateRef,
    /// Current frequency decides the cache policy.
    frequency: ReadFrequency,
    /// Init those fields in `init_if_necessary`
    meta_data: Option<MetaData>,
    row_projector: Option<RowProjector>,

    /// Options for `read_parallelly`
    metrics: Metrics,
    df_plan_metrics: ExecutionPlanMetricsSet,
}

#[derive(Default, Debug, Clone, TraceMetricWhenDrop)]
pub(crate) struct Metrics {
    #[metric(boolean)]
    pub meta_data_cache_hit: bool,
    #[metric(duration)]
    pub read_meta_data_duration: Duration,
    #[metric(number)]
    pub parallelism: usize,
    #[metric(collector)]
    pub metrics_collector: Option<MetricsCollector>,
}

impl<'a> Reader<'a> {
    pub fn new(
        path: &'a Path,
        options: &SstReadOptions,
        file_size_hint: Option<usize>,
        store_picker: &'a ObjectStorePickerRef,
        metrics_collector: Option<MetricsCollector>,
    ) -> Self {
        let store = store_picker.pick_by_freq(options.frequency);
        let df_plan_metrics = ExecutionPlanMetricsSet::new();
        let metrics = Metrics {
            metrics_collector,
            ..Default::default()
        };

        Self {
            path,
            store,
            file_size_hint,
            num_rows_per_row_group: options.num_rows_per_row_group,
            projected_schema: options.projected_schema.clone(),
            meta_cache: options.meta_cache.clone(),
            predicate: options.predicate.clone(),
            frequency: options.frequency,
            meta_data: None,
            row_projector: None,
            metrics,
            df_plan_metrics,
        }
    }

    async fn maybe_read_parallelly(
        &mut self,
        read_parallelism: usize,
    ) -> Result<Vec<Box<dyn Stream<Item = Result<RecordBatchWithKey>> + Send + Unpin>>> {
        assert!(read_parallelism > 0);

        self.init_if_necessary().await?;
        let streams = self.fetch_record_batch_streams(read_parallelism).await?;
        if streams.is_empty() {
            return Ok(Vec::new());
        }

        let row_projector = {
            let row_projector = self.row_projector.take().unwrap();
            ArrowRecordBatchProjector::from(row_projector)
        };

        let sst_meta_data = self
            .meta_data
            .as_ref()
            // metadata must be inited after `init_if_necessary`.
            .unwrap()
            .custom();

        let streams: Vec<_> = streams
            .into_iter()
            .map(|stream| {
                Box::new(RecordBatchProjector::new(
                    self.path.to_string(),
                    stream,
                    row_projector.clone(),
                    sst_meta_data.clone(),
                )) as _
            })
            .collect();

        Ok(streams)
    }

    fn prune_row_groups(
        &self,
        schema: SchemaRef,
        row_groups: &[RowGroupMetaData],
        parquet_filter: Option<&ParquetFilter>,
    ) -> Result<Vec<usize>> {
        let metrics_collector = self
            .metrics
            .metrics_collector
            .as_ref()
            .map(|v| v.span(PRUNE_ROW_GROUPS_METRICS_COLLECTOR_NAME.to_string()));
        let mut pruner = RowGroupPruner::try_new(
            &schema,
            row_groups,
            parquet_filter,
            self.predicate.exprs(),
            metrics_collector,
        )?;

        Ok(pruner.prune())
    }

    /// The final parallelism is ensured in the range: [1, num_row_groups].
    #[inline]
    fn decide_read_parallelism(suggested: usize, num_row_groups: usize) -> usize {
        suggested.min(num_row_groups).max(1)
    }

    fn build_row_selection(
        &self,
        arrow_schema: SchemaRef,
        row_groups: &[usize],
        file_metadata: &parquet_ext::ParquetMetaData,
    ) -> Result<Option<RowSelection>> {
        // TODO: remove fixed partition
        let partition = 0;
        let exprs = datafusion::optimizer::utils::conjunction(self.predicate.exprs().to_vec());
        let exprs = match exprs {
            Some(exprs) => exprs,
            None => return Ok(None),
        };

        let df_schema = arrow_schema
            .clone()
            .to_dfschema()
            .context(DataFusionError)?;
        let physical_expr =
            create_physical_expr(&exprs, &df_schema, &arrow_schema, &ExecutionProps::new())
                .context(DataFusionError)?;
        let page_predicate = PagePruningPredicate::try_new(&physical_expr, arrow_schema.clone())
            .context(DataFusionError)?;

        let metrics = ParquetFileMetrics::new(partition, self.path.as_ref(), &self.df_plan_metrics);
        page_predicate
            .prune(row_groups, file_metadata, &metrics)
            .context(DataFusionError)
    }

    async fn fetch_record_batch_streams(
        &mut self,
        suggested_parallelism: usize,
    ) -> Result<Vec<SendableRecordBatchStream>> {
        assert!(self.meta_data.is_some());

        let meta_data = self.meta_data.as_ref().unwrap();
        let row_projector = self.row_projector.as_ref().unwrap();
        let arrow_schema = meta_data.custom().schema.to_arrow_schema_ref();
        // Get target row groups.
        let target_row_groups = self.prune_row_groups(
            arrow_schema.clone(),
            meta_data.parquet().row_groups(),
            meta_data.custom().parquet_filter.as_ref(),
        )?;

        debug!(
            "Reader fetch record batches, path:{}, row_groups total:{}, after prune:{}",
            self.path,
            meta_data.parquet().num_row_groups(),
            target_row_groups.len(),
        );

        if target_row_groups.is_empty() {
            return Ok(Vec::new());
        }

        // Partition the batches by `read_parallelism`.
        let parallelism =
            Self::decide_read_parallelism(suggested_parallelism, target_row_groups.len());

        // TODO: we only support read parallelly when `batch_size` ==
        // `num_rows_per_row_group`, so this placing method is ok, we should
        // adjust it when supporting it other situations.
        let chunks_num = parallelism;
        let chunk_size = target_row_groups.len() / parallelism;
        self.metrics.parallelism = parallelism;

        let mut target_row_group_chunks = vec![Vec::with_capacity(chunk_size); chunks_num];
        for (row_group_idx, row_group) in target_row_groups.into_iter().enumerate() {
            let chunk_idx = row_group_idx % chunks_num;
            target_row_group_chunks[chunk_idx].push(row_group);
        }

        let parquet_metadata = meta_data.parquet();
        let proj_mask = ProjectionMask::leaves(
            meta_data.parquet().file_metadata().schema_descr(),
            row_projector.existed_source_projection().iter().copied(),
        );
        debug!(
            "Reader fetch record batches, parallelism suggest:{}, real:{}, chunk_size:{}, project:{:?}",
            suggested_parallelism, parallelism, chunk_size, proj_mask
        );

        let mut streams = Vec::with_capacity(target_row_group_chunks.len());
        for chunk in target_row_group_chunks {
            let object_store_reader =
                ObjectStoreReader::new(self.store.clone(), self.path.clone(), meta_data.clone());
            let mut builder = ParquetRecordBatchStreamBuilder::new(
                object_store_reader
            )
            .await
            .with_context(|| ParquetError)?;

            let row_selection =
                self.build_row_selection(arrow_schema.clone(), &chunk, parquet_metadata)?;
            if let Some(selection) = row_selection {
                builder = builder.with_row_selection(selection);
            };

            let stream = builder
                .with_batch_size(self.num_rows_per_row_group)
                .with_row_groups(chunk)
                .with_projection(proj_mask.clone())
                .build()
                .with_context(|| ParquetError)?
                .map(|batch| batch.with_context(|| ParquetError));

            streams.push(Box::pin(stream) as _);
        }

        Ok(streams)
    }

    async fn init_if_necessary(&mut self) -> Result<()> {
        if self.meta_data.is_some() {
            return Ok(());
        }

        let meta_data = {
            let start = Instant::now();
            let meta_data = self.read_sst_meta().await?;
            self.metrics.read_meta_data_duration = start.elapsed();
            meta_data
        };

        let row_projector = self
            .projected_schema
            .try_project_with_key(&meta_data.custom().schema)
            .box_err()
            .context(Projection)?;
        self.meta_data = Some(meta_data);
        self.row_projector = Some(row_projector);
        Ok(())
    }

    async fn load_file_size(&self) -> Result<usize> {
        let file_size = match self.file_size_hint {
            Some(v) => v,
            None => {
                let object_meta = self.store.head(self.path).await.context(ObjectStoreError)?;
                object_meta.size
            }
        };

        Ok(file_size)
    }

    async fn load_meta_data_from_storage(&self) -> Result<parquet_ext::ParquetMetaDataRef> {
        let file_size = self.load_file_size().await?;
        let chunk_reader_adapter = ChunkReaderAdapter::new(self.path, self.store);

        let (meta_data, _) =
            parquet_ext::meta_data::fetch_parquet_metadata(file_size, &chunk_reader_adapter)
                .await
                .with_context(|| FetchAndDecodeSstMeta {
                    file_path: self.path.to_string(),
                })?;
        

        let object_store_reader =
        ObjectStoreReader::new(self.store.clone(), self.path.clone(), MetaData::try_new(&meta_data, true).unwrap());
        let  read_options = ArrowReaderOptions::new().with_page_index(true);
        let  builder = ParquetRecordBatchStreamBuilder::new_with_options(
                object_store_reader,
                read_options,
            )
            .await
            .with_context(|| ParquetError)?;

        Ok(builder.metadata().clone())
    }

    fn need_update_cache(&self) -> bool {
        match self.frequency {
            ReadFrequency::Once => false,
            ReadFrequency::Frequent => true,
        }
    }

    async fn read_sst_meta(&mut self) -> Result<MetaData> {
        if let Some(cache) = &self.meta_cache {
            if let Some(meta_data) = cache.get(self.path.as_ref()) {
                self.metrics.meta_data_cache_hit = true;
                return Ok(meta_data);
            }
        }

        // The metadata can't be found in the cache, and let's fetch it from the
        // storage.
        let avoid_update_cache = !self.need_update_cache();
        let empty_predicate = self.predicate.exprs().is_empty();

        let meta_data = {
            let parquet_meta_data = self.load_meta_data_from_storage().await?;

            let ignore_sst_filter = avoid_update_cache && empty_predicate;
            MetaData::try_new(&parquet_meta_data, ignore_sst_filter)
                .box_err()
                .context(DecodeSstMeta)?
        };

        if avoid_update_cache || self.meta_cache.is_none() {
            return Ok(meta_data);
        }

        // Update the cache.
        self.meta_cache
            .as_ref()
            .unwrap()
            .put(self.path.to_string(), meta_data.clone());

        Ok(meta_data)
    }

    #[cfg(test)]
    pub(crate) async fn row_groups(&mut self) -> Vec<parquet::file::metadata::RowGroupMetaData> {
        let meta_data = self.read_sst_meta().await.unwrap();
        meta_data.parquet().row_groups().to_vec()
    }
}

impl<'a> Drop for Reader<'a> {
    fn drop(&mut self) {
        debug!(
            "Parquet reader dropped, path:{:?}, df_plan_metrics:{}",
            self.path,
            self.df_plan_metrics.clone_inner().to_string()
        );
    }
}

#[derive(Clone)]
pub struct ObjectStoreReader {
    storage: ObjectStoreRef,
    path: Path,
    meta_data: MetaData,
    begin: Instant,
}

impl ObjectStoreReader {
    pub fn new(storage: ObjectStoreRef, path: Path, meta_data: MetaData) -> Self {
        Self {
            storage,
            path,
            meta_data,
            begin: Instant::now(),
        }
    }
}

impl Drop for ObjectStoreReader {
    fn drop(&mut self) {
        debug!(
            "ObjectStoreReader dropped, path:{}, elapsed:{:?}",
            &self.path,
            self.begin.elapsed()
        );
    }
}

impl AsyncFileReader for ObjectStoreReader {
    fn get_bytes(&mut self, range: Range<usize>) -> BoxFuture<'_, parquet::errors::Result<Bytes>> {
        self.storage
            .get_range(&self.path, range)
            .map_err(|e| {
                parquet::errors::ParquetError::General(format!(
                    "Failed to fetch range from object store, err:{e}"
                ))
            })
            .boxed()
    }

    fn get_byte_ranges(
        &mut self,
        ranges: Vec<Range<usize>>,
    ) -> BoxFuture<'_, parquet::errors::Result<Vec<Bytes>>> {
        async move {
            self.storage
                .get_ranges(&self.path, &ranges)
                .map_err(|e| {
                    parquet::errors::ParquetError::General(format!(
                        "Failed to fetch ranges from object store, err:{e}"
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

pub struct ChunkReaderAdapter<'a> {
    path: &'a Path,
    store: &'a ObjectStoreRef,
}

impl<'a> ChunkReaderAdapter<'a> {
    pub fn new(path: &'a Path, store: &'a ObjectStoreRef) -> Self {
        Self { path, store }
    }
}

#[async_trait]
impl<'a> ChunkReader for ChunkReaderAdapter<'a> {
    async fn get_bytes(&self, range: Range<usize>) -> GenericResult<Bytes> {
        self.store.get_range(self.path, range).await.box_err()
    }
}

struct RecordBatchProjector {
    path: String,
    stream: SendableRecordBatchStream,
    row_projector: ArrowRecordBatchProjector,

    row_num: usize,
    start_time: Instant,
    sst_meta: ParquetMetaDataRef,
}

impl RecordBatchProjector {
    fn new(
        path: String,
        stream: SendableRecordBatchStream,
        row_projector: ArrowRecordBatchProjector,
        sst_meta: ParquetMetaDataRef,
    ) -> Self {
        Self {
            path,
            stream,
            row_projector,
            row_num: 0,
            start_time: Instant::now(),
            sst_meta,
        }
    }
}

impl Drop for RecordBatchProjector {
    fn drop(&mut self) {
        debug!(
            "RecordBatchProjector dropped, path:{} rows:{}, cost:{}ms.",
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
                match record_batch.box_err().context(DecodeRecordBatch {}) {
                    Err(e) => Poll::Ready(Some(Err(e))),
                    Ok(record_batch) => {
                        let parquet_decoder =
                            ParquetDecoder::new(&projector.sst_meta.collapsible_cols_idx);
                        let record_batch = parquet_decoder
                            .decode_record_batch(record_batch)
                            .box_err()
                            .context(DecodeRecordBatch)?;

                        projector.row_num += record_batch.num_rows();

                        let projected_batch = projector
                            .row_projector
                            .project_to_record_batch_with_key(record_batch)
                            .box_err()
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
    async fn meta_data(&mut self) -> Result<SstMetaData> {
        self.init_if_necessary().await?;

        Ok(SstMetaData::Parquet(
            self.meta_data.as_ref().unwrap().custom().clone(),
        ))
    }

    async fn read(
        &mut self,
    ) -> Result<Box<dyn Stream<Item = Result<RecordBatchWithKey>> + Send + Unpin>> {
        let mut streams = self.maybe_read_parallelly(1).await?;
        assert_eq!(streams.len(), 1);
        let stream = streams.pop().expect("impossible to fetch no stream");

        Ok(stream)
    }
}

struct RecordBatchReceiver {
    rx_group: Vec<Receiver<Result<RecordBatchWithKey>>>,
    cur_rx_idx: usize,
    #[allow(dead_code)]
    drop_helper: AbortOnDropMany<()>,
}

impl Stream for RecordBatchReceiver {
    type Item = Result<RecordBatchWithKey>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.rx_group.is_empty() {
            return Poll::Ready(None);
        }

        let cur_rx_idx = self.cur_rx_idx;
        // `cur_rx_idx` is impossible to be out-of-range, because it is got by round
        // robin.
        let rx_group_len = self.rx_group.len();
        let cur_rx = self.rx_group.get_mut(cur_rx_idx).unwrap_or_else(|| {
            panic!(
                "cur_rx_idx is impossible to be out-of-range, cur_rx_idx:{cur_rx_idx}, rx_group len:{rx_group_len}"
            )
        });
        let poll_result = cur_rx.poll_recv(cx);

        match poll_result {
            Poll::Ready(result) => {
                // If found `Poll::Pending`, we need to keep polling current rx
                // until found `Poll::Ready` for ensuring the order of record batches,
                // because batches are placed into each stream by round robin:
                // +------+    +------+    +------+
                // |  1   |    |  2   |    |  3   |
                // +------+    +------+    +------+
                // |  4   |    |  5   |    |  6   |
                // +------+    +------+    +------+
                // | ...  |    | ...  |    | ...  |
                // +------+    +------+    +------+
                self.cur_rx_idx = (self.cur_rx_idx + 1) % self.rx_group.len();
                Poll::Ready(result)
            }
            Poll::Pending => Poll::Pending,
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (0, None)
    }
}

/// Spawn a new thread to read record_batches
pub struct ThreadedReader<'a> {
    inner: Reader<'a>,
    runtime: Arc<Runtime>,

    channel_cap: usize,
    read_parallelism: usize,
}

impl<'a> ThreadedReader<'a> {
    pub fn new(
        reader: Reader<'a>,
        runtime: Arc<Runtime>,
        read_parallelism: usize,
        channel_cap: usize,
    ) -> Self {
        assert!(
            read_parallelism > 0,
            "read parallelism must be greater than 0"
        );

        Self {
            inner: reader,
            runtime,
            channel_cap,
            read_parallelism,
        }
    }

    fn read_record_batches_from_sub_reader(
        &mut self,
        mut reader: Box<dyn Stream<Item = Result<RecordBatchWithKey>> + Send + Unpin>,
        tx: Sender<Result<RecordBatchWithKey>>,
    ) -> JoinHandle<()> {
        self.runtime.spawn(async move {
            while let Some(batch) = reader.next().await {
                if let Err(e) = tx.send(batch).await {
                    error!("fail to send the fetched record batch result, err:{}", e);
                }
            }
        })
    }
}

#[async_trait]
impl<'a> SstReader for ThreadedReader<'a> {
    async fn meta_data(&mut self) -> Result<SstMetaData> {
        self.inner.meta_data().await
    }

    async fn read(
        &mut self,
    ) -> Result<Box<dyn Stream<Item = Result<RecordBatchWithKey>> + Send + Unpin>> {
        // Get underlying sst readers and channels.
        let sub_readers = self
            .inner
            .maybe_read_parallelly(self.read_parallelism)
            .await?;
        if sub_readers.is_empty() {
            return Ok(Box::new(RecordBatchReceiver {
                rx_group: Vec::new(),
                cur_rx_idx: 0,
                drop_helper: AbortOnDropMany(Vec::new()),
            }) as _);
        }

        let read_parallelism = sub_readers.len();
        debug!(
            "ThreadedReader read, suggest read_parallelism:{}, actual:{}",
            self.read_parallelism, read_parallelism
        );

        let channel_cap_per_sub_reader = self.channel_cap / sub_readers.len();
        let (tx_group, rx_group): (Vec<_>, Vec<_>) = (0..read_parallelism)
            .map(|_| mpsc::channel::<Result<RecordBatchWithKey>>(channel_cap_per_sub_reader))
            .unzip();

        // Start the background readings.
        let mut handles = Vec::with_capacity(sub_readers.len());
        for (sub_reader, tx) in sub_readers.into_iter().zip(tx_group.into_iter()) {
            handles.push(self.read_record_batches_from_sub_reader(sub_reader, tx));
        }

        Ok(Box::new(RecordBatchReceiver {
            rx_group,
            cur_rx_idx: 0,
            drop_helper: AbortOnDropMany(handles),
        }) as _)
    }
}

#[cfg(test)]
mod tests {
    use std::{
        pin::Pin,
        task::{Context, Poll},
        time::Duration,
    };

    use futures::{Stream, StreamExt};
    use tokio::sync::mpsc::{self, Receiver, Sender};

    struct MockReceivers {
        rx_group: Vec<Receiver<u32>>,
        cur_rx_idx: usize,
    }

    impl Stream for MockReceivers {
        type Item = u32;

        fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
            let cur_rx_idx = self.cur_rx_idx;
            // `cur_rx_idx` is impossible to be out-of-range, because it is got by round
            // robin.
            let cur_rx = self.rx_group.get_mut(cur_rx_idx).unwrap();
            let poll_result = cur_rx.poll_recv(cx);

            match poll_result {
                Poll::Ready(result) => {
                    self.cur_rx_idx = (self.cur_rx_idx + 1) % self.rx_group.len();
                    Poll::Ready(result)
                }
                Poll::Pending => Poll::Pending,
            }
        }

        fn size_hint(&self) -> (usize, Option<usize>) {
            (0, None)
        }
    }

    struct MockRandomSenders {
        tx_group: Vec<Sender<u32>>,
        test_datas: Vec<Vec<u32>>,
    }

    impl MockRandomSenders {
        fn start_to_send(&mut self) {
            while !self.tx_group.is_empty() {
                let tx = self.tx_group.pop().unwrap();
                let test_data = self.test_datas.pop().unwrap();
                tokio::spawn(async move {
                    for datum in test_data {
                        let random_millis = rand::random::<u64>() % 30;
                        tokio::time::sleep(Duration::from_millis(random_millis)).await;
                        tx.send(datum).await.unwrap();
                    }
                });
            }
        }
    }

    fn gen_test_data(amount: usize) -> Vec<u32> {
        (0..amount).map(|_| rand::random::<u32>()).collect()
    }

    // We mock a thread model same as the one in `ThreadedReader` to check its
    // validity.
    // TODO: we should make the `ThreadedReader` mockable and refactor this test
    // using it.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_simulated_threaded_reader() {
        let test_data = gen_test_data(123);
        let expected = test_data.clone();
        let channel_cap_per_sub_reader = 10;
        let reader_num = 5;
        let (tx_group, rx_group): (Vec<_>, Vec<_>) = (0..reader_num)
            .map(|_| mpsc::channel::<u32>(channel_cap_per_sub_reader))
            .unzip();

        // Partition datas.
        let chunk_len = reader_num;
        let mut test_data_chunks = vec![Vec::new(); chunk_len];
        for (idx, datum) in test_data.into_iter().enumerate() {
            let chunk_idx = idx % chunk_len;
            test_data_chunks.get_mut(chunk_idx).unwrap().push(datum);
        }

        // Start senders.
        let mut mock_senders = MockRandomSenders {
            tx_group,
            test_datas: test_data_chunks,
        };
        mock_senders.start_to_send();

        // Poll receivers.
        let mut actual = Vec::new();
        let mut mock_receivers = MockReceivers {
            rx_group,
            cur_rx_idx: 0,
        };
        while let Some(datum) = mock_receivers.next().await {
            actual.push(datum);
        }

        assert_eq!(actual, expected);
    }
}
