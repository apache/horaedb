// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

use std::{
    cmp,
    cmp::Ordering,
    collections::BinaryHeap,
    mem,
    ops::{Deref, DerefMut},
    time::{Duration, Instant},
};

use async_trait::async_trait;
use common_types::{
    projected_schema::ProjectedSchema,
    record_batch::{RecordBatchWithKey, RecordBatchWithKeyBuilder},
    request_id::RequestId,
    row::RowViewOnBatch,
    schema::RecordSchemaWithKey,
    SequenceNumber,
};
use common_util::{define_result, error::GenericError};
use futures::{future::try_join_all, StreamExt};
use log::{debug, trace};
use snafu::{ensure, Backtrace, ResultExt, Snafu};
use table_engine::{predicate::PredicateRef, table::TableId};
use trace_metric::{MetricsCollector, TraceMetricWhenDrop};

use crate::{
    row_iter::{
        record_batch_stream,
        record_batch_stream::{SequencedRecordBatch, SequencedRecordBatchStream},
        IterOptions, RecordBatchWithKeyIterator,
    },
    space::SpaceId,
    sst::{
        factory::{FactoryRef as SstFactoryRef, ObjectStorePickerRef, SstReadOptions},
        file::FileHandle,
        manager::MAX_LEVEL,
    },
    table::version::{MemTableVec, SamplingMemTable},
};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display(
        "Expect the same schema, expect:{:?}, given:{:?}.\nBacktrace:\n{}",
        expect,
        given,
        backtrace
    ))]
    MismatchedSchema {
        expect: RecordSchemaWithKey,
        given: RecordSchemaWithKey,
        backtrace: Backtrace,
    },

    #[snafu(display("Failed to pull record batch, error:{}", source))]
    PullRecordBatch { source: GenericError },

    #[snafu(display("Failed to build record batch, error:{}", source))]
    BuildRecordBatch {
        source: common_types::record_batch::Error,
    },

    #[snafu(display("Failed to append row, err:{:?}", source))]
    AppendRow {
        source: common_types::record_batch::Error,
    },

    #[snafu(display("Failed to build stream from memtable, err:{}", source))]
    BuildStreamFromMemtable {
        source: crate::row_iter::record_batch_stream::Error,
    },

    #[snafu(display("Failed to build record batch from sst, err:{}", source))]
    BuildStreamFromSst {
        source: crate::row_iter::record_batch_stream::Error,
    },
}

define_result!(Error);

/// Required parameters to construct the [MergeBuilder]
#[derive(Debug)]
pub struct MergeConfig<'a> {
    pub request_id: RequestId,
    pub metrics_collector: Option<MetricsCollector>,
    /// None for background jobs, such as: compaction
    pub deadline: Option<Instant>,
    pub space_id: SpaceId,
    pub table_id: TableId,
    /// Max visible sequence (inclusive)
    pub sequence: SequenceNumber,
    /// The projected schema to read.
    pub projected_schema: ProjectedSchema,
    /// The predicate of the query.
    pub predicate: PredicateRef,

    pub sst_read_options: SstReadOptions,
    /// Sst factory
    pub sst_factory: &'a SstFactoryRef,
    /// Store picker for persisting sst.
    pub store_picker: &'a ObjectStorePickerRef,

    pub merge_iter_options: IterOptions,

    pub need_dedup: bool,
    pub reverse: bool,
}

/// Builder for building merge stream from memtables and sst files.
#[must_use]
pub struct MergeBuilder<'a> {
    config: MergeConfig<'a>,

    /// Sampling memtable to read.
    sampling_mem: Option<SamplingMemTable>,
    /// MemTables to read.
    memtables: MemTableVec,
    /// Ssts to read of each level.
    ssts: Vec<Vec<FileHandle>>,
}

impl<'a> MergeBuilder<'a> {
    pub fn new(config: MergeConfig<'a>) -> Self {
        Self {
            config,
            sampling_mem: None,
            memtables: Vec::new(),
            ssts: vec![Vec::new(); MAX_LEVEL],
        }
    }

    pub fn sampling_mem(mut self, sampling_mem: Option<SamplingMemTable>) -> Self {
        self.sampling_mem = sampling_mem;
        self
    }

    pub fn memtables(mut self, memtables: MemTableVec) -> Self {
        self.memtables = memtables;
        self
    }

    pub fn ssts_of_level(mut self, ssts: Vec<Vec<FileHandle>>) -> Self {
        self.ssts = ssts;
        self
    }

    pub fn mut_memtables(&mut self) -> &mut MemTableVec {
        &mut self.memtables
    }

    /// Returns file handles in `level`, panic if level >= MAX_LEVEL
    pub fn mut_ssts_of_level(&mut self, level: u16) -> &mut Vec<FileHandle> {
        &mut self.ssts[usize::from(level)]
    }

    pub async fn build(self) -> Result<MergeIterator> {
        let sst_streams_num: usize = self
            .ssts
            .iter()
            .map(|leveled_ssts| leveled_ssts.len())
            .sum();
        let mut streams_num = sst_streams_num + self.memtables.len();
        if self.sampling_mem.is_some() {
            streams_num += 1;
        }
        let mut streams = Vec::with_capacity(streams_num);

        debug!(
            "Build merge iterator, table_id:{:?}, request_id:{}, sampling_mem:{:?}, memtables:{:?}, ssts:{:?}",
            self.config.table_id,
            self.config.request_id,
            self.sampling_mem,
            self.memtables,
            self.ssts
        );

        if let Some(v) = &self.sampling_mem {
            let stream = record_batch_stream::filtered_stream_from_memtable(
                self.config.projected_schema.clone(),
                self.config.need_dedup,
                &v.mem,
                self.config.reverse,
                self.config.predicate.as_ref(),
                self.config.deadline,
                self.config.metrics_collector.clone(),
            )
            .context(BuildStreamFromMemtable)?;
            streams.push(stream);
        }

        for memtable in &self.memtables {
            let stream = record_batch_stream::filtered_stream_from_memtable(
                self.config.projected_schema.clone(),
                self.config.need_dedup,
                &memtable.mem,
                self.config.reverse,
                self.config.predicate.as_ref(),
                self.config.deadline,
                self.config.metrics_collector.clone(),
            )
            .context(BuildStreamFromMemtable)?;
            streams.push(stream);
        }

        let mut sst_ids = Vec::with_capacity(self.ssts.len());
        for leveled_ssts in &self.ssts {
            for f in leveled_ssts {
                let stream = record_batch_stream::filtered_stream_from_sst_file(
                    self.config.space_id,
                    self.config.table_id,
                    f,
                    self.config.sst_factory,
                    &self.config.sst_read_options,
                    self.config.store_picker,
                    self.config.metrics_collector.clone(),
                )
                .await
                .context(BuildStreamFromSst)?;
                streams.push(stream);
                sst_ids.push(f.id());
            }
        }

        Ok(MergeIterator::new(
            self.config.table_id,
            self.config.request_id,
            // Use the schema after projection as the schema of the merge iterator.
            self.config.projected_schema.to_record_schema_with_key(),
            streams,
            self.ssts,
            self.config.merge_iter_options,
            self.config.reverse,
            Metrics::new(
                self.memtables.len(),
                sst_streams_num,
                self.config.metrics_collector,
            ),
        ))
    }
}

struct BufferedStreamState {
    /// Buffered record batch.
    ///
    /// invariant: `buffered_record_batch` is not empty.
    buffered_record_batch: SequencedRecordBatch,
    /// Cursor for reading buffered record batch.
    ///
    /// `cursor` increases monotonically from 0 to
    /// `buffered_record_batch.num_rows()` and `cursor ==
    /// buffered_record_batch.num_rows()` means no more buffered rows to read.
    cursor: usize,
}

impl BufferedStreamState {
    #[inline]
    fn is_valid(&self) -> bool {
        self.cursor < self.buffered_record_batch.num_rows()
    }

    #[inline]
    fn is_empty(&self) -> bool {
        self.cursor >= self.buffered_record_batch.num_rows()
    }

    #[inline]
    fn sequence(&self) -> SequenceNumber {
        self.buffered_record_batch.sequence
    }

    #[inline]
    fn first_row(&self) -> RowViewOnBatch<'_> {
        assert!(self.is_valid());

        RowViewOnBatch {
            record_batch: &self.buffered_record_batch.record_batch,
            row_idx: self.cursor,
        }
    }

    #[inline]
    fn last_row(&self) -> RowViewOnBatch<'_> {
        assert!(self.is_valid());

        RowViewOnBatch {
            record_batch: &self.buffered_record_batch.record_batch,
            row_idx: self.buffered_record_batch.num_rows() - 1,
        }
    }

    /// Returns the next available row in the buffer and advance the cursor by
    /// one step.
    fn next_row(&mut self) -> Option<RowViewOnBatch<'_>> {
        if self.cursor < self.buffered_record_batch.num_rows() {
            let row_view = RowViewOnBatch {
                record_batch: &self.buffered_record_batch.record_batch,
                row_idx: self.cursor,
            };
            self.cursor += 1;
            Some(row_view)
        } else {
            None
        }
    }

    /// Append `len` rows from cursor to the `builder` and advance the cursor.
    ///
    /// Returns number of rows added.
    fn append_rows_to(
        &mut self,
        builder: &mut RecordBatchWithKeyBuilder,
        len: usize,
    ) -> Result<usize> {
        let added = builder
            .append_batch_range(&self.buffered_record_batch.record_batch, self.cursor, len)
            .context(AppendRow)?;
        self.cursor += added;
        Ok(added)
    }

    /// Take record batch slice with at most `len` rows from cursor and advance
    /// the cursor.
    fn take_record_batch_slice(&mut self, len: usize) -> RecordBatchWithKey {
        let len_to_fetch = cmp::min(
            self.buffered_record_batch.record_batch.num_rows() - self.cursor,
            len,
        );
        let record_batch = self
            .buffered_record_batch
            .record_batch
            .slice(self.cursor, len_to_fetch);
        self.cursor += record_batch.num_rows();
        record_batch
    }

    #[inline]
    fn reset(&mut self, record_batch: SequencedRecordBatch) {
        self.buffered_record_batch = record_batch;
        self.cursor = 0;
    }
}

struct BufferedStream {
    schema: RecordSchemaWithKey,
    stream: SequencedRecordBatchStream,
    /// `None` state means the stream is exhausted.
    state: Option<BufferedStreamState>,
}

impl BufferedStream {
    async fn build(
        schema: RecordSchemaWithKey,
        mut stream: SequencedRecordBatchStream,
    ) -> Result<Self> {
        let buffered_record_batch = Self::pull_next_non_empty_batch(&mut stream).await?;
        let state = buffered_record_batch.map(|v| BufferedStreamState {
            buffered_record_batch: v,
            cursor: 0,
        });

        Ok(Self {
            schema,
            stream,
            state,
        })
    }

    fn sequence_in_buffer(&self) -> SequenceNumber {
        self.state.as_ref().unwrap().sequence()
    }

    /// REQUIRE: the buffer is not exhausted.
    fn first_row_in_buffer(&self) -> RowViewOnBatch<'_> {
        self.state.as_ref().unwrap().first_row()
    }

    /// REQUIRE: the buffer is not exhausted.
    fn last_row_in_buffer(&self) -> RowViewOnBatch<'_> {
        self.state.as_ref().unwrap().last_row()
    }

    /// REQUIRE: the buffer is not exhausted.
    fn next_row_in_buffer(&mut self) -> Option<RowViewOnBatch<'_>> {
        self.state.as_mut().unwrap().next_row()
    }

    /// REQUIRE: the buffer is not exhausted.
    fn append_rows_to(
        &mut self,
        builder: &mut RecordBatchWithKeyBuilder,
        len: usize,
    ) -> Result<usize> {
        self.state.as_mut().unwrap().append_rows_to(builder, len)
    }

    /// REQUIRE: the buffer is not exhausted.
    fn take_record_batch_slice(&mut self, len: usize) -> RecordBatchWithKey {
        self.state.as_mut().unwrap().take_record_batch_slice(len)
    }

    /// Pull the next non empty record batch.
    ///
    /// The returned record batch is ensured `num_rows() > 0`.
    async fn pull_next_non_empty_batch(
        stream: &mut SequencedRecordBatchStream,
    ) -> Result<Option<SequencedRecordBatch>> {
        loop {
            match stream.next().await.transpose().context(PullRecordBatch)? {
                Some(record_batch) => {
                    trace!(
                        "MergeIterator one record batch is fetched:{:?}",
                        record_batch
                    );

                    if record_batch.num_rows() > 0 {
                        return Ok(Some(record_batch));
                    }
                }
                None => return Ok(None),
            }
        }
    }

    /// Pull the next batch if the stream is not exhausted and the inner state
    /// is empty.
    async fn pull_next_batch_if_necessary(&mut self, metrics: &mut Metrics) -> Result<bool> {
        let need_pull_new_batch = !self.is_exhausted() && self.state.as_ref().unwrap().is_empty();
        if !need_pull_new_batch {
            return Ok(false);
        }

        // TODO(xikai): do the metrics collection in the `pull_next_non_empty_batch`.
        let pull_start = Instant::now();
        let pulled = match Self::pull_next_non_empty_batch(&mut self.stream).await? {
            None => {
                self.state = None;
                Ok(false)
            }
            Some(record_batch) => {
                self.state.as_mut().unwrap().reset(record_batch);
                Ok(true)
            }
        };

        metrics.scan_duration += pull_start.elapsed();
        metrics.scan_count += 1;

        pulled
    }

    #[inline]
    fn is_exhausted(&self) -> bool {
        self.state.is_none()
    }

    fn into_heaped(self, reverse: bool) -> HeapBufferedStream {
        HeapBufferedStream {
            stream: self,
            reverse,
        }
    }

    #[inline]
    fn schema(&self) -> &RecordSchemaWithKey {
        &self.schema
    }
}

/// The wrapper struct determines the compare result for the min binary heap.
struct HeapBufferedStream {
    stream: BufferedStream,
    reverse: bool,
}

impl HeapBufferedStream {
    /// Check whether all the buffered rows in the `stream` is after the
    /// `boundary_row`.
    ///
    /// NOTE:
    ///  - The first row in the stream is actually the max row if in reverse
    ///    order and should check whether it is smaller than `boundary_row`.
    ///  - The first row in the stream is actually the min row if in normal
    ///    order and should check whether it is greater than `boundary_row`.
    fn is_after_boundary(
        &self,
        schema: &RecordSchemaWithKey,
        boundary_row: &RowViewOnBatch,
    ) -> bool {
        if self.reverse {
            // Compare the max row(the first row) in of the stream with the boundary row.
            // The stream is after the boundary if the max row is smaller than boundary.
            // is_after: (boundary_row) > [first_row in buffer]
            matches!(
                schema.compare_row(boundary_row, &self.first_row_in_buffer()),
                Ordering::Greater
            )
        } else {
            // compare the min row(the first row) in of the stream with the boundary row.
            // The stream is after the boundary if the min row is greater than boundary.
            // is_after: (boundary_row) < [first_row in buffer]
            matches!(
                schema.compare_row(&self.first_row_in_buffer(), boundary_row),
                Ordering::Greater
            )
        }
    }
}

impl Deref for HeapBufferedStream {
    type Target = BufferedStream;

    fn deref(&self) -> &BufferedStream {
        &self.stream
    }
}

impl DerefMut for HeapBufferedStream {
    fn deref_mut(&mut self) -> &mut BufferedStream {
        &mut self.stream
    }
}

impl PartialEq for HeapBufferedStream {
    fn eq(&self, other: &Self) -> bool {
        let ordering = self
            .schema
            .compare_row(&self.first_row_in_buffer(), &other.first_row_in_buffer());
        if let Ordering::Equal = ordering {
            self.sequence_in_buffer() == other.sequence_in_buffer()
        } else {
            false
        }
    }
}

impl Eq for HeapBufferedStream {}

impl PartialOrd for HeapBufferedStream {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for HeapBufferedStream {
    fn cmp(&self, other: &Self) -> Ordering {
        let ordering = if self.reverse {
            // keep the original ordering so the greater row comes before the smaller one.
            self.schema
                .compare_row(&self.first_row_in_buffer(), &other.first_row_in_buffer())
        } else {
            // reverse the original ordering so the smaller row comes before the greater
            // one.
            self.schema
                .compare_row(&other.first_row_in_buffer(), &self.first_row_in_buffer())
        };

        if let Ordering::Equal = ordering {
            // The larger sequence number should always comes before the smaller one.
            self.sequence_in_buffer().cmp(&other.sequence_in_buffer())
        } else {
            ordering
        }
    }
}

/// Metrics for merge iterator.
#[derive(TraceMetricWhenDrop)]
pub struct Metrics {
    #[metric(number)]
    num_memtables: usize,
    #[metric(number)]
    num_ssts: usize,
    /// Total rows collected using fetch_rows_from_one_stream().
    #[metric(number)]
    total_rows_fetch_from_one: usize,
    /// Times to fetch rows from one stream.
    #[metric(number)]
    times_fetch_rows_from_one: usize,
    /// Times to fetch one row from multiple stream.
    #[metric(number)]
    times_fetch_row_from_multiple: usize,
    /// Init time cost of the metrics.
    #[metric(duration)]
    init_duration: Duration,
    /// Scan time cost of the metrics.
    #[metric(duration)]
    scan_duration: Duration,
    /// Scan count
    #[metric(number)]
    scan_count: usize,
    #[metric(collector)]
    metrics_collector: Option<MetricsCollector>,
}

impl Metrics {
    fn new(num_memtables: usize, num_ssts: usize, collector: Option<MetricsCollector>) -> Self {
        Self {
            num_memtables,
            num_ssts,
            times_fetch_rows_from_one: 0,
            total_rows_fetch_from_one: 0,
            times_fetch_row_from_multiple: 0,
            init_duration: Duration::default(),
            scan_duration: Duration::default(),
            scan_count: 0,
            metrics_collector: collector,
        }
    }
}

pub struct MergeIterator {
    table_id: TableId,
    request_id: RequestId,
    inited: bool,
    schema: RecordSchemaWithKey,
    record_batch_builder: RecordBatchWithKeyBuilder,
    origin_streams: Vec<SequencedRecordBatchStream>,
    /// ssts are kept here to avoid them from being purged.
    #[allow(dead_code)]
    ssts: Vec<Vec<FileHandle>>,
    /// Any [BufferedStream] in the hot heap is not empty.
    hot: BinaryHeap<HeapBufferedStream>,
    /// Any [BufferedStream] in the cold heap is not empty.
    cold: BinaryHeap<HeapBufferedStream>,
    iter_options: IterOptions,
    reverse: bool,
    metrics: Metrics,
}

impl MergeIterator {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        table_id: TableId,
        request_id: RequestId,
        schema: RecordSchemaWithKey,
        streams: Vec<SequencedRecordBatchStream>,
        ssts: Vec<Vec<FileHandle>>,
        iter_options: IterOptions,
        reverse: bool,
        metrics: Metrics,
    ) -> Self {
        let heap_cap = streams.len();
        let record_batch_builder =
            RecordBatchWithKeyBuilder::with_capacity(schema.clone(), iter_options.batch_size);
        Self {
            table_id,
            request_id,
            inited: false,
            schema,
            ssts,
            record_batch_builder,
            origin_streams: streams,
            hot: BinaryHeap::with_capacity(heap_cap),
            cold: BinaryHeap::with_capacity(heap_cap),
            iter_options,
            reverse,
            metrics,
        }
    }

    fn merge_window_end(&self) -> Option<RowViewOnBatch> {
        self.hot.peek().as_ref().map(|v| v.last_row_in_buffer())
    }

    async fn init_if_necessary(&mut self) -> Result<()> {
        if self.inited {
            return Ok(());
        }

        debug!(
            "Merge iterator init, table_id:{:?}, request_id:{}, schema:{:?}",
            self.table_id, self.request_id, self.schema
        );
        let init_start = Instant::now();

        // Initialize buffered streams concurrently.
        let mut init_buffered_streams = Vec::with_capacity(self.origin_streams.len());
        for origin_stream in mem::take(&mut self.origin_streams) {
            let schema = self.schema.clone();
            init_buffered_streams
                .push(async move { BufferedStream::build(schema, origin_stream).await });
        }

        let pull_start = Instant::now();
        let buffered_streams = try_join_all(init_buffered_streams).await?;
        self.metrics.scan_duration += pull_start.elapsed();
        self.metrics.scan_count += buffered_streams.len();

        // Push streams to heap.
        let current_schema = &self.schema;
        for buffered_stream in buffered_streams {
            let stream_schema = buffered_stream.schema();
            ensure!(
                current_schema == stream_schema,
                MismatchedSchema {
                    expect: current_schema.clone(),
                    given: stream_schema.clone(),
                }
            );

            if !buffered_stream.is_exhausted() {
                self.cold.push(buffered_stream.into_heaped(self.reverse));
            }
        }
        self.refill_hot();

        self.inited = true;
        self.metrics.init_duration = init_start.elapsed();

        Ok(())
    }

    fn refill_hot(&mut self) {
        while !self.cold.is_empty() {
            if !self.hot.is_empty() {
                let merge_window_end = self.merge_window_end().unwrap();
                let warmest = self.cold.peek().unwrap();
                if warmest.is_after_boundary(&self.schema, &merge_window_end) {
                    // if the warmest stream in the cold stream sets is totally after the
                    // merge_window_end then no need to add more streams into
                    // the hot stream sets for merge sorting.
                    break;
                }
            }

            let warmest = self.cold.pop().unwrap();
            self.hot.push(warmest);
        }
    }

    /// Pull the next batch Rearrange the heap
    async fn reheap(&mut self, mut buffered_stream: HeapBufferedStream) -> Result<()> {
        let pulled_new_batch = buffered_stream
            .pull_next_batch_if_necessary(&mut self.metrics)
            .await?;

        if buffered_stream.is_exhausted() {
            self.refill_hot();
        } else if pulled_new_batch {
            // TODO(xikai): it seems no need to decide to which heap push the
            // `buffered_stream`. Just put the new batch into the cold heap if
            // the max bound of the hottest batch is smaller than the min bound
            // of new one.
            let cold_new_batch = if let Some(hottest) = self.hot.peek() {
                buffered_stream.is_after_boundary(&self.schema, &hottest.last_row_in_buffer())
            } else {
                false
            };

            if cold_new_batch {
                self.cold.push(buffered_stream);
            } else {
                self.hot.push(buffered_stream);
            }
            self.refill_hot();
        } else {
            // No new batch is pulled and the `buffered_stream` is not exhausted so just put
            // it back to the hot heap.
            self.hot.push(buffered_stream);
        }

        Ok(())
    }

    /// Fetch at most `num_rows_to_fetch` rows from the hottest
    /// `BufferedStream`.
    ///
    /// If the inner builder is empty, returns a slice of the record batch in
    /// stream.
    async fn fetch_rows_from_one_stream(
        &mut self,
        num_rows_to_fetch: usize,
    ) -> Result<Option<RecordBatchWithKey>> {
        assert_eq!(self.hot.len(), 1);
        self.metrics.times_fetch_rows_from_one += 1;

        let mut buffered_stream = self.hot.pop().unwrap();

        let record_batch = if self.record_batch_builder.is_empty() {
            let record_batch = buffered_stream.take_record_batch_slice(num_rows_to_fetch);

            self.metrics.total_rows_fetch_from_one += record_batch.num_rows();

            Some(record_batch)
        } else {
            let fetched_row_num = buffered_stream
                .append_rows_to(&mut self.record_batch_builder, num_rows_to_fetch)?;

            self.metrics.total_rows_fetch_from_one += fetched_row_num;

            None
        };

        self.reheap(buffered_stream).await?;

        Ok(record_batch)
    }

    /// Fetch one row from the hottest `BufferedStream`.
    ///
    /// REQUIRES: `self.hot` is not empty.
    async fn fetch_one_row_from_multiple_streams(&mut self) -> Result<()> {
        assert!(!self.hot.is_empty());
        self.metrics.times_fetch_row_from_multiple += 1;

        let mut hottest = self.hot.pop().unwrap();
        let row = hottest.next_row_in_buffer().unwrap();
        self.record_batch_builder
            .append_row_view(&row)
            .context(AppendRow)?;
        self.reheap(hottest).await
    }

    /// Fetch the next batch from the streams.
    ///
    /// `init_if_necessary` should be finished before this method.
    async fn fetch_next_batch(&mut self) -> Result<Option<RecordBatchWithKey>> {
        self.init_if_necessary().await?;

        self.record_batch_builder.clear();

        while !self.hot.is_empty() && self.record_batch_builder.len() < self.iter_options.batch_size
        {
            // no need to do merge sort if only one batch in the hot heap.
            if self.hot.len() == 1 {
                let fetch_row_num = self.iter_options.batch_size - self.record_batch_builder.len();

                if let Some(record_batch) = self.fetch_rows_from_one_stream(fetch_row_num).await? {
                    // The builder is empty and we have fetch a record batch from this stream, just
                    // return that batch.
                    return Ok(Some(record_batch));
                }
                // Else, some rows may have been pushed into the builder.
            } else {
                self.fetch_one_row_from_multiple_streams().await?;
            }
        }

        if self.record_batch_builder.is_empty() {
            Ok(None)
        } else {
            let record_batch = self
                .record_batch_builder
                .build()
                .context(BuildRecordBatch)?;
            Ok(Some(record_batch))
        }
    }
}

#[async_trait]
impl RecordBatchWithKeyIterator for MergeIterator {
    type Error = Error;

    fn schema(&self) -> &RecordSchemaWithKey {
        &self.schema
    }

    async fn next_batch(&mut self) -> Result<Option<RecordBatchWithKey>> {
        let record_batch = self.fetch_next_batch().await?;

        trace!("MergeIterator send next record batch:{:?}", record_batch);

        Ok(record_batch)
    }
}

#[cfg(test)]
mod tests {
    use common_types::{
        self,
        tests::{build_row, build_schema},
    };

    use super::*;
    use crate::row_iter::tests::check_iterator;

    #[tokio::test]
    async fn test_row_merge_iterator() {
        // first two columns are key columns
        let schema = build_schema();

        let testcases = vec![
            // (sequence, rows)
            (
                10,
                vec![build_row(b"y", 1000000, 10.0, "v4", 1000, 1_000_000)],
            ),
            (
                20,
                vec![build_row(b"y", 1000000, 10.0, "v3", 1000, 1_000_000)],
            ),
            (
                100,
                vec![build_row(b"b", 1000000, 10.0, "v2", 1000, 1_000_000)],
            ),
            (
                1,
                vec![build_row(b"a", 1000000, 10.0, "v1", 1000, 1_000_000)],
            ),
        ];

        let streams =
            record_batch_stream::tests::build_sequenced_record_batch_stream(&schema, testcases);
        let mut iter = MergeIterator::new(
            TableId::MIN,
            RequestId::next_id(),
            schema.to_record_schema_with_key(),
            streams,
            Vec::new(),
            IterOptions { batch_size: 500 },
            false,
            Metrics::new(1, 1, None),
        );

        check_iterator(
            &mut iter,
            vec![
                build_row(b"a", 1000000, 10.0, "v1", 1000, 1_000_000),
                build_row(b"b", 1000000, 10.0, "v2", 1000, 1_000_000),
                build_row(b"y", 1000000, 10.0, "v3", 1000, 1_000_000),
                build_row(b"y", 1000000, 10.0, "v4", 1000, 1_000_000),
            ],
        )
        .await;
    }

    #[tokio::test]
    async fn test_row_merge_iterator_reverse() {
        // first two columns are key columns
        let schema = build_schema();

        let testcases = vec![
            // (sequence, rows)
            (
                10,
                vec![
                    build_row(b"y", 1000001, 10.0, "v5", 1000, 1_000_000),
                    build_row(b"y", 1000000, 10.0, "v4", 1000, 1_000_000),
                ],
            ),
            (
                20,
                vec![build_row(b"y", 1000000, 10.0, "v3", 1000, 1_000_000)],
            ),
            (
                100,
                vec![build_row(b"b", 1000000, 10.0, "v2", 1000, 1_000_000)],
            ),
            (
                1,
                vec![build_row(b"a", 1000000, 10.0, "v1", 1000, 1_000_000)],
            ),
        ];

        let streams =
            record_batch_stream::tests::build_sequenced_record_batch_stream(&schema, testcases);
        let mut iter = MergeIterator::new(
            TableId::MIN,
            RequestId::next_id(),
            schema.to_record_schema_with_key(),
            streams,
            Vec::new(),
            IterOptions { batch_size: 500 },
            true,
            Metrics::new(1, 1, None),
        );

        check_iterator(
            &mut iter,
            vec![
                build_row(b"y", 1000001, 10.0, "v5", 1000, 1_000_000),
                build_row(b"y", 1000000, 10.0, "v3", 1000, 1_000_000),
                build_row(b"y", 1000000, 10.0, "v4", 1000, 1_000_000),
                build_row(b"b", 1000000, 10.0, "v2", 1000, 1_000_000),
                build_row(b"a", 1000000, 10.0, "v1", 1000, 1_000_000),
            ],
        )
        .await;
    }
}
