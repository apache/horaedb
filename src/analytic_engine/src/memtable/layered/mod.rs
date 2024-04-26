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

//! MemTable based on skiplist

pub mod factory;
pub mod iter;

use std::{
    mem,
    ops::{Bound, Deref},
    sync::{
        atomic::{self, AtomicU64},
        RwLock,
    },
};

use anyhow::Context;
use arena::CollectorRef;
use arrow::record_batch::RecordBatch as ArrowRecordBatch;
use bytes_ext::Bytes;
use common_types::{
    projected_schema::RowProjectorBuilder, row::Row, schema::Schema, time::TimeRange,
    SequenceNumber,
};
use logger::debug;
use skiplist::{BytewiseComparator, KeyComparator};

use crate::memtable::{
    factory::{FactoryRef, Options},
    key::KeySequence,
    layered::iter::ColumnarIterImpl,
    ColumnarIterPtr, MemTable, MemTableRef, Metrics as MemtableMetrics, PutContext, Result,
    ScanContext, ScanRequest,
};

/// MemTable implementation based on skiplist
pub(crate) struct LayeredMemTable {
    /// Schema of this memtable, is immutable.
    schema: Schema,

    /// The last sequence of the rows in this memtable. Update to this field
    /// require external synchronization.
    last_sequence: AtomicU64,

    inner: RwLock<Inner>,

    mutable_switch_threshold: usize,
}

impl LayeredMemTable {
    pub fn new(
        opts: &Options,
        inner_memtable_factory: FactoryRef,
        mutable_switch_threshold: usize,
    ) -> Result<Self> {
        let inner = Inner::new(inner_memtable_factory, opts)?;

        Ok(Self {
            schema: opts.schema.clone(),
            last_sequence: AtomicU64::new(opts.creation_sequence),
            inner: RwLock::new(inner),
            mutable_switch_threshold,
        })
    }

    // Used for testing only
    #[cfg(test)]
    fn force_switch_mutable_segment(&self) -> Result<()> {
        let inner = &mut *self.inner.write().unwrap();
        inner.switch_mutable_segment(self.schema.clone())
    }
}

impl MemTable for LayeredMemTable {
    fn schema(&self) -> &Schema {
        &self.schema
    }

    fn min_key(&self) -> Option<Bytes> {
        self.inner.read().unwrap().min_key()
    }

    fn max_key(&self) -> Option<Bytes> {
        self.inner.read().unwrap().max_key()
    }

    fn put(
        &self,
        ctx: &mut PutContext,
        sequence: KeySequence,
        row: &Row,
        schema: &Schema,
    ) -> Result<()> {
        let memory_usage = {
            let inner = self.inner.read().unwrap();
            inner.put(ctx, sequence, row, schema)?;
            inner.mutable_segment.0.approximate_memory_usage()
        };

        if memory_usage > self.mutable_switch_threshold {
            debug!(
                "LayeredMemTable put, memory_usage:{memory_usage}, mutable_switch_threshold:{}",
                self.mutable_switch_threshold
            );
            let inner = &mut *self.inner.write().unwrap();
            inner.switch_mutable_segment(self.schema.clone())?;
        }

        Ok(())
    }

    fn scan(&self, ctx: ScanContext, request: ScanRequest) -> Result<ColumnarIterPtr> {
        let inner = self.inner.read().unwrap();
        inner.scan(&self.schema, ctx, request)
    }

    fn approximate_memory_usage(&self) -> usize {
        self.inner.read().unwrap().approximate_memory_usage()
    }

    fn set_last_sequence(&self, sequence: SequenceNumber) -> Result<()> {
        self.last_sequence
            .store(sequence, atomic::Ordering::Relaxed);
        Ok(())
    }

    fn last_sequence(&self) -> SequenceNumber {
        self.last_sequence.load(atomic::Ordering::Relaxed)
    }

    fn time_range(&self) -> Option<TimeRange> {
        let inner = self.inner.read().unwrap();
        inner.time_range()
    }

    fn metrics(&self) -> MemtableMetrics {
        // FIXME: stats and return metrics
        MemtableMetrics::default()
    }
}

/// Layered memtable inner
struct Inner {
    mutable_segment_builder: MutableSegmentBuilder,
    mutable_segment: MutableSegment,
    immutable_segments: Vec<ImmutableSegment>,
}

impl Inner {
    fn new(memtable_factory: FactoryRef, opts: &Options) -> Result<Self> {
        let builder_opts = MutableBuilderOptions {
            schema: opts.schema.clone(),
            arena_block_size: opts.arena_block_size,
            collector: opts.collector.clone(),
        };
        let mutable_segment_builder = MutableSegmentBuilder::new(memtable_factory, builder_opts);

        // Build the first mutable batch.
        let init_mutable_segment = mutable_segment_builder.build()?;

        Ok(Self {
            mutable_segment_builder,
            mutable_segment: init_mutable_segment,
            immutable_segments: vec![],
        })
    }

    /// Scan batches including `mutable` and `immutable`s.
    #[inline]
    fn scan(
        &self,
        schema: &Schema,
        ctx: ScanContext,
        request: ScanRequest,
    ) -> Result<ColumnarIterPtr> {
        let iter = ColumnarIterImpl::new(
            schema,
            ctx,
            request,
            &self.mutable_segment,
            &self.immutable_segments,
        )?;
        Ok(Box::new(iter))
    }

    #[inline]
    fn put(
        &self,
        ctx: &mut PutContext,
        sequence: KeySequence,
        row: &Row,
        schema: &Schema,
    ) -> Result<()> {
        self.mutable_segment.put(ctx, sequence, row, schema)
    }

    fn switch_mutable_segment(&mut self, schema: Schema) -> Result<()> {
        let imm_num = self.immutable_segments.len();
        debug!("LayeredMemTable switch_mutable_segment, imm_num:{imm_num}");

        // Build a new mutable segment, and replace current's.
        let new_mutable = self.mutable_segment_builder.build()?;
        let current_mutable = mem::replace(&mut self.mutable_segment, new_mutable);
        let fetched_schema = schema.to_record_schema();

        // Convert current's to immutable.
        let scan_ctx = ScanContext::default();
        let row_projector_builder = RowProjectorBuilder::new(fetched_schema, schema, None);
        let scan_req = ScanRequest {
            start_user_key: Bound::Unbounded,
            end_user_key: Bound::Unbounded,
            sequence: common_types::MAX_SEQUENCE_NUMBER,
            need_dedup: false,
            reverse: false,
            metrics_collector: None,
            time_range: TimeRange::min_to_max(),
            row_projector_builder,
        };

        let immutable_batches = current_mutable
            .scan(scan_ctx, scan_req)?
            .map(|batch_res| batch_res.map(|batch| batch.into_arrow_record_batch()))
            .collect::<Result<Vec<_>>>()?;

        let time_range = current_mutable
            .time_range()
            .context("failed to get time range from mutable segment")?;
        let max_key = current_mutable
            .max_key()
            .context("failed to get max key from mutable segment")?;
        let min_key = current_mutable
            .min_key()
            .context("failed to get min key from mutable segment")?;
        let immutable = ImmutableSegment::new(immutable_batches, time_range, min_key, max_key);

        self.immutable_segments.push(immutable);

        Ok(())
    }

    pub fn min_key(&self) -> Option<Bytes> {
        let comparator = BytewiseComparator;

        let mutable_min_key = self.mutable_segment.min_key();

        let immutable_min_key = if self.immutable_segments.is_empty() {
            None
        } else {
            let mut min_key = self.immutable_segments.first().unwrap().min_key();
            let mut imm_iter = self.immutable_segments.iter();
            let _ = imm_iter.next();
            for imm in imm_iter {
                if let std::cmp::Ordering::Greater = comparator.compare_key(&min_key, &imm.min_key)
                {
                    min_key = imm.min_key();
                }
            }

            Some(min_key)
        };

        match (mutable_min_key, immutable_min_key) {
            (None, None) => None,
            (None, Some(key)) | (Some(key), None) => Some(key),
            (Some(key1), Some(key2)) => Some(match comparator.compare_key(&key1, &key2) {
                std::cmp::Ordering::Greater => key2,
                std::cmp::Ordering::Less | std::cmp::Ordering::Equal => key1,
            }),
        }
    }

    pub fn max_key(&self) -> Option<Bytes> {
        let comparator = BytewiseComparator;

        let mutable_max_key = self.mutable_segment.max_key();

        let immutable_max_key = if self.immutable_segments.is_empty() {
            None
        } else {
            let mut max_key = self.immutable_segments.first().unwrap().max_key();
            let mut imm_iter = self.immutable_segments.iter();
            let _ = imm_iter.next();
            for imm in imm_iter {
                if let std::cmp::Ordering::Less = comparator.compare_key(&max_key, &imm.max_key) {
                    max_key = imm.max_key();
                }
            }

            Some(max_key)
        };

        match (mutable_max_key, immutable_max_key) {
            (None, None) => None,
            (None, Some(key)) | (Some(key), None) => Some(key),
            (Some(key1), Some(key2)) => Some(match comparator.compare_key(&key1, &key2) {
                std::cmp::Ordering::Less => key2,
                std::cmp::Ordering::Greater | std::cmp::Ordering::Equal => key1,
            }),
        }
    }

    pub fn time_range(&self) -> Option<TimeRange> {
        let mutable_time_range = self.mutable_segment.time_range();

        let immutable_time_range = if self.immutable_segments.is_empty() {
            None
        } else {
            let mut time_range = self.immutable_segments.first().unwrap().time_range();
            let mut imm_iter = self.immutable_segments.iter();
            let _ = imm_iter.next();
            for imm in imm_iter {
                time_range = time_range.merge_range(imm.time_range());
            }

            Some(time_range)
        };

        match (mutable_time_range, immutable_time_range) {
            (None, None) => None,
            (None, Some(range)) | (Some(range), None) => Some(range),
            (Some(range1), Some(range2)) => Some(range1.merge_range(range2)),
        }
    }

    fn approximate_memory_usage(&self) -> usize {
        let mutable_mem_usage = self.mutable_segment.approximate_memory_usage();

        let immutable_mem_usage = self
            .immutable_segments
            .iter()
            .map(|imm| imm.approximate_memory_usage())
            .sum::<usize>();

        mutable_mem_usage + immutable_mem_usage
    }
}

/// Mutable batch
pub(crate) struct MutableSegment(MemTableRef);

impl Deref for MutableSegment {
    type Target = MemTableRef;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

/// Builder for `MutableBatch`
struct MutableSegmentBuilder {
    memtable_factory: FactoryRef,
    opts: MutableBuilderOptions,
}

impl MutableSegmentBuilder {
    fn new(memtable_factory: FactoryRef, opts: MutableBuilderOptions) -> Self {
        Self {
            memtable_factory,
            opts,
        }
    }

    fn build(&self) -> Result<MutableSegment> {
        let memtable_opts = Options {
            schema: self.opts.schema.clone(),
            arena_block_size: self.opts.arena_block_size,
            // `creation_sequence` is meaningless in inner memtable, just set it to min.
            creation_sequence: SequenceNumber::MIN,
            collector: self.opts.collector.clone(),
        };

        let memtable = self
            .memtable_factory
            .create_memtable(memtable_opts)
            .context("failed to build mutable segment")?;

        Ok(MutableSegment(memtable))
    }
}

struct MutableBuilderOptions {
    pub schema: Schema,

    /// Block size of arena in bytes.
    pub arena_block_size: u32,

    /// Memory usage collector
    pub collector: CollectorRef,
}

/// Immutable batch
pub(crate) struct ImmutableSegment {
    /// Record batch converted from `MutableBatch`
    record_batches: Vec<ArrowRecordBatch>,

    /// Min time of source `MutableBatch`
    time_range: TimeRange,

    /// Min key of source `MutableBatch`
    min_key: Bytes,

    /// Max key of source `MutableBatch`
    max_key: Bytes,

    approximate_memory_size: usize,
}

impl ImmutableSegment {
    fn new(
        record_batches: Vec<ArrowRecordBatch>,
        time_range: TimeRange,
        min_key: Bytes,
        max_key: Bytes,
    ) -> Self {
        let approximate_memory_size = record_batches
            .iter()
            .map(|batch| batch.get_array_memory_size())
            .sum();

        Self {
            record_batches,
            time_range,
            min_key,
            max_key,
            approximate_memory_size,
        }
    }

    pub fn time_range(&self) -> TimeRange {
        self.time_range
    }

    pub fn min_key(&self) -> Bytes {
        self.min_key.clone()
    }

    pub fn max_key(&self) -> Bytes {
        self.max_key.clone()
    }

    // TODO: maybe return a iterator?
    pub fn record_batches(&self) -> &[ArrowRecordBatch] {
        &self.record_batches
    }

    pub fn approximate_memory_usage(&self) -> usize {
        self.approximate_memory_size
    }
}

#[cfg(test)]
mod tests {

    use std::{ops::Bound, sync::Arc};

    use arena::NoopCollector;
    use bytes_ext::ByteVec;
    use codec::{memcomparable::MemComparable, Encoder};
    use common_types::{
        datum::Datum,
        projected_schema::{ProjectedSchema, RowProjectorBuilder},
        record_batch::FetchedRecordBatch,
        row::Row,
        schema::IndexInWriterSchema,
        tests::{build_row, build_schema},
    };

    use super::*;
    use crate::memtable::{
        factory::Options,
        key::ComparableInternalKey,
        skiplist::factory::SkiplistMemTableFactory,
        test_util::{TestMemtableBuilder, TestUtil},
        MemTableRef,
    };

    struct TestMemtableBuilderImpl;

    impl TestMemtableBuilder for TestMemtableBuilderImpl {
        fn build(&self, data: &[(KeySequence, Row)]) -> MemTableRef {
            let schema = build_schema();
            let factory = SkiplistMemTableFactory;
            let opts = Options {
                schema: schema.clone(),
                arena_block_size: 512,
                creation_sequence: 1,
                collector: Arc::new(NoopCollector {}),
            };
            let memtable = LayeredMemTable::new(&opts, Arc::new(factory), usize::MAX).unwrap();

            let mut ctx =
                PutContext::new(IndexInWriterSchema::for_same_schema(schema.num_columns()));
            let partitioned_data = data.chunks(3).collect::<Vec<_>>();
            let chunk_num = partitioned_data.len();

            for chunk in partitioned_data.iter().take(chunk_num - 1) {
                for (seq, row) in *chunk {
                    memtable.put(&mut ctx, *seq, row, &schema).unwrap();
                }
                memtable.force_switch_mutable_segment().unwrap();
            }

            let last_chunk = partitioned_data[chunk_num - 1];
            for (seq, row) in last_chunk {
                memtable.put(&mut ctx, *seq, row, &schema).unwrap();
            }

            Arc::new(memtable)
        }
    }

    fn test_data() -> Vec<(KeySequence, Row)> {
        vec![
            (
                KeySequence::new(1, 1),
                build_row(b"a", 1, 10.0, "v1", 1000, 1_000_000),
            ),
            (
                KeySequence::new(1, 2),
                build_row(b"b", 2, 10.0, "v2", 2000, 2_000_000),
            ),
            (
                KeySequence::new(1, 4),
                build_row(b"c", 3, 10.0, "v3", 3000, 3_000_000),
            ),
            (
                KeySequence::new(2, 1),
                build_row(b"d", 4, 10.0, "v4", 4000, 4_000_000),
            ),
            (
                KeySequence::new(2, 1),
                build_row(b"e", 5, 10.0, "v5", 5000, 5_000_000),
            ),
            (
                KeySequence::new(2, 3),
                build_row(b"f", 6, 10.0, "v6", 6000, 6_000_000),
            ),
            (
                KeySequence::new(3, 4),
                build_row(b"g", 7, 10.0, "v7", 7000, 7_000_000),
            ),
        ]
    }

    #[test]
    fn test_memtable_scan() {
        let builder = TestMemtableBuilderImpl;
        let data = test_data();
        let test_util = TestUtil::new(builder, data);
        let memtable = test_util.memtable();
        let schema = memtable.schema().clone();

        // No projection.
        let projection = (0..schema.num_columns()).collect::<Vec<_>>();
        let expected = test_util.data();
        test_memtable_scan_internal(
            schema.clone(),
            projection,
            TimeRange::min_to_max(),
            memtable.clone(),
            expected,
        );

        // Projection to first three.
        let projection = vec![0, 1, 3];
        let expected = test_util
            .data()
            .iter()
            .map(|row| {
                let datums = vec![row[0].clone(), row[1].clone(), row[3].clone()];
                Row::from_datums(datums)
            })
            .collect();
        test_memtable_scan_internal(
            schema.clone(),
            projection,
            TimeRange::min_to_max(),
            memtable.clone(),
            expected,
        );

        // No projection.
        let projection = (0..schema.num_columns()).collect::<Vec<_>>();
        let time_range = TimeRange::new(2.into(), 7.into()).unwrap();
        // Memtable data after switching may be like(just showing timestamp column using
        // to filter):  [1, 2, 3], [4, 5, 6], [7]
        //
        // And the target time range is: [2, 7)
        //
        // So the filter result should be: [1, 2, 3], [4, 5, 6]
        let expected = test_util
            .data()
            .iter()
            .enumerate()
            .filter_map(|(idx, row)| if idx < 6 { Some(row.clone()) } else { None })
            .collect();
        test_memtable_scan_internal(
            schema.clone(),
            projection,
            time_range,
            memtable.clone(),
            expected,
        );
    }

    #[test]
    fn test_time_range() {
        let builder = TestMemtableBuilderImpl;
        let data = test_data();
        let test_util = TestUtil::new(builder, data);
        let memtable = test_util.memtable();

        assert_eq!(TimeRange::new(1.into(), 8.into()), memtable.time_range());
    }

    #[test]
    fn test_min_max_key() {
        let builder = TestMemtableBuilderImpl;
        let data = test_data();
        let test_util = TestUtil::new(builder, data.clone());
        let memtable = test_util.memtable();
        let schema = memtable.schema();

        // Min key
        let key_encoder = ComparableInternalKey::new(data[0].0, schema);
        let mut min_key = Vec::with_capacity(key_encoder.estimate_encoded_size(&data[0].1));
        key_encoder.encode(&mut min_key, &data[0].1).unwrap();
        let key_encoder = ComparableInternalKey::new(data[0].0, schema);
        let mut min_key = Vec::with_capacity(key_encoder.estimate_encoded_size(&data[0].1));
        key_encoder.encode(&mut min_key, &data[0].1).unwrap();

        // Max key
        let key_encoder = ComparableInternalKey::new(data[6].0, schema);
        let mut max_key = Vec::with_capacity(key_encoder.estimate_encoded_size(&data[6].1));
        key_encoder.encode(&mut max_key, &data[6].1).unwrap();
        let key_encoder = ComparableInternalKey::new(data[6].0, schema);
        let mut max_key = Vec::with_capacity(key_encoder.estimate_encoded_size(&data[6].1));
        key_encoder.encode(&mut max_key, &data[6].1).unwrap();

        assert_eq!(min_key, memtable.min_key().unwrap().to_vec());
        assert_eq!(max_key, memtable.max_key().unwrap().to_vec());
    }

    fn test_memtable_scan_internal(
        schema: Schema,
        projection: Vec<usize>,
        time_range: TimeRange,
        memtable: Arc<dyn MemTable + Send + Sync>,
        expected: Vec<Row>,
    ) {
        let projected_schema = ProjectedSchema::new(schema, Some(projection)).unwrap();
        let fetched_schema = projected_schema.to_record_schema();
        let table_schema = projected_schema.table_schema();
        let row_projector_builder =
            RowProjectorBuilder::new(fetched_schema, table_schema.clone(), None);

        // limited by sequence
        let scan_request = ScanRequest {
            start_user_key: Bound::Unbounded,
            end_user_key: Bound::Unbounded,
            sequence: SequenceNumber::MAX,
            row_projector_builder,
            need_dedup: false,
            reverse: false,
            metrics_collector: None,
            time_range,
        };
        let scan_ctx = ScanContext::default();
        let iter = memtable.scan(scan_ctx, scan_request).unwrap();
        check_iterator(iter, expected);
    }

    fn check_iterator<T: Iterator<Item = Result<FetchedRecordBatch>>>(
        iter: T,
        expected_rows: Vec<Row>,
    ) {
        // sort it first.
        let mut rows = Vec::new();
        for batch in iter {
            let batch = batch.unwrap();
            for row_idx in 0..batch.num_rows() {
                rows.push(batch.clone_row_at(row_idx));
            }
        }

        rows.sort_by(|a, b| {
            let key1 = build_scan_key(
                &String::from_utf8_lossy(a[0].as_varbinary().unwrap()),
                a[1].as_timestamp().unwrap().as_i64(),
            );
            let key2 = build_scan_key(
                &String::from_utf8_lossy(b[0].as_varbinary().unwrap()),
                b[1].as_timestamp().unwrap().as_i64(),
            );
            BytewiseComparator.compare_key(&key1, &key2)
        });

        assert_eq!(rows, expected_rows);
    }

    fn build_scan_key(c1: &str, c2: i64) -> Bytes {
        let mut buf = ByteVec::new();
        let encoder = MemComparable;
        encoder.encode(&mut buf, &Datum::from(c1)).unwrap();
        encoder.encode(&mut buf, &Datum::from(c2)).unwrap();

        Bytes::from(buf)
    }
}
