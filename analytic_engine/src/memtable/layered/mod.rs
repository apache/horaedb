// Copyright 2023 The CeresDB Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! MemTable based on skiplist

pub mod factory;
pub mod iter;

use std::{
    fmt::format,
    mem,
    ops::{Bound, Deref},
    sync::{
        atomic::{self, AtomicI64, AtomicU64, AtomicUsize},
        Arc, RwLock,
    },
};

use arena::{Arena, BasicStats, CollectorRef};
use bytes_ext::Bytes;
use codec::Encoder;
use common_types::{
    projected_schema::ProjectedSchema,
    record_batch::RecordBatchWithKey,
    row::{contiguous::ContiguousRowWriter, Row},
    schema::Schema,
    time::TimeRange,
    SequenceNumber,
};
use generic_error::BoxError;
use logger::{debug, trace};
use skiplist::{BytewiseComparator, KeyComparator, Skiplist};
use snafu::{ensure, OptionExt, ResultExt};

use crate::memtable::{
    factory::{Factory, FactoryRef, Options},
    key::{ComparableInternalKey, KeySequence},
    layered::iter::ColumnarIterImpl,
    ColumnarIterPtr, EncodeInternalKey, Internal, InternalNoCause, InvalidPutSequence, InvalidRow,
    MemTable, MemTableRef, Metrics as MemtableMetrics, PutContext, Result, ScanContext,
    ScanRequest, TimestampNotFound,
};

#[derive(Default, Debug)]
struct Metrics {
    row_raw_size: AtomicUsize,
    row_encoded_size: AtomicUsize,
    row_count: AtomicUsize,
}

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
            inner.approximate_memory_usage()
        };

        if memory_usage > self.mutable_switch_threshold {
            let inner = &mut *self.inner.write().unwrap();
            inner.switch_mutable_segment(self.schema.clone())?;
        }

        Ok(())
    }

    fn scan(&self, ctx: ScanContext, request: ScanRequest) -> Result<ColumnarIterPtr> {
        let inner = self.inner.read().unwrap();
        inner.scan(ctx, request)
    }

    fn approximate_memory_usage(&self) -> usize {
        self.inner.read().unwrap().approximate_memory_usage()
    }

    fn set_last_sequence(&self, sequence: SequenceNumber) -> Result<()> {
        Ok(self
            .last_sequence
            .store(sequence, atomic::Ordering::Relaxed))
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

        let mutable_segment_builder = MutableSegmentBuilder {
            memtable_factory,
            opts: builder_opts,
        };

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
    fn scan(&self, ctx: ScanContext, request: ScanRequest) -> Result<ColumnarIterPtr> {
        let iter = ColumnarIterImpl::new(
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
        // Build a new mutable segment, and replace current's.
        let new_mutable = self.mutable_segment_builder.build()?;
        let current_mutable = mem::replace(&mut self.mutable_segment, new_mutable);

        // Convert current's to immutable.
        let scan_ctx = ScanContext::default();
        let scan_req = ScanRequest {
            start_user_key: Bound::Unbounded,
            end_user_key: Bound::Unbounded,
            sequence: common_types::MAX_SEQUENCE_NUMBER,
            projected_schema: ProjectedSchema::no_projection(schema),
            need_dedup: false,
            reverse: false,
            metrics_collector: None,
            time_range: TimeRange::min_to_max(),
        };

        let immutable_batches = current_mutable
            .scan(scan_ctx, scan_req)?
            .collect::<Result<Vec<_>>>()?;
        let time_range = current_mutable.time_range().context(InternalNoCause {
            msg: "failed to get time range from mutable segment",
        })?;
        let max_key = current_mutable.max_key().context(InternalNoCause {
            msg: "failed to get time range from mutable segment",
        })?;
        let min_key = current_mutable.min_key().context(InternalNoCause {
            msg: "failed to get time range from mutable segment",
        })?;
        let immutable = ImmutableSegment::new(immutable_batches, time_range, min_key, max_key);

        self.immutable_segments.push(immutable);

        Ok(())
    }

    pub fn min_key(&self) -> Option<Bytes> {
        let comparator = BytewiseComparator;

        let mut mutable_min_key = self.mutable_segment.min_key();

        let immutable_min_key = if self.immutable_segments.is_empty() {
            None
        } else {
            let mut min_key = self.immutable_segments.first().unwrap().min_key();
            let mut imm_iter = self.immutable_segments.iter();
            let _ = imm_iter.next();
            for imm in imm_iter {
                match comparator.compare_key(&min_key, &imm.min_key) {
                    std::cmp::Ordering::Greater => min_key = imm.min_key(),
                    std::cmp::Ordering::Less | std::cmp::Ordering::Equal => (),
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

        let mut mutable_max_key = self.mutable_segment.max_key();

        let immutable_max_key = if self.immutable_segments.is_empty() {
            None
        } else {
            let mut max_key = self.immutable_segments.first().unwrap().max_key();
            let mut imm_iter = self.immutable_segments.iter();
            let _ = imm_iter.next();
            for imm in imm_iter {
                match comparator.compare_key(&max_key, &imm.max_key) {
                    std::cmp::Ordering::Less => max_key = imm.max_key(),
                    std::cmp::Ordering::Greater | std::cmp::Ordering::Equal => (),
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
                time_range = time_range.intersected_range(imm.time_range()).unwrap();
            }

            Some(time_range)
        };

        match (mutable_time_range, immutable_time_range) {
            (None, None) => None,
            (None, Some(range)) | (Some(range), None) => Some(range),
            (Some(range1), Some(range2)) => Some(range1.intersected_range(range2).unwrap()),
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
            .box_err()
            .context(Internal {
                msg: "failed to build mutable segment",
            })?;

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
    record_batches: Vec<RecordBatchWithKey>,

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
        record_batches: Vec<RecordBatchWithKey>,
        time_range: TimeRange,
        min_key: Bytes,
        max_key: Bytes,
    ) -> Self {
        let approximate_memory_size = record_batches
            .iter()
            .map(|batch| batch.as_arrow_record_batch().get_array_memory_size())
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
    pub fn record_batches(&self) -> &[RecordBatchWithKey] {
        &self.record_batches
    }

    pub fn approximate_memory_usage(&self) -> usize {
        self.approximate_memory_size
    }
}
