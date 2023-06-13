// Copyright 2022-2023 CeresDB Project Authors. Licensed under Apache-2.0.

//! MemTable based on skiplist

pub mod factory;
pub mod iter;

use std::{
    cmp::Ordering,
    convert::TryInto,
    sync::atomic::{self, AtomicU64, AtomicUsize},
};

use arena::{Arena, BasicStats};
use common_types::{
    bytes::Bytes,
    row::{contiguous::ContiguousRowWriter, Row},
    schema::Schema,
    SequenceNumber,
};
use common_util::{codec::Encoder, error::BoxError};
use log::{debug, trace};
use skiplist::{KeyComparator, Skiplist};
use snafu::{ensure, ResultExt};

use crate::memtable::{
    key::{ComparableInternalKey, KeySequence},
    skiplist::iter::{ColumnarIterImpl, ReversedColumnarIterator},
    ColumnarIterPtr, EncodeInternalKey, InvalidPutSequence, InvalidRow, MemTable,
    Metrics as MemtableMetrics, PutContext, Result, ScanContext, ScanRequest,
};

#[derive(Default, Debug)]
struct Metrics {
    row_raw_size: AtomicUsize,
    row_encoded_size: AtomicUsize,
    row_count: AtomicUsize,
}

/// MemTable implementation based on skiplist
pub struct SkiplistMemTable<A: Arena<Stats = BasicStats> + Clone + Sync + Send> {
    /// Schema of this memtable, is immutable.
    schema: Schema,
    skiplist: Skiplist<BytewiseComparator, A>,
    /// The last sequence of the rows in this memtable. Update to this field
    /// require external synchronization.
    last_sequence: AtomicU64,

    metrics: Metrics,
}

impl<A: Arena<Stats = BasicStats> + Clone + Sync + Send + 'static> MemTable
    for SkiplistMemTable<A>
{
    fn schema(&self) -> &Schema {
        &self.schema
    }

    fn min_key(&self) -> Option<Bytes> {
        let mut iter = self.skiplist.iter();
        iter.seek_to_first();
        if !iter.valid() {
            None
        } else {
            Some(iter.key().to_vec().into())
        }
    }

    fn max_key(&self) -> Option<Bytes> {
        let mut iter = self.skiplist.iter();
        iter.seek_to_last();
        if !iter.valid() {
            None
        } else {
            Some(iter.key().to_vec().into())
        }
    }

    // TODO(yingwen): Encode value if value_buf is not set.
    // Now the caller is required to encode the row into the `value_buf` in
    // PutContext first.
    fn put(
        &self,
        ctx: &mut PutContext,
        sequence: KeySequence,
        row: &Row,
        schema: &Schema,
    ) -> Result<()> {
        trace!("skiplist put row, sequence:{:?}, row:{:?}", sequence, row);

        let key_encoder = ComparableInternalKey::new(sequence, schema);

        let internal_key = &mut ctx.key_buf;
        // Reset key buffer
        internal_key.clear();
        // Reserve capacity for key
        internal_key.reserve(key_encoder.estimate_encoded_size(row));
        // Encode key
        key_encoder
            .encode(internal_key, row)
            .context(EncodeInternalKey)?;

        // Encode row value. The ContiguousRowWriter will clear the buf.
        let row_value = &mut ctx.value_buf;
        let mut row_writer = ContiguousRowWriter::new(row_value, schema, &ctx.index_in_writer);
        row_writer.write_row(row).box_err().context(InvalidRow)?;
        let encoded_size = internal_key.len() + row_value.len();
        self.skiplist.put(internal_key, row_value);

        // Update metrics
        self.metrics
            .row_raw_size
            .fetch_add(row.size(), atomic::Ordering::Relaxed);
        self.metrics
            .row_count
            .fetch_add(1, atomic::Ordering::Relaxed);
        self.metrics
            .row_encoded_size
            .fetch_add(encoded_size, atomic::Ordering::Relaxed);

        Ok(())
    }

    fn scan(&self, ctx: ScanContext, request: ScanRequest) -> Result<ColumnarIterPtr> {
        debug!(
            "Scan skiplist memtable, ctx:{:?}, request:{:?}",
            ctx, request
        );

        let num_rows = self.skiplist.len();
        let (reverse, batch_size) = (request.reverse, ctx.batch_size);
        let iter = ColumnarIterImpl::new(self, ctx, request)?;
        if reverse {
            Ok(Box::new(ReversedColumnarIterator::new(
                iter, num_rows, batch_size,
            )))
        } else {
            Ok(Box::new(iter))
        }
    }

    fn approximate_memory_usage(&self) -> usize {
        // Mem size of skiplist is u32, need to cast to usize
        match self.skiplist.mem_size().try_into() {
            Ok(v) => v,
            // The skiplist already use bytes larger than usize
            Err(_) => usize::MAX,
        }
    }

    fn set_last_sequence(&self, sequence: SequenceNumber) -> Result<()> {
        let last = self.last_sequence();
        ensure!(
            sequence >= last,
            InvalidPutSequence {
                given: sequence,
                last
            }
        );

        self.last_sequence
            .store(sequence, atomic::Ordering::Relaxed);

        Ok(())
    }

    fn last_sequence(&self) -> SequenceNumber {
        self.last_sequence.load(atomic::Ordering::Relaxed)
    }

    fn metrics(&self) -> MemtableMetrics {
        let row_raw_size = self.metrics.row_raw_size.load(atomic::Ordering::Relaxed);
        let row_encoded_size = self
            .metrics
            .row_encoded_size
            .load(atomic::Ordering::Relaxed);
        let row_count = self.metrics.row_count.load(atomic::Ordering::Relaxed);
        MemtableMetrics {
            row_raw_size,
            row_encoded_size,
            row_count,
        }
    }
}

#[derive(Debug, Clone)]
pub struct BytewiseComparator;

impl KeyComparator for BytewiseComparator {
    #[inline]
    fn compare_key(&self, lhs: &[u8], rhs: &[u8]) -> Ordering {
        lhs.cmp(rhs)
    }

    #[inline]
    fn same_key(&self, lhs: &[u8], rhs: &[u8]) -> bool {
        lhs == rhs
    }
}

#[cfg(test)]
mod tests {

    use std::{ops::Bound, sync::Arc};

    use arena::NoopCollector;
    use common_types::{
        bytes::ByteVec,
        datum::Datum,
        projected_schema::ProjectedSchema,
        record_batch::RecordBatchWithKey,
        schema::IndexInWriterSchema,
        tests::{build_row, build_schema},
        time::Timestamp,
    };
    use common_util::codec::memcomparable::MemComparable;

    use super::*;
    use crate::memtable::{
        factory::{Factory, Options},
        skiplist::factory::SkiplistMemTableFactory,
    };

    fn test_memtable_scan_for_scan_request(
        schema: Schema,
        memtable: Arc<dyn MemTable + Send + Sync>,
    ) {
        let projection: Vec<usize> = (0..schema.num_columns()).collect();
        let projected_schema = ProjectedSchema::new(schema, Some(projection)).unwrap();

        let testcases = vec![
            (
                // limited by sequence
                ScanRequest {
                    start_user_key: Bound::Unbounded,
                    end_user_key: Bound::Unbounded,
                    sequence: 2,
                    projected_schema: projected_schema.clone(),
                    need_dedup: true,
                    reverse: false,
                    metrics_collector: None,
                },
                vec![
                    build_row(b"a", 1, 10.0, "v1", 1000, 1_000_000),
                    build_row(b"b", 2, 10.0, "v2", 2000, 2_000_000),
                    build_row(b"c", 3, 10.0, "v3", 3000, 3_000_000),
                    build_row(b"d", 4, 10.0, "v4", 4000, 4_000_000),
                    build_row(b"e", 5, 10.0, "v5", 5000, 5_000_000),
                    build_row(b"f", 6, 10.0, "v6", 6000, 6_000_000),
                ],
            ),
            (
                // limited by sequence and start/end key
                ScanRequest {
                    start_user_key: Bound::Included(build_scan_key("a", 1)),
                    end_user_key: Bound::Excluded(build_scan_key("e", 5)),
                    sequence: 2,
                    projected_schema: projected_schema.clone(),
                    need_dedup: true,
                    reverse: false,
                    metrics_collector: None,
                },
                vec![
                    build_row(b"a", 1, 10.0, "v1", 1000, 1_000_000),
                    build_row(b"b", 2, 10.0, "v2", 2000, 2_000_000),
                    build_row(b"c", 3, 10.0, "v3", 3000, 3_000_000),
                    build_row(b"d", 4, 10.0, "v4", 4000, 4_000_000),
                ],
            ),
            (
                // limited by sequence and start/end key
                // but seq is one smaller than last case
                ScanRequest {
                    start_user_key: Bound::Included(build_scan_key("a", 1)),
                    end_user_key: Bound::Excluded(build_scan_key("e", 5)),
                    sequence: 1,
                    projected_schema,
                    need_dedup: true,
                    reverse: false,
                    metrics_collector: None,
                },
                vec![
                    build_row(b"a", 1, 10.0, "v1", 1000, 1_000_000),
                    build_row(b"b", 2, 10.0, "v2", 2000, 2_000_000),
                    build_row(b"c", 3, 10.0, "v3", 3000, 3_000_000),
                ],
            ),
        ];

        for (req, expected) in testcases {
            let scan_ctx = ScanContext::default();
            let iter = memtable.scan(scan_ctx, req).unwrap();
            check_iterator(iter, expected);
        }
    }

    fn test_memtable_scan_for_projection(
        schema: Schema,
        memtable: Arc<dyn MemTable + Send + Sync>,
    ) {
        let projection: Vec<usize> = (0..2).collect();
        let projected_schema = ProjectedSchema::new(schema, Some(projection)).unwrap();

        let testcases = vec![(
            ScanRequest {
                start_user_key: Bound::Included(build_scan_key("a", 1)),
                end_user_key: Bound::Excluded(build_scan_key("e", 5)),
                sequence: 2,
                projected_schema,
                need_dedup: true,
                reverse: false,
                metrics_collector: None,
            },
            vec![
                build_row_for_two_column(b"a", 1),
                build_row_for_two_column(b"b", 2),
                build_row_for_two_column(b"c", 3),
                build_row_for_two_column(b"d", 4),
            ],
        )];

        for (req, expected) in testcases {
            let scan_ctx = ScanContext::default();
            let iter = memtable.scan(scan_ctx, req).unwrap();
            check_iterator(iter, expected);
        }
    }

    #[test]
    fn test_memtable_scan() {
        let schema = build_schema();
        let factory = SkiplistMemTableFactory;
        let memtable = factory
            .create_memtable(Options {
                schema: schema.clone(),
                arena_block_size: 512,
                creation_sequence: 1,
                collector: Arc::new(NoopCollector {}),
            })
            .unwrap();

        let mut ctx = PutContext::new(IndexInWriterSchema::for_same_schema(schema.num_columns()));
        let input = vec![
            (
                KeySequence::new(1, 1),
                build_row(b"a", 1, 10.0, "v1", 1000, 1_000_000),
            ),
            (
                KeySequence::new(1, 2),
                build_row(b"b", 2, 10.0, "v2", 2000, 2_000_000),
            ),
            (
                KeySequence::new(1, 3),
                build_row(
                    b"c",
                    3,
                    10.0,
                    "primary_key same with next row",
                    3000,
                    3_000_000,
                ),
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
        ];

        for (seq, row) in input {
            memtable.put(&mut ctx, seq, &row, &schema).unwrap();
        }

        test_memtable_scan_for_scan_request(schema.clone(), memtable.clone());
        test_memtable_scan_for_projection(schema, memtable);
    }

    fn check_iterator<T: Iterator<Item = Result<RecordBatchWithKey>>>(
        iter: T,
        expected_rows: Vec<Row>,
    ) {
        let mut visited_rows = 0;
        for batch in iter {
            let batch = batch.unwrap();
            for row_idx in 0..batch.num_rows() {
                assert_eq!(batch.clone_row_at(row_idx), expected_rows[visited_rows]);
                visited_rows += 1;
            }
        }

        assert_eq!(visited_rows, expected_rows.len());
    }

    fn build_scan_key(c1: &str, c2: i64) -> Bytes {
        let mut buf = ByteVec::new();
        let encoder = MemComparable;
        encoder.encode(&mut buf, &Datum::from(c1)).unwrap();
        encoder.encode(&mut buf, &Datum::from(c2)).unwrap();

        Bytes::from(buf)
    }

    pub fn build_row_for_two_column(key1: &[u8], key2: i64) -> Row {
        let datums = vec![
            Datum::Varbinary(Bytes::copy_from_slice(key1)),
            Datum::Timestamp(Timestamp::new(key2)),
        ];

        Row::from_datums(datums)
    }
}
