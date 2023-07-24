// Copyright 2022-2023 CeresDB Project Authors. Licensed under Apache-2.0.

use std::{
    cmp::Ordering,
    collections::HashMap,
    ops::Bound,
    sync::{Arc, RwLock},
    time::Instant,
};
use std::iter::Rev;

use arena::{Arena, BasicStats, MonoIncArena};
use ceresdbproto::storage::value;
use common_types::{
    bytes::{ByteVec, Bytes},
    column::Column,
    datum::{Datum, DatumKind},
    projected_schema::{ProjectedSchema, RowProjector},
    record_batch::{RecordBatchWithKey, RecordBatchWithKeyBuilder},
    row::Row,
    schema::Schema,
    time::Timestamp,
    SequenceNumber,
};
use common_util::{
    codec::{memcomparable::MemComparable, Encoder},
    time::InstantExt,
};
use log::{error, trace};
use parquet::data_type::AsBytes;
use skiplist::{ArenaSlice, IterRef, Skiplist};
use snafu::ResultExt;
use common_util::error::BoxError;

use crate::memtable::{columnar::factory::BytewiseComparator, factory::Options, key, key::{ComparableInternalKey, Error::EncodeKeyDatum, KeySequence, SequenceCodec}, AppendRow, BuildRecordBatch, DecodeInternalKey, IterTimeout, ProjectSchema, Result, ScanContext, ScanRequest, IterReverse};

/// Iterator state
#[derive(Debug, PartialEq)]
enum State {
    /// The iterator struct is created but not initialized
    Uninitialized,
    /// The iterator is initialized (seek)
    Initialized,
    /// No more element the iterator can return
    Finished,
}

/// Columnar iterator for [ColumnarMemTable]
pub struct ColumnarIterImpl<A: Arena<Stats = BasicStats> + Clone + Sync + Send> {
    memtable: Arc<RwLock<HashMap<String, Column>>>,
    current_idx: usize,
    // Schema related:
    /// Schema of this memtable, used to decode row
    memtable_schema: Schema,
    /// Projection of schema to read
    projected_schema: ProjectedSchema,
    projector: RowProjector,

    // Options related:
    batch_size: usize,
    deadline: Option<Instant>,

    start_user_key: Bound<Bytes>,
    end_user_key: Bound<Bytes>,
    last_sequence: SequenceNumber,
    /// Max visible sequence
    sequence: SequenceNumber,
    /// State of iterator
    state: State,

    /// Dedup rows with key
    need_dedup: bool,

    skiplist: Skiplist<BytewiseComparator, A>,
    /// The internal skiplist iter
    iter: IterRef<Skiplist<BytewiseComparator, A>, BytewiseComparator, A>,
    last_internal_key: Option<ArenaSlice<A>>,
}

impl ColumnarIterImpl<MonoIncArena> {
    /// Create a new [ColumnarIterImpl]
    pub fn new(
        memtable: Arc<RwLock<HashMap<String, Column>>>,
        schema: Schema,
        opts: Options,
        ctx: ScanContext,
        request: ScanRequest,
        last_sequence: SequenceNumber,
    ) -> Result<Self> {
        let projector = request
            .projected_schema
            .try_project_with_key(&schema)
            .context(ProjectSchema)?;
        let arena =
            MonoIncArena::with_collector(opts.arena_block_size as usize, opts.collector.clone());
        let skiplist = Skiplist::with_arena(BytewiseComparator, arena);
        let mut columnar_iter = Self {
            memtable,
            current_idx: 0,
            memtable_schema: schema,
            projected_schema: request.projected_schema,
            projector,
            batch_size: ctx.batch_size,
            deadline: ctx.deadline,
            start_user_key: request.start_user_key,
            end_user_key: request.end_user_key,
            sequence: request.sequence,
            state: State::Uninitialized,
            need_dedup: request.need_dedup,
            iter: skiplist.iter(),
            skiplist,
            last_internal_key: None,
            last_sequence,
        };

        columnar_iter.init()?;

        Ok(columnar_iter)
    }

    /// Init the iterator, will seek to the proper position for first next()
    /// call, so the first entry next() returned is after the
    /// `start_user_key`, but we still need to check `end_user_key`
    fn init(&mut self) -> Result<()> {
        self.current_idx = 0;
        self.state = State::Initialized;

        if self.need_dedup {
            let memtable = self.memtable.read().unwrap();
            let mut row_num = 0;
            for (_, column) in &*memtable {
                row_num = column.len();
                break;
            }
            let mut key_vec = vec![ByteVec::new(); row_num];
            let encoder = MemComparable;

            // Reserve capacity for key
            // internal_key.reserve(key_encoder.estimate_encoded_size(row));
            for idx in self.memtable_schema.primary_key_indexes() {
                let column_schema = self.memtable_schema.column(*idx);
                let column = memtable.get(&column_schema.name).unwrap();
                for i in 0..row_num {
                    let datum = column.get_datum(i);
                    encoder.encode(&mut key_vec[i], &datum).unwrap();
                }
            }

            for (i, mut key) in key_vec.into_iter().enumerate() {
                // SequenceCodec.encode(buf, &self.sequence).unwrap();
                SequenceCodec
                    .encode(&mut key, &KeySequence::new(self.last_sequence, i as u32))
                    .unwrap();
                println!("key: {:?}", key);
                self.skiplist.put(&key, (i as u32).to_le_bytes().as_slice());
            }
            self.iter.seek_to_first();
        }
        println!("init");
        Ok(())
    }

    /// Fetch next record batch
    fn fetch_next_record_batch(&mut self) -> Result<Option<RecordBatchWithKey>> {
        debug_assert_eq!(State::Initialized, self.state);
        assert!(self.batch_size > 0);
        if !self.need_dedup {
            return self.fetch_next_record_batch2();
        }

        let mut builder = RecordBatchWithKeyBuilder::with_capacity(
            self.projected_schema.to_record_schema_with_key(),
            self.batch_size,
        );
        let mut num_rows = 0;
        let mut row_idxs = Vec::with_capacity(self.batch_size);
        while self.iter.valid() && num_rows < self.batch_size {
            if let Some(row) = self.fetch_next_row()? {
                let u32_vec = [row[0], row[1], row[2], row[3]];
                let idx = u32::from_le_bytes(u32_vec);
                row_idxs.push(idx);
                num_rows += 1;
            } else {
                // There is no more row to fetch
                self.finish();
                break;
            }
        }

        let rows = {
            let memtable = self.memtable.read().unwrap();
            let mut rows = vec![
                Row::from_datums(vec![Datum::Null; self.memtable_schema.num_columns()]);
                self.batch_size
            ];
            for (col_idx, column_schema_idx) in self.projector.source_projection().iter().enumerate() {
                if let Some(column_schema_idx)= column_schema_idx {
                    let column_schema = self.memtable_schema.column(*column_schema_idx);
                    if let Some(column) = memtable.get(&column_schema.name) {
                        for (i, row_idx) in row_idxs.iter().enumerate() {
                            let datum = column.get_datum(*row_idx as usize);
                            rows[i].cols[col_idx] = datum;
                        }
                    }
                }
                // if let Some(column) = memtable.get(&column_schema.name) {
                //     for (i, row_idx) in row_idxs.iter().enumerate() {
                //         let datum = column.get_datum(*row_idx as usize);
                //         rows[i].cols[col_idx] = datum;
                //     }
                // }
            }
            rows
        };

        if num_rows > 0 {
            if let Some(deadline) = self.deadline {
                if deadline.check_deadline() {
                    return IterTimeout {}.fail();
                }
            }

            let mut builder = RecordBatchWithKeyBuilder::with_capacity(
                self.projected_schema.to_record_schema_with_key(),
                self.batch_size,
            );
            for (i, row) in rows.into_iter().enumerate() {
                if i >= num_rows {
                    break;
                }
                builder.append_row(row).context(AppendRow)?;
            }

            let batch = builder.build().context(BuildRecordBatch)?;
            trace!("column iterator send one batch:{:?}", batch);
            println!("column iterator send one batch:{:?}", batch);
            Ok(Some(batch))
        } else {
            // If iter is invalid after seek (nothing matched), then it may not be marked as
            // finished yet
            println!("column iterator send none");
            self.finish();
            Ok(None)
        }
    }

    /// Fetch next row matched the given condition, the current entry of iter
    /// will be considered
    ///
    /// REQUIRE: The iter is valid
    fn fetch_next_row(&mut self) -> Result<Option<ArenaSlice<MonoIncArena>>> {
        debug_assert_eq!(State::Initialized, self.state);

        // TODO(yingwen): Some operation like delete needs to be considered during
        // iterating: we need to ignore this key if found a delete mark
        while self.iter.valid() {
            println!("iter valid");
            // Fetch current entry
            let key = self.iter.key();
            let (user_key, sequence) =
                key::user_key_from_internal_key(key).context(DecodeInternalKey)?;

            // Check user key is still in range
            if self.is_after_end_bound(user_key) {
                println!("user_key:{:?} is after end bound", user_key);
                // Out of bound
                self.finish();
                return Ok(None);
            }

            if self.need_dedup {
                println!("need dedup");
                // Whether this user key is already returned
                let same_key = match &self.last_internal_key {
                    Some(last_internal_key) => {
                        // TODO(yingwen): Actually this call wont fail, only valid internal key will
                        // be set as last_internal_key so maybe we can just
                        // unwrap it?
                        let (last_user_key, _) = key::user_key_from_internal_key(last_internal_key)
                            .context(DecodeInternalKey)?;
                        user_key == last_user_key
                    }
                    // This is the first user key
                    None => false,
                };

                if same_key {
                    println!("skip duplicate key: {:?}", user_key);
                    // We meet duplicate key, move forward and continue to find next user key
                    self.iter.next();
                    continue;
                }
                // Now this is a new user key
            }

            // // Check whether this key is visible
            // if !self.is_visible(sequence) {
            //     println!("skip invisible key: {:?}", user_key);
            //     // The sequence of this key is not visible, move forward
            //     self.iter.next();
            //     continue;
            // }

            // This is the row we want
            let row = self.iter.value_with_arena();

            // Store the last key
            self.last_internal_key = Some(self.iter.key_with_arena());
            // Move iter forward
            self.iter.next();

            return Ok(Some(row));
        }

        // No more row in range, we can stop the iterator
        self.finish();
        Ok(None)
    }

    /// Fetch next record batch
    fn fetch_next_record_batch2(&mut self) -> Result<Option<RecordBatchWithKey>> {
        debug_assert_eq!(State::Initialized, self.state);
        assert!(self.batch_size > 0);

        let mut row_counter = 0;
        let rows = {
            let memtable = self.memtable.read().unwrap();

            let record_schema = self.projected_schema.to_record_schema();
            let mut rows = vec![
                Row::from_datums(vec![Datum::Null; record_schema.num_columns()]);
                self.batch_size
            ];
            for (col_idx, column_schema) in record_schema.columns().iter().enumerate() {
                if let Some(column) = memtable.get(&column_schema.name) {
                    for i in 0..self.batch_size {
                        let row_idx = self.current_idx + i;
                        if row_idx >= column.len() {
                            break;
                        }
                        if col_idx == 0 {
                            row_counter += 1;
                        }
                        let datum = column.get_datum(row_idx);
                        rows[i].cols[col_idx] = datum;
                    }
                }
            }
            self.current_idx += row_counter;
            rows
        };

        if row_counter > 0 {
            if let Some(deadline) = self.deadline {
                if deadline.check_deadline() {
                    return IterTimeout {}.fail();
                }
            }
            let mut builder = RecordBatchWithKeyBuilder::with_capacity(
                self.projected_schema.to_record_schema_with_key(),
                self.batch_size,
            );
            for (i, row) in rows.into_iter().enumerate() {
                if i >= row_counter {
                    break;
                }
                builder.append_row(row).context(AppendRow)?;
            }

            let batch = builder.build().context(BuildRecordBatch)?;
            trace!("column iterator send one batch:{:?}", batch);

            Ok(Some(batch))
        } else {
            self.finish();
            Ok(None)
        }
    }

    /// Return true if the sequence is visible
    #[inline]
    fn is_visible(&self, sequence: KeySequence) -> bool {
        sequence.sequence() <= self.sequence
    }

    /// Return true if the key is after the `end_user_key` bound
    fn is_after_end_bound(&self, key: &[u8]) -> bool {
        match &self.end_user_key {
            Bound::Included(end) => match key.cmp(end) {
                Ordering::Less | Ordering::Equal => false,
                Ordering::Greater => true,
            },
            Bound::Excluded(end) => match key.cmp(end) {
                Ordering::Less => false,
                Ordering::Equal | Ordering::Greater => true,
            },
            // All key is valid
            Bound::Unbounded => false,
        }
    }

    /// Mark the iterator state to finished and return None
    fn finish(&mut self) {
        self.state = State::Finished;
    }
}

impl Iterator for ColumnarIterImpl<MonoIncArena> {
    type Item = Result<RecordBatchWithKey>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.state != State::Initialized {
            println!("state is not initialized");
            return None;
        }

        self.fetch_next_record_batch().transpose()
    }
}

/// Reversed columnar iterator.
// TODO(xikai): Now the implementation is not perfect: read all the entries
//  into a buffer and reverse read it. The memtable should support scan in
// reverse  order naturally.
pub struct ReversedColumnarIterator<I> {
    iter: I,
    reversed_iter: Option<Rev<std::vec::IntoIter<Result<RecordBatchWithKey>>>>,
    num_record_batch: usize,
}

impl<I> ReversedColumnarIterator<I>
    where
        I: Iterator<Item = Result<RecordBatchWithKey>>,
{
    pub fn new(iter: I, num_rows: usize, batch_size: usize) -> Self {
        Self {
            iter,
            reversed_iter: None,
            num_record_batch: num_rows / batch_size,
        }
    }

    fn init_if_necessary(&mut self) {
        if self.reversed_iter.is_some() {
            return;
        }

        let mut buf = Vec::with_capacity(self.num_record_batch);
        for item in &mut self.iter {
            buf.push(item);
        }
        self.reversed_iter = Some(buf.into_iter().rev());
    }
}

impl<I> Iterator for ReversedColumnarIterator<I>
    where
        I: Iterator<Item = Result<RecordBatchWithKey>>,
{
    type Item = Result<RecordBatchWithKey>;

    fn next(&mut self) -> Option<Self::Item> {
        self.init_if_necessary();
        self.reversed_iter
            .as_mut()
            .unwrap()
            .next()
            .map(|v| match v {
                Ok(mut batch_with_key) => {
                    batch_with_key
                        .reverse_data()
                        .box_err()
                        .context(IterReverse)?;

                    Ok(batch_with_key)
                }
                Err(e) => Err(e),
            })
    }
}
