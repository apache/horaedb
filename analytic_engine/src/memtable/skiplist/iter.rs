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

//! Skiplist memtable iterator

use std::{cmp::Ordering, ops::Bound, time::Instant};

use arena::{Arena, BasicStats};
use bytes_ext::{Bytes, BytesMut};
use codec::row;
use common_types::{
    projected_schema::{RecordFetchingContext},
    record_batch::{FetchingRecordBatch, FetchingRecordBatchBuilder},
    row::contiguous::{ContiguousRowReader, ProjectedContiguousRow},
    schema::Schema,
    SequenceNumber,
};
use logger::trace;
use skiplist::{ArenaSlice, BytewiseComparator, IterRef, Skiplist};
use snafu::ResultExt;

use crate::memtable::{
    key::{self, KeySequence},
    skiplist::SkiplistMemTable,
    AppendRow, BuildRecordBatch, DecodeContinuousRow, DecodeInternalKey, EncodeInternalKey,
    IterTimeout, ProjectSchema, Result, ScanContext, ScanRequest,
};

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

/// Columnar iterator for [SkiplistMemTable]
pub struct ColumnarIterImpl<A: Arena<Stats = BasicStats> + Clone + Sync + Send> {
    /// The internal skiplist iter
    iter: IterRef<Skiplist<BytewiseComparator, A>, BytewiseComparator, A>,

    // Schema related:
    /// Schema of this memtable, used to decode row
    memtable_schema: Schema,
    /// Projection of schema to read
    record_fetching_ctx: RecordFetchingContext,

    // Options related:
    batch_size: usize,
    deadline: Option<Instant>,

    start_user_key: Bound<Bytes>,
    end_user_key: Bound<Bytes>,
    /// Max visible sequence
    sequence: SequenceNumber,
    /// State of iterator
    state: State,
    /// Last internal key this iterator returned
    // TODO(yingwen): Wrap a internal key struct?
    last_internal_key: Option<ArenaSlice<A>>,

    /// Dedup rows with key
    need_dedup: bool,
}

impl<A: Arena<Stats = BasicStats> + Clone + Sync + Send> ColumnarIterImpl<A> {
    /// Create a new [ColumnarIterImpl]
    pub fn new(
        memtable: &SkiplistMemTable<A>,
        ctx: ScanContext,
        request: ScanRequest,
    ) -> Result<Self> {
        // Create projection for the memtable schema
        let record_fetching_ctx = request
            .record_fetching_ctx_builder
            .build(&memtable.schema)
            .context(ProjectSchema)?;

        let iter = memtable.skiplist.iter();
        let mut columnar_iter = Self {
            iter,
            memtable_schema: memtable.schema.clone(),
            record_fetching_ctx,
            batch_size: ctx.batch_size,
            deadline: ctx.deadline,
            start_user_key: request.start_user_key,
            end_user_key: request.end_user_key,
            sequence: request.sequence,
            state: State::Uninitialized,
            last_internal_key: None,
            need_dedup: request.need_dedup,
        };

        columnar_iter.init()?;

        Ok(columnar_iter)
    }

    /// Init the iterator, will seek to the proper position for first next()
    /// call, so the first entry next() returned is after the
    /// `start_user_key`, but we still need to check `end_user_key`
    fn init(&mut self) -> Result<()> {
        match &self.start_user_key {
            Bound::Included(user_key) => {
                // Construct seek key
                let mut key_buf = BytesMut::new();
                let seek_key = key::internal_key_for_seek(user_key, self.sequence, &mut key_buf)
                    .context(EncodeInternalKey)?;

                // Seek the skiplist
                self.iter.seek(seek_key);
            }
            Bound::Excluded(user_key) => {
                // Construct seek key, just seek to the key with next prefix, so there is no
                // need to skip the key until we meet the first key >
                // start_user_key
                let next_user_key = row::key_prefix_next(user_key);
                let mut key_buf = BytesMut::new();
                let seek_key =
                    key::internal_key_for_seek(&next_user_key, self.sequence, &mut key_buf)
                        .context(EncodeInternalKey)?;

                // Seek the skiplist
                self.iter.seek(seek_key);
            }
            Bound::Unbounded => self.iter.seek_to_first(),
        }

        self.state = State::Initialized;

        Ok(())
    }

    /// Fetch next record batch
    fn fetch_next_record_batch(&mut self) -> Result<Option<FetchingRecordBatch>> {
        debug_assert_eq!(State::Initialized, self.state);
        assert!(self.batch_size > 0);

        let record_schema = self.record_fetching_ctx.fetching_schema().clone();
        let primary_key_indexes = self
            .record_fetching_ctx
            .primary_key_indexes()
            .map(|idxs| idxs.to_vec());
        let mut builder = FetchingRecordBatchBuilder::with_capacity(
            record_schema,
            primary_key_indexes,
            self.batch_size,
        );
        let mut num_rows = 0;
        while self.iter.valid() && num_rows < self.batch_size {
            if let Some(row) = self.fetch_next_row()? {
                let row_reader = ContiguousRowReader::try_new(&row, &self.memtable_schema)
                    .context(DecodeContinuousRow)?;
                let projected_row =
                    ProjectedContiguousRow::new(row_reader, &self.record_fetching_ctx);

                trace!("Column iterator fetch next row, row:{:?}", projected_row);

                builder
                    .append_projected_contiguous_row(&projected_row)
                    .context(AppendRow)?;
                num_rows += 1;
            } else {
                // There is no more row to fetch
                self.finish();
                break;
            }
        }

        if num_rows > 0 {
            if let Some(deadline) = self.deadline {
                let now = Instant::now();
                if now >= deadline {
                    return IterTimeout { now, deadline }.fail();
                }
            }

            let batch = builder.build().context(BuildRecordBatch)?;
            trace!("column iterator send one batch:{:?}", batch);

            Ok(Some(batch))
        } else {
            // If iter is invalid after seek (nothing matched), then it may not be marked as
            // finished yet
            self.finish();
            Ok(None)
        }
    }

    /// Fetch next row matched the given condition, the current entry of iter
    /// will be considered
    ///
    /// REQUIRE: The iter is valid
    fn fetch_next_row(&mut self) -> Result<Option<ArenaSlice<A>>> {
        debug_assert_eq!(State::Initialized, self.state);

        // TODO(yingwen): Some operation like delete needs to be considered during
        // iterating: we need to ignore this key if found a delete mark
        while self.iter.valid() {
            // Fetch current entry
            let key = self.iter.key();
            let (user_key, sequence) =
                key::user_key_from_internal_key(key).context(DecodeInternalKey)?;

            // Check user key is still in range
            if self.is_after_end_bound(user_key) {
                // Out of bound
                self.finish();
                return Ok(None);
            }

            if self.need_dedup {
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
                    // We meet duplicate key, move forward and continue to find next user key
                    self.iter.next();
                    continue;
                }
                // Now this is a new user key
            }

            // Check whether this key is visible
            if !self.is_visible(sequence) {
                // The sequence of this key is not visible, move forward
                self.iter.next();
                continue;
            }

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

impl<A: Arena<Stats = BasicStats> + Clone + Sync + Send> Iterator for ColumnarIterImpl<A> {
    type Item = Result<FetchingRecordBatch>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.state != State::Initialized {
            return None;
        }

        self.fetch_next_record_batch().transpose()
    }
}

// TODO(yingwen): Test
