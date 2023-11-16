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

use std::{
    cmp::Ordering,
    collections::HashMap,
    ops::Bound,
    sync::{Arc, RwLock},
    time::Instant,
};

use arena::{Arena, BasicStats, MonoIncArena};
use bytes_ext::{ByteVec, Bytes};
use codec::{memcomparable::MemComparable, row, Encoder};
use common_types::{
    column::Column,
    column_schema::ColumnId,
    datum::Datum,
    projected_schema::{RecordFetchingContext},
    record_batch::{FetchingRecordBatch, FetchingRecordBatchBuilder},
    row::Row,
    schema::Schema,
    SequenceNumber,
};
use generic_error::BoxError;
use logger::trace;
use parquet::data_type::AsBytes;
use skiplist::{ArenaSlice, BytewiseComparator, IterRef, Skiplist};
use snafu::{OptionExt, ResultExt};

use crate::memtable::{
    key,
    key::{KeySequence, SequenceCodec},
    AppendRow, BuildRecordBatch, DecodeInternalKey, Internal, InternalNoCause, IterTimeout,
    ProjectSchema, Result, ScanContext, ScanRequest,
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

/// Columnar iterator for [ColumnarMemTable]
pub struct ColumnarIterImpl<A: Arena<Stats = BasicStats> + Clone + Sync + Send> {
    memtable: Arc<RwLock<HashMap<ColumnId, Column>>>,
    row_num: usize,
    current_idx: usize,
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
    /// The last sequence of the memtable.
    last_sequence: SequenceNumber,

    /// State of iterator
    state: State,

    /// Dedup rows with key
    need_dedup: bool,

    skiplist: Skiplist<BytewiseComparator, A>,
    /// The internal skiplist iter
    iter: IterRef<Skiplist<BytewiseComparator, A>, BytewiseComparator, A>,
    last_internal_key: Option<ArenaSlice<A>>,
}

impl<A: Arena<Stats = BasicStats> + Clone + Sync + Send> ColumnarIterImpl<A> {
    /// Create a new [ColumnarIterImpl].
    pub fn new(
        memtable: Arc<RwLock<HashMap<ColumnId, Column>>>,
        row_num: usize,
        schema: Schema,
        ctx: ScanContext,
        request: ScanRequest,
        last_sequence: SequenceNumber,
        skiplist: Skiplist<BytewiseComparator, A>,
    ) -> Result<Self> {
        let record_fetching_ctx = request
            .record_fetching_ctx_builder
            .build(&schema)
            .context(ProjectSchema)?;
        let mut columnar_iter = Self {
            memtable,
            row_num,
            current_idx: 0,
            memtable_schema: schema,
            record_fetching_ctx,
            batch_size: ctx.batch_size,
            deadline: ctx.deadline,
            start_user_key: request.start_user_key,
            end_user_key: request.end_user_key,
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
    /// `start_user_key`, but we still need to check `end_user_key`.
    fn init(&mut self) -> Result<()> {
        self.current_idx = 0;
        self.state = State::Initialized;
        // If need_dedup is true, we need to build the skiplist to dedup.
        if self.need_dedup {
            // TODO: remove the lock or else it will block write.
            let memtable = self.memtable.read().unwrap();
            let mut key_vec = vec![ByteVec::new(); self.row_num];
            let encoder = MemComparable;

            for idx in self.memtable_schema.primary_key_indexes() {
                let column_schema = self.memtable_schema.column(*idx);
                let column = memtable
                    .get(&column_schema.id)
                    .with_context(|| InternalNoCause {
                        msg: format!("column not found, column:{}", column_schema.name),
                    })?;
                for (i, key) in key_vec.iter_mut().enumerate().take(self.row_num) {
                    let datum = column.get_datum(i);
                    encoder
                        .encode(key, &datum)
                        .box_err()
                        .context(Internal { msg: "encode key" })?;
                }
            }

            // TODO: Persist the skiplist.
            for (i, mut key) in key_vec.into_iter().enumerate() {
                SequenceCodec
                    .encode(&mut key, &KeySequence::new(self.last_sequence, i as u32))
                    .box_err()
                    .context(Internal {
                        msg: "encode key sequence",
                    })?;
                self.skiplist.put(&key, (i as u32).to_le_bytes().as_slice());
            }

            match &self.start_user_key {
                Bound::Included(user_key) => {
                    // Seek the skiplist
                    self.iter.seek(user_key.as_bytes());
                }
                Bound::Excluded(user_key) => {
                    // Construct seek key, just seek to the key with next prefix, so there is no
                    // need to skip the key until we meet the first key >
                    // start_user_key
                    let seek_key = row::key_prefix_next(user_key);

                    // Seek the skiplist
                    self.iter.seek(seek_key.as_ref());
                }
                Bound::Unbounded => self.iter.seek_to_first(),
            }
        }
        Ok(())
    }

    /// Fetch next record batch
    fn fetch_next_record_batch(&mut self) -> Result<Option<FetchingRecordBatch>> {
        debug_assert_eq!(State::Initialized, self.state);
        assert!(self.batch_size > 0);
        let rows = if !self.need_dedup {
            self.fetch_next_record_batch_rows_no_dedup()?
        } else {
            self.fetch_next_record_batch_rows()?
        };

        if !rows.is_empty() {
            if let Some(deadline) = self.deadline {
                let now = Instant::now();
                if now >= deadline {
                    return IterTimeout { now, deadline }.fail();
                }
            }

            let fetching_schema = self.record_fetching_ctx.fetching_schema().clone();
            let primary_key_indexes = self
                .record_fetching_ctx
                .primary_key_indexes()
                .map(|idxs| idxs.to_vec());
            let mut builder = FetchingRecordBatchBuilder::with_capacity(
                fetching_schema,
                primary_key_indexes,
                self.batch_size,
            );
            for row in rows.into_iter() {
                builder.append_row(row).context(AppendRow)?;
            }

            let batch = builder.build().context(BuildRecordBatch)?;
            trace!("column iterator send one batch:{:?}", batch);
            Ok(Some(batch))
        } else {
            // If iter is invalid after seek (nothing matched), then it may not be marked as
            // finished yet.
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
            let (user_key, _) = key::user_key_from_internal_key(key).context(DecodeInternalKey)?;

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

    fn fetch_next_record_batch_rows(&mut self) -> Result<Vec<Row>> {
        let mut num_rows = 0;
        let mut row_idxs = Vec::with_capacity(self.batch_size);
        while self.iter.valid() && num_rows < self.batch_size {
            if let Some(row) = self.fetch_next_row()? {
                let mut buf = [0u8; 4];
                buf.copy_from_slice(&row);
                let idx = u32::from_le_bytes(buf);
                row_idxs.push(idx);
                num_rows += 1;
            } else {
                // There is no more row to fetch.
                self.finish();
                break;
            }
        }

        let memtable = self.memtable.read().unwrap();
        let mut rows = vec![
            Row::from_datums(vec![Datum::Null; self.memtable_schema.num_columns()]);
            self.batch_size
        ];
        for (col_idx, column_schema_idx) in self
            .record_fetching_ctx
            .fetching_source_column_indexes()
            .iter()
            .enumerate()
        {
            if let Some(column_schema_idx) = column_schema_idx {
                let column_schema = self.memtable_schema.column(*column_schema_idx);
                if let Some(column) = memtable.get(&column_schema.id) {
                    for (i, row_idx) in row_idxs.iter().enumerate() {
                        let datum = column.get_datum(*row_idx as usize);
                        rows[i][col_idx] = datum;
                    }
                }
            }
        }
        rows.resize(num_rows, Row::from_datums(vec![]));
        Ok(rows)
    }

    /// Fetch next record batch
    fn fetch_next_record_batch_rows_no_dedup(&mut self) -> Result<Vec<Row>> {
        let mut num_rows = 0;
        let memtable = self.memtable.read().unwrap();

        let record_schema = self.record_fetching_ctx.fetching_schema();
        let mut rows =
            vec![Row::from_datums(vec![Datum::Null; record_schema.num_columns()]); self.batch_size];

        for (col_idx, column_schema_idx) in self
            .record_fetching_ctx
            .fetching_source_column_indexes()
            .iter()
            .enumerate()
        {
            if let Some(column_schema_idx) = column_schema_idx {
                let column_schema = self.memtable_schema.column(*column_schema_idx);
                if let Some(column) = memtable.get(&column_schema.id) {
                    for (i, row) in rows.iter_mut().enumerate().take(self.batch_size) {
                        let row_idx = self.current_idx + i;
                        if row_idx >= column.len() {
                            break;
                        }
                        if col_idx == 0 {
                            num_rows += 1;
                        }
                        let datum = column.get_datum(row_idx);
                        row[col_idx] = datum;
                    }
                }
            }
        }
        rows.resize(num_rows, Row::from_datums(vec![]));
        self.current_idx += num_rows;
        Ok(rows)
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
    type Item = Result<FetchingRecordBatch>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.state != State::Initialized {
            return None;
        }

        self.fetch_next_record_batch().transpose()
    }
}
