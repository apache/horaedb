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
    collections::HashMap,
    sync::{
        atomic,
        atomic::{AtomicU64, AtomicUsize, Ordering},
        Arc, RwLock,
    },
};

use arena::MonoIncArena;
use bytes_ext::Bytes;
use common_types::{column::Column, datum::Datum, row::Row, schema::Schema, SequenceNumber};
use generic_error::BoxError;
use log::debug;
use skiplist::Skiplist;
use snafu::{ensure, OptionExt, ResultExt};

use crate::memtable::{
    columnar::iter::ColumnarIterImpl,
    factory::Options,
    iter::ReversedColumnarIterator,
    key::{BytewiseComparator, KeySequence},
    ColumnarIterPtr, Internal, InternalNoCause, InvalidPutSequence, MemTable, PutContext, Result,
    ScanContext, ScanRequest,
};

pub mod factory;
pub mod iter;

pub struct ColumnarMemTable {
    /// Schema of this memtable, is immutable.
    schema: Schema,
    memtable: Arc<RwLock<HashMap<String, Column>>>,
    /// The last sequence of the rows in this memtable. Update to this field
    /// require external synchronization.
    last_sequence: AtomicU64,
    row_num: AtomicUsize,
    opts: Options,
    memtable_size: AtomicUsize,
}

impl ColumnarMemTable {
    // TODO: Optimize memtable size calculation.
    fn memtable_size(&self) -> usize {
        self.memtable
            .read()
            .unwrap()
            .iter()
            .map(|(_, column)| column.size())
            .sum()
    }
}

impl MemTable for ColumnarMemTable {
    fn schema(&self) -> &Schema {
        &self.schema
    }

    fn min_key(&self) -> Option<Bytes> {
        // TODO: columnar memtable should support min_key and max_key
        Some(Bytes::from("0"))
    }

    fn max_key(&self) -> Option<Bytes> {
        // TODO: columnar memtable should support min_key and max_key
        Some(Bytes::from("9"))
    }

    // Now the caller is required to encode the row into the `value_buf` in
    // PutContext first.
    fn put(
        &self,
        ctx: &mut PutContext,
        _sequence: KeySequence,
        row: &Row,
        schema: &Schema,
    ) -> Result<()> {
        let mut columns = HashMap::with_capacity(schema.num_columns());

        for (i, column_schema) in schema.columns().iter().enumerate() {
            let column = if let Some(column) = columns.get_mut(&column_schema.name) {
                column
            } else {
                let column =
                    Column::new(1, column_schema.data_type)
                        .box_err()
                        .context(Internal {
                            msg: "new column failed",
                        })?;
                columns.insert(column_schema.name.to_string(), column);
                columns
                    .get_mut(&column_schema.name)
                    .context(InternalNoCause {
                        msg: "get column failed",
                    })?
            };

            if let Some(writer_index) = ctx.index_in_writer.column_index_in_writer(i) {
                let datum = &row[writer_index];
                if datum == &Datum::Null {
                    column.append_nulls(1);
                } else {
                    column
                        .append_datum_ref(&row[writer_index])
                        .box_err()
                        .context(Internal {
                            msg: "append datum failed",
                        })?
                }
            } else {
                column.append_nulls(1);
            }
        }
        {
            let mut memtable = self.memtable.write().unwrap();
            for (k, v) in columns {
                if let Some(column) = memtable.get_mut(&k) {
                    column.append_column(v).box_err().context(Internal {
                        msg: "append column",
                    })?;
                } else {
                    memtable.insert(k, v);
                };
            }
        }

        self.row_num.fetch_add(1, Ordering::Acquire);

        // May have performance issue.
        self.memtable_size
            .store(self.memtable_size(), Ordering::Relaxed);
        Ok(())
    }

    fn scan(&self, ctx: ScanContext, request: ScanRequest) -> Result<ColumnarIterPtr> {
        debug!(
            "Scan columnar memtable, ctx:{:?}, request:{:?}",
            ctx, request
        );

        let num_rows = self
            .memtable
            .read()
            .unwrap()
            .get(self.schema.timestamp_name())
            .context(InternalNoCause {
                msg: "get timestamp column failed",
            })?
            .len();
        let (reverse, batch_size) = (request.reverse, ctx.batch_size);
        let arena = MonoIncArena::with_collector(
            self.opts.arena_block_size as usize,
            self.opts.collector.clone(),
        );
        let skiplist = Skiplist::with_arena(BytewiseComparator, arena);
        let iter = ColumnarIterImpl::new(
            self.memtable.clone(),
            self.row_num.load(Ordering::Relaxed),
            self.schema.clone(),
            ctx,
            request,
            self.last_sequence.load(Ordering::Acquire),
            skiplist,
        )?;
        if reverse {
            Ok(Box::new(ReversedColumnarIterator::new(
                iter, num_rows, batch_size,
            )))
        } else {
            Ok(Box::new(iter))
        }
    }

    fn approximate_memory_usage(&self) -> usize {
        self.memtable_size.load(Ordering::Relaxed)
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

    fn metrics(&self) -> super::Metrics {
        todo!()
    }
}