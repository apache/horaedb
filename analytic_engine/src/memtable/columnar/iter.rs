// Copyright 2022-2023 CeresDB Project Authors. Licensed under Apache-2.0.

use std::{
    collections::HashMap,
    ops::Bound,
    sync::{Arc, RwLock},
    time::Instant,
};

use ceresdbproto::storage::value;
use common_types::{
    bytes::Bytes,
    column::Column,
    datum::{Datum, DatumKind},
    projected_schema::{ProjectedSchema, RowProjector},
    record_batch::{RecordBatchWithKey, RecordBatchWithKeyBuilder},
    row::Row,
    schema::Schema,
    time::Timestamp,
    SequenceNumber,
};
use common_util::time::InstantExt;
use log::{error, trace};
use snafu::ResultExt;

use crate::memtable::{
    AppendRow, BuildRecordBatch, IterTimeout, ProjectSchema, Result, ScanContext, ScanRequest,
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
pub struct ColumnarIterImpl {
    memtable: Arc<RwLock<HashMap<String, Column>>>,
    current_idx: usize,
    // Schema related:
    /// Schema of this memtable, used to decode row
    memtable_schema: Schema,
    /// Projection of schema to read
    projected_schema: ProjectedSchema,

    // Options related:
    batch_size: usize,
    deadline: Option<Instant>,

    /// Max visible sequence
    sequence: SequenceNumber,
    /// State of iterator
    state: State,

    /// Dedup rows with key
    need_dedup: bool,
}

impl ColumnarIterImpl {
    /// Create a new [ColumnarIterImpl]
    pub fn new(
        memtable: Arc<RwLock<HashMap<String, Column>>>,
        schema: Schema,
        ctx: ScanContext,
        request: ScanRequest,
    ) -> Result<Self> {
        let mut columnar_iter = Self {
            memtable,
            current_idx: 0,
            memtable_schema: schema,
            projected_schema: request.projected_schema,
            batch_size: ctx.batch_size,
            deadline: ctx.deadline,
            sequence: request.sequence,
            state: State::Uninitialized,
            need_dedup: request.need_dedup,
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

        Ok(())
    }

    /// Fetch next record batch
    fn fetch_next_record_batch(&mut self) -> Result<Option<RecordBatchWithKey>> {
        debug_assert_eq!(State::Initialized, self.state);
        assert!(self.batch_size > 0);
        // println!("column schema:{:?}", self.projected_schema);
        let mut row_counter = 0;
        let rows = {
            let memtable = self.memtable.read().unwrap();

            // println!("columnar memtable:{:?}",memtable);

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
                        rows[i].cols[col_idx] = (datum);
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
            // println!(
            //     "column iterator send one row_counter:{}, rows:{:?}",
            //     row_counter, rows
            // );
            for (i, row) in rows.into_iter().enumerate() {
                if i >= row_counter {
                    break;
                }
                // println!("column iterator send one row:{:?}", row);
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

    /// Mark the iterator state to finished and return None
    fn finish(&mut self) {
        self.state = State::Finished;
    }
}

impl Iterator for ColumnarIterImpl {
    type Item = Result<RecordBatchWithKey>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.state != State::Initialized {
            return None;
        }

        self.fetch_next_record_batch().transpose()
    }
}
