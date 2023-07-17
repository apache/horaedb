// Copyright 2023 CeresDB Project Authors. Licensed under Apache-2.0.

use std::{
    collections::HashMap,
    sync::{atomic, atomic::AtomicU64, Arc, RwLock},
};

use bytes::Bytes;
use common_types::{
    column::Column,
    row::{Row, RowGroupSlicer, RowGroupSplitter},
    schema::Schema,
    SequenceNumber,
};
use log::{debug, trace};
use snafu::ensure;

use crate::memtable::{
    columnar::iter::ColumnarIterImpl,
    key::{ComparableInternalKey, KeySequence},
    ColumnarIterPtr, InvalidPutSequence, MemTable, PutContext, Result, ScanContext, ScanRequest,
};

pub mod factory;
pub mod iter;

pub struct ColumnarMemTable {
    /// Schema of this memtable, is immutable.
    schema: Schema,
    pub memtable: Arc<RwLock<HashMap<String, Column>>>,
    /// The last sequence of the rows in this memtable. Update to this field
    /// require external synchronization.
    last_sequence: AtomicU64,
}

impl MemTable for ColumnarMemTable {
    fn schema(&self) -> &Schema {
        &self.schema
    }

    fn min_key(&self) -> Option<Bytes> {
        Some(Bytes::from("0"))
    }

    fn max_key(&self) -> Option<Bytes> {
        Some(Bytes::from("9"))
    }

    // Now the caller is required to encode the row into the `value_buf` in
    // PutContext first.
    fn put(
        &self,
        ctx: &mut PutContext,
        sequence: KeySequence,
        row_group: &RowGroupSplitter,
        schema: &Schema,
    ) -> Result<()> {
        let row_count = row_group.split_idx.len();
        let mut columns = HashMap::with_capacity(schema.num_columns());
        for i in 0..row_count {
            let row = row_group.get(i).unwrap();
            for (i, column_schema) in schema.columns().iter().enumerate() {
                let column = if let Some(column) = columns.get_mut(&column_schema.name) {
                    column
                } else {
                    let mut column = Column::new(row_count, column_schema.data_type);
                    columns.insert(column_schema.name.to_string(), column);
                    columns.get_mut(&column_schema.name).unwrap()
                };

                if let Some(writer_index) = ctx.index_in_writer.column_index_in_writer(i) {
                    column.append_datum_ref(&row[writer_index]).unwrap();
                } else {
                    column.append_nulls(1);
                }
            }
        }

        let mut memtable = self.memtable.write().unwrap();
        for (k, v) in columns {
            if let Some(column) = memtable.get_mut(&k) {
                column.append_column(v);
            } else {
                memtable.insert(k, v);
            };
        }

        Ok(())
    }

    fn scan(&self, ctx: ScanContext, request: ScanRequest) -> Result<ColumnarIterPtr> {
        debug!(
            "Scan columnar memtable, ctx:{:?}, request:{:?}",
            ctx, request
        );

        let timestamp_column = self.schema.column(self.schema.timestamp_index());
        let num_rows = self
            .memtable
            .read()
            .unwrap()
            .get(self.schema.timestamp_name())
            .unwrap()
            .len();
        let (reverse, batch_size) = (request.reverse, ctx.batch_size);
        let iter = ColumnarIterImpl::new(self.memtable.clone(), self.schema.clone(), ctx, request)?;
        Ok(Box::new(iter))
    }

    fn approximate_memory_usage(&self) -> usize {
        self.memtable
            .read()
            .unwrap()
            .iter()
            .map(|(_, column)| column.size())
            .sum()
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
}