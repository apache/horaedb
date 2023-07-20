// Copyright 2023 CeresDB Project Authors. Licensed under Apache-2.0.

use std::{
    collections::HashMap,
    sync::{
        atomic,
        atomic::{AtomicU64, AtomicUsize, Ordering},
        Arc, RwLock,
    },
};

use arena::{Arena, BasicStats, MonoIncArena};
use bytes::Bytes;
use common_types::{column::Column, row::RowGroupSplitter, schema::Schema, SequenceNumber};
use common_util::codec::Encoder;
use log::debug;
use skiplist::Skiplist;
use snafu::{ensure, ResultExt};

use crate::memtable::{
    columnar::{factory::BytewiseComparator, iter::ColumnarIterImpl},
    factory::Options,
    key::{ComparableInternalKey, KeySequence},
    ColumnarIterPtr, InvalidPutSequence, MemTable, PutContext, Result, ScanContext, ScanRequest,
};
use crate::memtable::columnar::iter::ReversedColumnarIterator;

pub mod factory;
pub mod iter;

pub struct ColumnarMemTable {
    /// Schema of this memtable, is immutable.
    schema: Schema,
    pub memtable: Arc<RwLock<HashMap<String, Column>>>,
    /// The last sequence of the rows in this memtable. Update to this field
    /// require external synchronization.
    last_sequence: AtomicU64,
    row_num: AtomicUsize,
    opts: Options,
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
        println!("ColumnarMemTable::put, row_group: {:?}", row_group);
        let row_count = row_group.split_idx.len();
        let mut columns = HashMap::with_capacity(schema.num_columns());
        let row_num = self.row_num.load(Ordering::Relaxed);
        for i in 0..row_count {
            let row = row_group.get(i).unwrap();

            // let key_encoder = ComparableInternalKey::new(sequence, schema);
            //
            // let internal_key = &mut ctx.key_buf;
            // // Reset key buffer
            // internal_key.clear();
            // // Reserve capacity for key
            // internal_key.reserve(key_encoder.estimate_encoded_size(row));
            // // Encode key
            // key_encoder.encode(internal_key, row).unwrap();
            // self.key_index
            //     .put(internal_key, (row_num + i).to_le_bytes().as_slice());

            for (i, column_schema) in schema.columns().iter().enumerate() {
                let column = if let Some(column) = columns.get_mut(&column_schema.name) {
                    column
                } else {
                    let column = Column::new(row_count, column_schema.data_type);
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
        self.row_num.fetch_add(row_count, Ordering::Acquire);
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
        let iter: ColumnarIterImpl<MonoIncArena> = ColumnarIterImpl::new(
            self.memtable.clone(),
            self.schema.clone(),
            self.opts.clone(),
            ctx,
            request,
            self.last_sequence.load(Ordering::Acquire),
        )?;
        if reverse {
            Ok(Box::new(ReversedColumnarIterator::new(
                iter, num_rows, batch_size,
            )))
        } else {
            Ok(Box::new(iter))
        }
        // Ok(Box::new(iter))
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
