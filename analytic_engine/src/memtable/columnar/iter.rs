// Copyright 2022-2023 CeresDB Project Authors. Licensed under Apache-2.0.

use std::{
    cmp::Ordering,
    collections::HashMap,
    iter::Rev,
    ops::Bound,
    sync::{Arc, RwLock},
    time::Instant,
};

use arena::{Arena, BasicStats};
use arrow::datatypes::SchemaRef;
use ceresdbproto::storage::value;
use common_types::{
    bytes::{Bytes, BytesMut},
    column::Column,
    datum::{Datum, DatumKind},
    projected_schema::{ProjectedSchema, RowProjector},
    record_batch::{RecordBatchWithKey, RecordBatchWithKeyBuilder},
    row::{
        contiguous::{ContiguousRowReader, ProjectedContiguousRow},
        Row,
    },
    schema::Schema,
    time::Timestamp,
    SequenceNumber,
};
use common_util::{codec::row, error::BoxError, time::InstantExt};
use log::{error, trace};
use snafu::ResultExt;

use crate::memtable::{
    columnar::ColumnarMemTable,
    key::{self, KeySequence},
    AppendRow, BuildRecordBatch, DecodeInternalKey, EncodeInternalKey, IterReverse, IterTimeout,
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
pub struct ColumnarIterImpl {
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
        // Create projection for the memtable schema
        let projector = request
            .projected_schema
            .try_project_with_key(&schema)
            .context(ProjectSchema)?;

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

        let memtable = self.memtable.read().unwrap();

        let mut row_count = 0;
        for (k, v) in &*memtable {
            row_count = v.len();
            break;
        }

        let mut rows = vec![
            Row::from_datums(Vec::with_capacity(self.memtable_schema.num_columns()));
            row_count
        ];
        let mut row_counter = 0;
        for (i, column_schema) in self.memtable_schema.columns().iter().enumerate() {
            let column = memtable.get(&column_schema.name).unwrap().clone();

            for (row_idx, col) in column.into_iter().enumerate() {
                let datum = convert_proto_value_to_datum(col, column_schema.data_type).unwrap();
                rows[row_idx].cols.push(datum);
            }
        }

        let mut builder = RecordBatchWithKeyBuilder::with_capacity(
            self.projected_schema.to_record_schema_with_key(),
            self.batch_size,
        );

        for row in rows {
            builder.append_row(row).context(AppendRow)?;
            row_counter += 1;
        }
        drop(memtable);
        self.finish();
        let batch = builder.build().context(BuildRecordBatch)?;

        Ok(Some(batch))
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

fn convert_proto_value_to_datum(value: value::Value, data_type: DatumKind) -> Result<Datum> {
    match (value, data_type) {
        (value::Value::Float64Value(v), DatumKind::Double) => Ok(Datum::Double(v)),
        (value::Value::StringValue(v), DatumKind::String) => Ok(Datum::String(v.into())),
        (value::Value::Int64Value(v), DatumKind::Int64) => Ok(Datum::Int64(v)),
        (value::Value::Int64Value(v), DatumKind::Timestamp) => {
            Ok(Datum::Timestamp(Timestamp::new(v)))
        }
        (value::Value::Float32Value(v), DatumKind::Float) => Ok(Datum::Float(v)),
        (value::Value::Int32Value(v), DatumKind::Int32) => Ok(Datum::Int32(v)),
        (value::Value::Int16Value(v), DatumKind::Int16) => Ok(Datum::Int16(v as i16)),
        (value::Value::Int8Value(v), DatumKind::Int8) => Ok(Datum::Int8(v as i8)),
        (value::Value::BoolValue(v), DatumKind::Boolean) => Ok(Datum::Boolean(v)),
        (value::Value::Uint64Value(v), DatumKind::UInt64) => Ok(Datum::UInt64(v)),
        (value::Value::Uint32Value(v), DatumKind::UInt32) => Ok(Datum::UInt32(v)),
        (value::Value::Uint16Value(v), DatumKind::UInt16) => Ok(Datum::UInt16(v as u16)),
        (value::Value::Uint8Value(v), DatumKind::UInt8) => Ok(Datum::UInt8(v as u8)),
        (value::Value::TimestampValue(v), DatumKind::Timestamp) => {
            Ok(Datum::Timestamp(Timestamp::new(v)))
        }
        (value::Value::VarbinaryValue(v), DatumKind::Varbinary) => {
            Ok(Datum::Varbinary(Bytes::from(v)))
        }
        (v, d) => {
            error!("Unexpected value type, value:{:?}, datum:{:?}", v, d);
            todo!();
        }
    }
}
