// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use std::{
    any::Any,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use arrow::{
    array::{AsArray, BinaryArray, PrimitiveArray, RecordBatch},
    compute::concat_batches,
    datatypes::{GenericBinaryType, Int8Type, UInt64Type, UInt8Type},
};
use arrow_schema::SchemaRef;
use datafusion::{
    common::internal_err,
    datasource::physical_plan::{FileMeta, ParquetFileReaderFactory},
    error::{DataFusionError, Result as DfResult},
    execution::{RecordBatchStream, SendableRecordBatchStream, TaskContext},
    logical_expr::AggregateUDFImpl,
    parquet::arrow::async_reader::AsyncFileReader,
    physical_plan::{
        metrics::ExecutionPlanMetricsSet, DisplayAs, Distribution, ExecutionPlan, PlanProperties,
    },
};
use futures::{ready, Stream, StreamExt};
use parquet::arrow::async_reader::ParquetObjectReader;

use crate::types::ObjectStoreRef;

#[derive(Debug, Clone)]
pub struct DefaultParquetFileReaderFactory {
    object_store: ObjectStoreRef,
}

/// Returns a AsyncFileReader factory
impl DefaultParquetFileReaderFactory {
    pub fn new(object_store: ObjectStoreRef) -> Self {
        Self { object_store }
    }
}

impl ParquetFileReaderFactory for DefaultParquetFileReaderFactory {
    fn create_reader(
        &self,
        _partition_index: usize,
        file_meta: FileMeta,
        metadata_size_hint: Option<usize>,
        _metrics: &ExecutionPlanMetricsSet,
    ) -> DfResult<Box<dyn AsyncFileReader + Send>> {
        let object_store = self.object_store.clone();
        let mut reader = ParquetObjectReader::new(object_store, file_meta.object_meta);
        if let Some(size) = metadata_size_hint {
            reader = reader.with_footer_size_hint(size);
        }
        Ok(Box::new(reader))
    }
}

/// Execution plan for merge RecordBatch values, like Merge Operator in RocksDB.
///
/// Input record batches are sorted by the primary key columns and seq
/// column.
#[derive(Debug)]
struct MergeExec {
    /// Input plan
    input: Arc<dyn ExecutionPlan>,
    /// (0..num_primary_keys) are primary key columns
    num_primary_keys: usize,
    /// Sequence column index
    seq_idx: usize,
    // (idx, merge_op)
    value_idx: usize,
    value_op: Arc<dyn AggregateUDFImpl>,
}

impl MergeExec {
    fn new(
        input: Arc<dyn ExecutionPlan>,
        num_primary_keys: usize,
        seq_idx: usize,
        value_idx: usize,
        value_op: Arc<dyn AggregateUDFImpl>,
    ) -> Self {
        Self {
            input,
            num_primary_keys,
            seq_idx,
            value_idx,
            value_op,
        }
    }
}
impl DisplayAs for MergeExec {
    fn fmt_as(
        &self,
        _t: datafusion::physical_plan::DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        write!(
            f,
            "MergeExec: [primary_keys: {}, seq_idx: {}]",
            self.num_primary_keys, self.seq_idx
        )?;
        Ok(())
    }
}

impl ExecutionPlan for MergeExec {
    fn name(&self) -> &str {
        "MergeExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        self.input.properties()
    }

    fn required_input_distribution(&self) -> Vec<Distribution> {
        vec![Distribution::SinglePartition; self.children().len()]
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn maintains_input_order(&self) -> Vec<bool> {
        vec![true; self.children().len()]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(MergeExec::new(
            Arc::clone(&children[0]),
            self.num_primary_keys,
            self.seq_idx,
            self.value_idx,
            self.value_op.clone(),
        )))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DfResult<SendableRecordBatchStream> {
        if 0 != partition {
            return internal_err!("MergeExec invalid partition {partition}");
        }

        Ok(Box::pin(MergeStream::new(
            self.input.execute(partition, context)?,
            self.num_primary_keys,
            self.seq_idx,
            self.value_idx,
            self.value_op.clone(),
        )))
    }
}

struct MergeStream {
    stream: SendableRecordBatchStream,
    num_primary_keys: usize,
    seq_idx: usize,
    value_idx: usize,
    value_op: Arc<dyn AggregateUDFImpl>,

    pending_batch: Option<RecordBatch>,
}

impl MergeStream {
    fn new(
        stream: SendableRecordBatchStream,
        num_primary_keys: usize,
        seq_idx: usize,
        value_idx: usize,
        value_op: Arc<dyn AggregateUDFImpl>,
    ) -> Self {
        Self {
            stream,
            num_primary_keys,
            seq_idx,
            value_idx,
            value_op,
            pending_batch: None,
        }
    }

    fn primary_key_eq2(
        &self,
        lhs: &RecordBatch,
        lhs_idx: usize,
        rhs: &RecordBatch,
        rhs_idx: usize,
    ) -> bool {
        for k in 0..self.num_primary_keys {
            let lhs_col = lhs.column(k);
            let rhs_col = rhs.column(k);
            if let Some(lhs_col) = lhs_col.as_primitive_opt::<UInt8Type>() {
                let rhs_col = rhs_col.as_primitive::<UInt8Type>();
                if !lhs_col.value(lhs_idx).eq(&rhs_col.value(rhs_idx)) {
                    return false;
                }
            } else if let Some(lhs_col) = lhs_col.as_primitive_opt::<UInt64Type>() {
                let rhs_col = rhs_col.as_primitive::<UInt64Type>();
                if !lhs_col.value(lhs_idx).eq(&rhs_col.value(rhs_idx)) {
                    return false;
                }
            } else if let Some(lhs_col) = lhs_col.as_bytes_opt::<GenericBinaryType<i32>>() {
                let rhs_col = rhs_col.as_bytes::<GenericBinaryType<i32>>();
                if !rhs_col.value(rhs_idx).eq(lhs_col.value(lhs_idx)) {
                    return false;
                }
            } else {
                unreachable!("unsupported column type: {:?}", lhs_col.data_type())
            }
        }

        true
    }

    fn primary_key_eq(&self, batch: &RecordBatch, i: usize, j: usize) -> bool {
        self.primary_key_eq2(batch, i, batch, j)
    }

    // TODO: only support deduplication now, merge operation will be added later.
    fn merge_batch(&mut self, batch: RecordBatch) -> DfResult<RecordBatch> {
        let mut row_idx = 0;
        let mut batches = vec![];
        while row_idx < batch.num_rows() {
            let mut cursor = row_idx + 1;
            while self.primary_key_eq(&batch, row_idx, cursor) {
                cursor += 1;
            }

            let same_pk_batch = batch.slice(row_idx, cursor - row_idx);
            if let Some(pending) = self.pending_batch.take() {
                if !self.primary_key_eq2(&pending, pending.num_rows() - 1, &same_pk_batch, 0) {
                    // only keep the last row in this batch
                    batches.push(pending.slice(pending.num_rows() - 1, 1));
                }
            }
            batches.push(same_pk_batch.slice(same_pk_batch.num_rows() - 1, 1));

            row_idx = cursor;
        }
        self.pending_batch = batches.pop();

        concat_batches(&self.stream.schema(), batches.iter().map(|v| v))
            .map_err(|e| DataFusionError::ArrowError(e, None))
    }
}

impl Stream for MergeStream {
    type Item = DfResult<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, ctx: &mut Context) -> Poll<Option<Self::Item>> {
        Poll::Ready(ready!(self.stream.poll_next_unpin(ctx)).map(|r| {
            r.and_then(|batch| {
                let batch = self.merge_batch(batch)?;
                Ok(batch)
            })
        }))
    }
}

impl RecordBatchStream for MergeStream {
    fn schema(&self) -> SchemaRef {
        todo!()
    }
}
