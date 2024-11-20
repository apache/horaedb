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
    array::{AsArray, RecordBatch},
    compute::concat_batches,
    datatypes::{
        GenericBinaryType, Int32Type, Int64Type, Int8Type, Schema, UInt32Type, UInt64Type,
        UInt8Type,
    },
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
use futures::{Stream, StreamExt};
use itertools::Itertools;
use parquet::arrow::async_reader::ParquetObjectReader;

use crate::{
    compare_primitive_columns,
    types::{ObjectStoreRef, SEQ_COLUMN_NAME},
};

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
pub(crate) struct MergeExec {
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
    pub fn new(
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
    arrow_schema: SchemaRef,
}

impl MergeStream {
    fn new(
        stream: SendableRecordBatchStream,
        num_primary_keys: usize,
        seq_idx: usize,
        value_idx: usize,
        value_op: Arc<dyn AggregateUDFImpl>,
    ) -> Self {
        let fields = stream
            .schema()
            .fields()
            .into_iter()
            .filter_map(|f| {
                if f.name() == SEQ_COLUMN_NAME {
                    None
                } else {
                    Some(f.clone())
                }
            })
            .collect_vec();
        let arrow_schema = Arc::new(Schema::new_with_metadata(
            fields,
            stream.schema().metadata.clone(),
        ));
        Self {
            stream,
            num_primary_keys,
            seq_idx,
            value_idx,
            value_op,
            pending_batch: None,
            arrow_schema,
        }
    }

    fn primary_key_eq(
        &self,
        lhs: &RecordBatch,
        lhs_idx: usize,
        rhs: &RecordBatch,
        rhs_idx: usize,
    ) -> bool {
        for k in 0..self.num_primary_keys {
            let lhs_col = lhs.column(k);
            let rhs_col = rhs.column(k);
            compare_primitive_columns!(
                lhs_col, rhs_col, lhs_idx, rhs_idx, // TODO: Add more types here
                UInt8Type, Int8Type, UInt32Type, Int32Type, UInt64Type, Int64Type
            );

            if let Some(lhs_col) = lhs_col.as_bytes_opt::<GenericBinaryType<i32>>() {
                let rhs_col = rhs_col.as_bytes::<GenericBinaryType<i32>>();
                if !rhs_col.value(rhs_idx).eq(lhs_col.value(lhs_idx)) {
                    return false;
                }
            }
        }

        true
    }

    // TODO: only support deduplication now, merge operation will be added later.
    fn merge_batch(&mut self, batch: RecordBatch) -> DfResult<RecordBatch> {
        let mut batches = vec![];
        let mut start_idx = 0;
        while start_idx < batch.num_rows() {
            let mut end_idx = start_idx + 1;
            while end_idx < batch.num_rows()
                && self.primary_key_eq(&batch, start_idx, &batch, end_idx)
            {
                end_idx += 1;
            }
            let rows_with_same_primary_keys = batch.slice(start_idx, end_idx - start_idx);
            if let Some(pending) = self.pending_batch.take() {
                if !self.primary_key_eq(
                    &pending,
                    pending.num_rows() - 1,
                    &rows_with_same_primary_keys,
                    0,
                ) {
                    // only keep the last row in this batch
                    batches.push(pending.slice(pending.num_rows() - 1, 1));
                }
            }
            batches.push(
                rows_with_same_primary_keys.slice(rows_with_same_primary_keys.num_rows() - 1, 1),
            );

            start_idx = end_idx;
        }

        // last batch may have overlapping rows with the next batch, so keep them in
        // pending_batch
        self.pending_batch = batches.pop();

        concat_batches(&self.stream.schema(), batches.iter())
            .map_err(|e| DataFusionError::ArrowError(e, None))
            .map(|mut batch| {
                // Remove seq column
                batch.remove_column(self.seq_idx);
                batch
            })
    }
}

impl Stream for MergeStream {
    type Item = DfResult<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, ctx: &mut Context) -> Poll<Option<Self::Item>> {
        match self.stream.poll_next_unpin(ctx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(None) => {
                let value = if let Some(mut pending) = self.pending_batch.take() {
                    pending.remove_column(self.seq_idx);
                    Some(Ok(pending))
                } else {
                    None
                };
                Poll::Ready(value)
            }
            Poll::Ready(Some(v)) => Poll::Ready(Some(v.and_then(|batch| {
                let batch = self.merge_batch(batch)?;
                Ok(batch)
            }))),
        }
    }
}

impl RecordBatchStream for MergeStream {
    fn schema(&self) -> SchemaRef {
        self.arrow_schema.clone()
    }
}
