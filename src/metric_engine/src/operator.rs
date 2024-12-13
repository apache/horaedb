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

use std::{fmt::Debug, sync::Arc};

use anyhow::Context;
use arrow::{
    array::{Array, BinaryArray, RecordBatch},
    buffer::OffsetBuffer,
};
use arrow_schema::DataType;
use tracing::debug;

use crate::{ensure, Result};

pub trait MergeOperator: Send + Sync + Debug {
    fn merge(&self, batch: RecordBatch) -> Result<RecordBatch>;
}

pub type MergeOperatorRef = Arc<dyn MergeOperator>;

#[derive(Debug)]
pub struct LastValueOperator;

impl MergeOperator for LastValueOperator {
    fn merge(&self, batch: RecordBatch) -> Result<RecordBatch> {
        let last_row = batch.slice(batch.num_rows() - 1, 1);
        Ok(last_row)
    }
}

#[derive(Debug)]
pub struct BytesMergeOperator {
    /// Column index of the column need to append together
    /// The column type must be `Binary`.
    value_idxes: Vec<usize>,
}

impl BytesMergeOperator {
    pub fn new(value_idxes: Vec<usize>) -> Self {
        Self { value_idxes }
    }
}

impl MergeOperator for BytesMergeOperator {
    fn merge(&self, batch: RecordBatch) -> Result<RecordBatch> {
        assert!(batch.num_rows() > 0);

        for idx in &self.value_idxes {
            let data_type = batch.column(*idx).data_type();
            ensure!(
                data_type == &DataType::Binary,
                "MergeOperator is only used for binary column, current:{data_type}"
            );
        }
        debug!(batch = ?batch, "BytesMergeOperator merge");

        let schema = batch.schema();
        let columns = batch
            .columns()
            .iter()
            .enumerate()
            .map(|(idx, column)| {
                if self.value_idxes.contains(&idx) {
                    // For value column, we append all elements
                    let binary_array = column.as_any().downcast_ref::<BinaryArray>().unwrap();
                    if binary_array.is_empty() {
                       return column.clone();
                    }

                    let offsets = binary_array.offsets();
                    let start = offsets[0] as usize;
                    let length = offsets[offsets.len()-1] as usize - start;
                    if length == 0 {
                       return column.clone();
                    }

                    // bytes buffer is cheap for clone.
                    let byte_buffer = binary_array.values().slice_with_length(start,length). clone();
                    debug!(byte_buffer = ?byte_buffer, offset = ?offsets, "BytesMergeOperator merge");
                    let offsets = OffsetBuffer::from_lengths([byte_buffer.len()]);
                    let concated_column = BinaryArray::new(offsets, byte_buffer, None);
                    Arc::new(concated_column)
                } else {
                    // For other columns, we just take the first element since the primary key
                    // columns are the same.
                    column.slice(0, 1)
                }
            })
            .collect();

        let merged_batch = RecordBatch::try_new(schema, columns)
            .context("failed to construct RecordBatch in BytesMergeOperator.")?;

        Ok(merged_batch)
    }
}

#[cfg(test)]
mod tests {

    use super::*;
    use crate::record_batch;

    #[test]
    fn test_last_value_operator() {
        let operator = LastValueOperator;
        let batch = record_batch!(
            ("pk1", UInt8, vec![11, 11, 11, 11]),
            ("pk2", UInt8, vec![100, 100, 100, 100]),
            ("value", Int64, vec![2, 7, 4, 1])
        )
        .unwrap();

        let actual = operator.merge(batch).unwrap();
        let expected = record_batch!(
            ("pk1", UInt8, vec![11]),
            ("pk2", UInt8, vec![100]),
            ("value", Int64, vec![1])
        )
        .unwrap();
        assert_eq!(actual, expected);
    }

    #[test]
    fn test_bytes_merge_operator() {
        let operator = BytesMergeOperator::new(vec![2]);

        let batch = record_batch!(
            ("pk1", UInt8, vec![11, 11, 11, 11]),
            ("pk2", UInt8, vec![100, 100, 100, 100]),
            ("value", Binary, vec![b"one", b"two", b"three", b"four"])
        )
        .unwrap();

        let actual = operator.merge(batch).unwrap();
        let expected = record_batch!(
            ("pk1", UInt8, vec![11]),
            ("pk2", UInt8, vec![100]),
            ("value", Binary, vec![b"onetwothreefour"])
        )
        .unwrap();

        assert_eq!(actual, expected);
    }
}
