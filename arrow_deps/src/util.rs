// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

//! utilities for manipulating arrow/parquet/datafusion data structures.

use std::convert::TryFrom;

use arrow::{
    array::UInt32Array,
    compute,
    error::{ArrowError, Result},
    record_batch::RecordBatch,
};

/// Select the data in the [`RecordBatch`] by read and copy from the source
/// `batch`.
pub fn select_record_batch(batch: &RecordBatch, selected_rows: &[bool]) -> Result<RecordBatch> {
    assert_eq!(batch.num_rows(), selected_rows.len());
    let selected_columns = {
        // ensure the the selected_rows.len() is not greater than u32::MAX.
        let _ = u32::try_from(selected_rows.len()).map_err(|e| {
            ArrowError::InvalidArgumentError(format!(
                "too many rows in a batch, convert usize to u32 failed, num_rows:{}, err:{}",
                batch.num_rows(),
                e
            ))
        })?;

        let selected_index_iter = selected_rows
            .iter()
            .enumerate()
            .filter_map(|(idx, selected)| if *selected { Some(idx as u32) } else { None });
        // TODO(xikai): avoid this memory allocation.
        let indices = UInt32Array::from_iter_values(selected_index_iter);

        let mut cols = Vec::with_capacity(batch.num_columns());
        for orig_col_data in batch.columns() {
            let new_col_data = compute::take(orig_col_data.as_ref(), &indices, None)?;
            cols.push(new_col_data);
        }

        cols
    };

    RecordBatch::try_new(batch.schema(), selected_columns)
}

/// Reverse the data in the [`RecordBatch`] by read and copy from the source
/// `batch`.
pub fn reverse_record_batch(batch: &RecordBatch) -> Result<RecordBatch> {
    let reversed_columns = {
        let num_rows = u32::try_from(batch.num_rows()).map_err(|e| {
            ArrowError::InvalidArgumentError(format!(
                "too many rows in a batch, convert usize to u32 failed, num_rows:{}, err:{}",
                batch.num_rows(),
                e
            ))
        })?;
        // TODO(xikai): avoid this memory allocation.
        let indices = UInt32Array::from_iter_values((0..num_rows).into_iter().rev());

        let mut cols = Vec::with_capacity(batch.num_columns());
        for orig_col_data in batch.columns() {
            let new_col_data = compute::take(orig_col_data.as_ref(), &indices, None)?;
            cols.push(new_col_data);
        }

        cols
    };

    RecordBatch::try_new(batch.schema(), reversed_columns)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::{
        array::Int32Array,
        datatypes::{DataType, Field, Schema},
    };

    use super::*;

    #[test]
    fn test_reverse_record_batch() {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        let (ids, reverse_ids) = {
            let mut source = vec![1, 2, 3, 4, 5];
            let arr = Int32Array::from(source.clone());
            source.reverse();
            let reversed_arr = Int32Array::from(source);
            (arr, reversed_arr)
        };

        let batch =
            RecordBatch::try_new(schema.clone(), vec![Arc::new(ids)]).expect("build record batch");
        let expect_reversed_batch =
            RecordBatch::try_new(schema, vec![Arc::new(reverse_ids)]).expect("build record batch");
        let reversed_batch = reverse_record_batch(&batch).expect("reverse record batch");

        assert_eq!(expect_reversed_batch, reversed_batch);
    }

    #[test]
    fn test_reverse_empty_record_batch() {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        let arr = Int32Array::from(Vec::<i32>::new());

        let batch = RecordBatch::try_new(schema, vec![Arc::new(arr)]).expect("build record batch");
        let reversed_batch = reverse_record_batch(&batch).expect("reverse record batch");

        assert_eq!(batch, reversed_batch);
    }

    #[test]
    fn test_select_record_batch() {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        let (ids, expect_selected_ids, selected_rows) = {
            let arr = Int32Array::from(vec![1, 2, 3, 4, 5]);
            let selected_arr = Int32Array::from(vec![2, 3, 5]);
            (arr, selected_arr, vec![false, true, true, false, true])
        };
        let batch =
            RecordBatch::try_new(schema.clone(), vec![Arc::new(ids)]).expect("build record batch");
        let selected_batch =
            select_record_batch(&batch, &selected_rows).expect("select record batch");
        let expect_selected_batch =
            RecordBatch::try_new(schema, vec![Arc::new(expect_selected_ids)])
                .expect("build record batch");

        assert_eq!(selected_batch, expect_selected_batch);
    }
}
