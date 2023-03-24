// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

use std::cmp::Ordering;

use async_trait::async_trait;
use common_types::{
    record_batch::{RecordBatchWithKey, RecordBatchWithKeyBuilder},
    request_id::RequestId,
    row::{Row, RowViewOnBatch, RowWithMeta},
    schema::RecordSchemaWithKey,
};
use common_util::{
    define_result,
    error::{BoxError, GenericError},
};
use log::{info, trace};
use snafu::{ResultExt, Snafu};

use crate::row_iter::{IterOptions, RecordBatchWithKeyIterator};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Failed to iterate column, error:{:?}", source))]
    IterateColumn { source: common_types::row::Error },

    #[snafu(display("Failed to build record batch, error:{:?}", source))]
    BuildRecordBatch {
        source: common_types::record_batch::Error,
    },

    #[snafu(display("Failed to append row, err:{:?}", source))]
    AppendRow {
        source: common_types::record_batch::Error,
    },

    #[snafu(display("Failed to read data from the sub iterator, err:{:?}", source))]
    ReadFromSubIter { source: GenericError },
}

define_result!(Error);

/// Dedup the elements from the `iter` by choosing the first one in the
/// duplicate rows.
pub struct DedupIterator<I> {
    request_id: RequestId,
    schema: RecordSchemaWithKey,
    record_batch_builder: RecordBatchWithKeyBuilder,
    iter: I,
    /// Previous row returned.
    prev_row: Option<Row>,
    /// Store which row in record batch is keep, use Vec<bool> is a bit faster
    /// than a bitmap.
    selected_rows: Vec<bool>,

    // Metrics:
    total_duplications: usize,
    total_selected_rows: usize,
}

impl<I: RecordBatchWithKeyIterator> DedupIterator<I> {
    pub fn new(request_id: RequestId, iter: I, iter_options: IterOptions) -> Self {
        let schema = iter.schema();

        let record_batch_builder =
            RecordBatchWithKeyBuilder::with_capacity(schema.clone(), iter_options.batch_size);
        Self {
            request_id,
            schema: schema.clone(),
            record_batch_builder,
            iter,
            prev_row: None,
            selected_rows: Vec::new(),
            total_duplications: 0,
            total_selected_rows: 0,
        }
    }

    fn dedup_batch(&mut self, record_batch: RecordBatchWithKey) -> Result<RecordBatchWithKey> {
        self.selected_rows.clear();
        // Ignore all rows by default.
        self.selected_rows.resize(record_batch.num_rows(), false);

        if record_batch.is_empty() {
            return Ok(record_batch);
        }

        // Dedup batch.
        for col_idx in self.schema.primary_key_idx() {
            let column = record_batch.column(*col_idx);

            column.dedup(&mut self.selected_rows);
        }

        // Dedup first row in record batch with previous row.
        if let Some(prev_row) = &self.prev_row {
            let prev_row_view = RowWithMeta {
                row: prev_row,
                schema: &self.schema,
            };
            let curr_row_view = RowViewOnBatch {
                record_batch: &record_batch,
                // First row.
                row_idx: 0,
            };

            let is_equal = matches!(
                // TODO(yingwen): Compare row needs clone data of row.
                self.schema.compare_row(&prev_row_view, &curr_row_view),
                Ordering::Equal
            );

            if is_equal {
                // Depulicate with previous row.
                self.selected_rows[0] = false;
            }
        }

        let selected_num = self
            .selected_rows
            .iter()
            .map(|v| if *v { 1 } else { 0 })
            .sum();

        // Eventhough all rows are duplicate, we can still use row pointed by
        // prev_row_idx because they have same row key.
        self.prev_row = Some(record_batch.clone_row_at(record_batch.num_rows() - 1));

        self.filter_batch(record_batch, selected_num)
    }

    /// Filter batch by `selected_rows`.
    fn filter_batch(
        &mut self,
        record_batch: RecordBatchWithKey,
        selected_num: usize,
    ) -> Result<RecordBatchWithKey> {
        self.total_selected_rows += selected_num;
        self.total_duplications += record_batch.num_rows() - selected_num;

        if selected_num == record_batch.num_rows() {
            // No duplicate rows in batch.
            return Ok(record_batch);
        }

        self.record_batch_builder.clear();
        for (row_idx, selected) in self.selected_rows.iter().enumerate() {
            if *selected {
                self.record_batch_builder
                    .append_row_view(&RowViewOnBatch {
                        record_batch: &record_batch,
                        row_idx,
                    })
                    .context(AppendRow)?;
            }
        }

        self.record_batch_builder.build().context(BuildRecordBatch)
    }
}

#[async_trait]
impl<I: RecordBatchWithKeyIterator> RecordBatchWithKeyIterator for DedupIterator<I> {
    type Error = Error;

    fn schema(&self) -> &RecordSchemaWithKey {
        &self.schema
    }

    async fn next_batch(&mut self) -> Result<Option<RecordBatchWithKey>> {
        match self
            .iter
            .next_batch()
            .await
            .box_err()
            .context(ReadFromSubIter)?
        {
            Some(record_batch) => {
                trace!(
                    "DedupIterator received next record batch, request_id:{}, batch:{:?}",
                    self.request_id,
                    record_batch
                );

                self.dedup_batch(record_batch).map(Some)
            }
            None => {
                info!(
                    "DedupIterator received none record batch, request_id:{}, total_duplications:{}, total_selected_rows:{}",
                    self.request_id, self.total_duplications, self.total_selected_rows,
                );

                Ok(None)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use common_types::tests::{build_row, build_schema};

    use super::*;
    use crate::row_iter::tests::{build_record_batch_with_key, check_iterator, VectorIterator};

    #[tokio::test]
    async fn test_dedup_iterator() {
        // first two columns are key columns
        let schema = build_schema();
        let iter = VectorIterator::new(
            schema.to_record_schema_with_key(),
            vec![
                build_record_batch_with_key(
                    schema.clone(),
                    vec![
                        build_row(b"a", 1, 10.0, "v1", 1000, 1_000_000),
                        build_row(b"a", 1, 10.0, "v", 1000, 1_000_000),
                        build_row(b"a", 2, 10.0, "v2", 2000, 2_000_000),
                    ],
                ),
                build_record_batch_with_key(
                    schema,
                    vec![
                        build_row(b"a", 2, 10.0, "v", 2000, 2_000_000),
                        build_row(b"a", 3, 10.0, "v3", 3000, 3_000_000),
                        build_row(b"a", 3, 10.0, "v", 3000, 3_000_000),
                        build_row(b"a", 4, 10.0, "v4", 4000, 4_000_000),
                    ],
                ),
            ],
        );

        let mut iter =
            DedupIterator::new(RequestId::next_id(), iter, IterOptions { batch_size: 500 });
        check_iterator(
            &mut iter,
            vec![
                build_row(b"a", 1, 10.0, "v1", 1000, 1_000_000),
                build_row(b"a", 2, 10.0, "v2", 2000, 2_000_000),
                build_row(b"a", 3, 10.0, "v3", 3000, 3_000_000),
                build_row(b"a", 4, 10.0, "v4", 4000, 4_000_000),
            ],
        )
        .await;
    }
}
