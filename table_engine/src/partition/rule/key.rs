// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

//! Key partition rule

use std::collections::{HashMap, HashSet};

use common_types::{
    bytes::{BufMut, Bytes, BytesMut},
    datum::Datum,
    hash::hash64,
    row::{Row, RowGroup},
};
use snafu::OptionExt;

use crate::partition::{
    self,
    rule::{filter::PartitionCondition, ColumnWithType, PartitionFilter, PartitionRule},
    LocateWritePartitionNoCause, Result,
};

pub struct KeyRule {
    pub typed_columns: Vec<ColumnWithType>,
    pub partition_num: u64,
}

impl PartitionRule for KeyRule {
    fn columns(&self) -> Vec<String> {
        self.typed_columns
            .iter()
            .map(|typed_col| typed_col.column.clone())
            .collect()
    }

    fn locate_partitions_for_write(&self, row_group: &RowGroup) -> Result<Vec<usize>> {
        // Extract idxs.
        let typed_idxs = self
            .typed_columns
            .iter()
            .map(|typed_col| row_group.schema().index_of(typed_col.column.as_str()))
            .collect::<Option<Vec<_>>>()
            .with_context(|| LocateWritePartitionNoCause {
                msg: format!(
                    "in key partition not all key columns found in schema, key columns:{:?}",
                    self.typed_columns
                ),
            })?;

        // Compute partitions.
        let mut buf = BytesMut::new();
        let partitions = row_group
            .iter()
            .map(|row| {
                compute_write_partition_for_row(row, &typed_idxs, self.partition_num, &mut buf)
            })
            .collect::<Vec<_>>();
        Ok(partitions)
    }

    fn locate_partitions_for_read(&self, filters: &[PartitionFilter]) -> Result<Vec<usize>> {
        let all_partitions = (0..self.partition_num as usize)
            .into_iter()
            .collect::<Vec<_>>();

        // Filters are empty.
        if filters.is_empty() {
            return Ok(all_partitions);
        }

        let mut filters_map = HashMap::with_capacity(self.typed_columns.len());
        filters.iter().enumerate().for_each(|(idx, pf)| {
            let column_entry = filters_map
                .entry(pf.column.clone())
                .or_insert(Vec::default());
            column_entry.push(idx);
        });

        // If exist `colum` not in filters, return all partitions.
        assert!(filters_map.len() <= self.typed_columns.len());
        if filters_map.len() < self.typed_columns.len() {
            return Ok(all_partitions);
        }

        // Get value
        let mut prepare_bytes = vec![Vec::default()];
        for typed_column in &self.typed_columns {
            let idxs = filters_map.get(typed_column.column.as_str()).unwrap();
            // TODO: because we can build `HashSet<Datum>` due to float in `Datum`,
            // we can't support the situation "idxs.len() > 1".
            if idxs.len() > 1 {
                return Ok(all_partitions);
            }

            let idx = idxs.get(0).unwrap();
            let filter = filters.get(*idx).unwrap();
            let datums = match filter.condition.clone() {
                PartitionCondition::Eq(datum) => vec![datum],
                // TODO:`In` should be supported
                _ => unreachable!(),
            };

            prepare_bytes = compute_prepare_bytes(prepare_bytes, &datums);
        }

        let partition_set = prepare_bytes
            .into_iter()
            .map(|mut bytes| (hash64(&mut bytes) % self.partition_num) as usize)
            .collect::<HashSet<_>>();

        Ok(partition_set.into_iter().collect())
    }
}

// Helper function for compute partition for reading
fn compute_prepare_bytes(old_bytes: Vec<Vec<u8>>, datums: &[Datum]) -> Vec<Vec<u8>> {
    let mut new_bytes = Vec::with_capacity(old_bytes.len() * datums.len());
    let mut buf = BytesMut::new();
    for one_bytes in old_bytes {
        for datum in datums {
            buf.clear();
            buf.put_slice(&one_bytes);
            buf.put_slice(&datum.to_bytes());

            new_bytes.push(buf.to_vec())
        }
    }

    new_bytes
}

// Helper function for compute partition for writing
fn compute_write_partition_for_row(
    row: &Row,
    target_columns: &[usize],
    partition_num: u64,
    buf: &mut BytesMut,
) -> usize {
    buf.clear();
    target_columns
        .iter()
        .for_each(|idx| buf.put_slice(&row[*idx].to_bytes()));

    (hash64(&buf.to_vec()) % partition_num) as usize
}
