// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

//! Key partition rule

use std::collections::{HashMap, HashSet};

use common_types::{
    bytes::{BufMut, BytesMut},
    datum::Datum,
    hash::hash64,
    row::{Row, RowGroup},
};
use itertools::{iproduct, Itertools};
use snafu::OptionExt;

use crate::partition::{
    rule::{filter::PartitionCondition, ColumnWithType, PartitionFilter, PartitionRule},
    LocateReadPartition, LocateWritePartition, Result,
};

pub struct KeyRule {
    pub typed_columns: Vec<ColumnWithType>,
    pub partition_num: u64,
}

impl KeyRule {
    fn get_candidate_partition_keys_groups(
        &self,
        filters: &[PartitionFilter],
    ) -> Result<Vec<Vec<usize>>> {
        let column_name_to_idxs = self
            .typed_columns
            .iter()
            .enumerate()
            .map(|(col_idx, typed_col)| (typed_col.column.clone(), col_idx))
            .collect::<HashMap<_, _>>();
        let mut filter_by_columns = vec![Vec::new(); self.typed_columns.len()];
        for (filter_idx, filter) in filters.iter().enumerate() {
            let col_idx = column_name_to_idxs
                .get(filter.column.as_str())
                .with_context(|| LocateReadPartition {
                    msg: format!(
                        "column in filters but not in targets, column:{}, targets:{:?}",
                        filter.column, self.typed_columns
                    ),
                })?;

            filter_by_columns
                .get_mut(*col_idx)
                .unwrap()
                .push(filter_idx);
        }

        // Check and do cartesian product to get candidate partition groups.
        let empty_filter = filter_by_columns.iter().find(|filter| filter.is_empty());
        if empty_filter.is_some() {
            return Ok(Vec::default());
        }

        Ok(iproduct!(filter_by_columns).collect())
    }

    fn compute_partition_for_inserted_row(
        &self,
        row: &Row,
        target_column_idxs: &[usize],
        buf: &mut BytesMut,
    ) -> usize {
        let partition_keys = target_column_idxs
            .iter()
            .map(|col_idx| &row[*col_idx])
            .collect_vec();
        compute_partition(&partition_keys, self.partition_num, buf)
    }
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
            .with_context(|| LocateWritePartition {
                msg: format!(
                    "in key partition not all key columns found in schema, key columns:{:?}",
                    self.typed_columns
                ),
            })?;

        // Compute partitions.
        let mut buf = BytesMut::new();
        let partitions = row_group
            .iter()
            .map(|row| self.compute_partition_for_inserted_row(row, &typed_idxs, &mut buf))
            .collect_vec();
        Ok(partitions)
    }

    fn locate_partitions_for_read(&self, filters: &[PartitionFilter]) -> Result<Vec<usize>> {
        let all_partitions = (0..self.partition_num as usize).into_iter().collect_vec();

        // Filters are empty.
        if filters.is_empty() {
            return Ok(all_partitions);
        }

        // Group the filters by their columns.
        let candidate_partition_keys_groups = self.get_candidate_partition_keys_groups(filters)?;
        if candidate_partition_keys_groups.is_empty() {
            return Ok(all_partitions);
        }

        let mut partitions = HashSet::new();
        let mut buf = BytesMut::new();
        for group in candidate_partition_keys_groups {
            let expanded_group = expand_partition_keys_group(group, filters)?;
            for partition_keys in expanded_group {
                let partition_key_refs = partition_keys.iter().collect_vec();
                let partition =
                    compute_partition(&partition_key_refs, self.partition_num, &mut buf);
                partitions.insert(partition);
            }
        }

        Ok(partitions.into_iter().collect_vec())
    }
}

fn expand_partition_keys_group(
    group: Vec<usize>,
    filters: &[PartitionFilter],
) -> Result<Vec<Vec<Datum>>> {
    let mut datum_by_columns = Vec::with_capacity(group.len());
    for filter_idx in group {
        let filter = &filters[filter_idx];
        let datums = match &filter.condition {
            // Only `Eq` is supported now.
            // TODO: support `In`.
            PartitionCondition::Eq(datum) => vec![datum.clone()],
            _ => {
                return LocateReadPartition {
                    msg: format!("invalid partition filter found, filter:{:?},", filter),
                }
                .fail()
            }
        };

        datum_by_columns.push(datums);
    }

    Ok(iproduct!(datum_by_columns).collect())
}

// Helper function for computing partition
fn compute_partition(partition_keys: &[&Datum], partition_num: u64, buf: &mut BytesMut) -> usize {
    buf.clear();
    partition_keys
        .iter()
        .for_each(|datum| buf.put_slice(&datum.to_bytes()));

    (hash64(buf) % partition_num) as usize
}

#[cfg(test)]
mod tests {
    use common_types::{datum::DatumKind, string::StringBytes};

    use super::*;

    #[test]
    fn test_compute_partition_for_inserted_row() {
        let partition_num = 16;
        let key_rule = KeyRule {
            typed_columns: vec![ColumnWithType::new("col1".to_string(), DatumKind::UInt32)],
            partition_num,
        };

        let datums = vec![
            Datum::Int32(1),
            Datum::Int32(42),
            Datum::String(StringBytes::copy_from_str("test")),
            Datum::Int64(84),
            Datum::Null,
        ];
        let row = Row::from_datums(datums.clone());
        let defined_idxs = vec![1_usize, 2, 3, 4];

        // Actual
        let mut buf = BytesMut::new();
        let actual = key_rule.compute_partition_for_inserted_row(&row, &defined_idxs, &mut buf);

        // Expected
        buf.clear();
        buf.put_slice(&datums[1].to_bytes());
        buf.put_slice(&datums[2].to_bytes());
        buf.put_slice(&datums[3].to_bytes());
        buf.put_slice(&datums[4].to_bytes());
        let expected = (hash64(&buf) % partition_num) as usize;

        assert_eq!(actual, expected);
    }
}
