// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

//! Key partition rule

use std::collections::{HashMap, HashSet};

use common_types::{
    bytes::{BufMut, BytesMut},
    datum::Datum,
    hash::hash64,
    row::{Row, RowGroup},
};
use itertools::Itertools;
use log::debug;
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

        // Group the filters by their columns.
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
        debug!(
            "KeyRule get candidate partition keys groups, filter_by_columns:{:?}",
            filter_by_columns
        );

        // Check and do cartesian product to get candidate partition groups.
        // for example:
        //      key_col1: [f1, f2, f3]
        //      key_col2: [f4, f5]
        // will convert to:
        //      group1: [key_col1: f1, key_col2: f4]
        //      group2: [key_col1: f1, key_col2: f5]
        //      group3: [key_col1: f2, key_col2: f4]
        //      ...
        let empty_filter = filter_by_columns.iter().find(|filter| filter.is_empty());
        if empty_filter.is_some() {
            return Ok(Vec::default());
        }

        let groups = filter_by_columns
            .into_iter()
            .map(|filters| filters.into_iter())
            .multi_cartesian_product()
            .collect_vec();
        debug!(
            "KeyRule get candidate partition keys groups, groups:{:?}",
            groups
        );

        Ok(groups)
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
            // TODO: to support `In`'s extracting.
            PartitionCondition::Eq(datum) => vec![datum.clone()],
            PartitionCondition::In(datums) => datums.clone(),
            _ => {
                return LocateReadPartition {
                    msg: format!("invalid partition filter found, filter:{:?},", filter),
                }
                .fail()
            }
        };

        datum_by_columns.push(datums);
    }

    let expanded_group = datum_by_columns
        .into_iter()
        .map(|filters| filters.into_iter())
        .multi_cartesian_product()
        .collect_vec();
    Ok(expanded_group)
}

// Compute partition
fn compute_partition(partition_keys: &[&Datum], partition_num: u64, buf: &mut BytesMut) -> usize {
    buf.clear();
    partition_keys
        .iter()
        .for_each(|datum| buf.put_slice(&datum.to_bytes()));

    (hash64(buf) % partition_num) as usize
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

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

    #[test]
    fn test_get_candidate_partition_keys_groups() {
        // Key rule of keys:[col1, col2, col3]
        let partition_num = 16;
        let key_rule = KeyRule {
            typed_columns: vec![
                ColumnWithType::new("col1".to_string(), DatumKind::UInt32),
                ColumnWithType::new("col2".to_string(), DatumKind::UInt32),
                ColumnWithType::new("col3".to_string(), DatumKind::UInt32),
            ],
            partition_num,
        };

        // Filters(related columns: col1, col2, col3, col1, col2)
        let filter1 = PartitionFilter {
            column: "col1".to_string(),
            condition: PartitionCondition::Eq(Datum::UInt32(1)),
        };

        let filter2 = PartitionFilter {
            column: "col2".to_string(),
            condition: PartitionCondition::Eq(Datum::UInt32(2)),
        };

        let filter3 = PartitionFilter {
            column: "col3".to_string(),
            condition: PartitionCondition::Eq(Datum::UInt32(3)),
        };

        let filter4 = PartitionFilter {
            column: "col1".to_string(),
            condition: PartitionCondition::Eq(Datum::UInt32(4)),
        };

        let filter5 = PartitionFilter {
            column: "col2".to_string(),
            condition: PartitionCondition::Eq(Datum::UInt32(5)),
        };

        let filters = vec![filter1, filter2, filter3, filter4, filter5];

        // Groups' len: 4
        // Col1's filter idxs: [0, 3]
        // Col2's filter idxs: [1, 4]
        // Col3's filter idxs: [2]
        let groups = key_rule
            .get_candidate_partition_keys_groups(&filters)
            .unwrap();
        assert_eq!(groups.len(), 4);

        let mut filter_in_groups = vec![BTreeSet::new(); 3];
        for group in groups {
            filter_in_groups[0].insert(group[0]);
            filter_in_groups[1].insert(group[1]);
            filter_in_groups[2].insert(group[2]);
        }
        let filter_in_groups = filter_in_groups
            .into_iter()
            .map(|filters| filters.into_iter().collect_vec())
            .collect_vec();

        assert_eq!(filter_in_groups[0], [0, 3]);
        assert_eq!(filter_in_groups[1], [1, 4]);
        assert_eq!(filter_in_groups[2], [2]);
    }

    #[test]
    fn test_expand_partition_keys_group() {
        // Filters(related columns: col1, col2, col3, col1)
        let filter1 = PartitionFilter {
            column: "col1".to_string(),
            condition: PartitionCondition::Eq(Datum::UInt32(1)),
        };

        let filter2 = PartitionFilter {
            column: "col2".to_string(),
            condition: PartitionCondition::In(vec![Datum::UInt32(2), Datum::UInt32(22)]),
        };

        let filter3 = PartitionFilter {
            column: "col3".to_string(),
            condition: PartitionCondition::Eq(Datum::UInt32(3)),
        };

        let filter4 = PartitionFilter {
            column: "col1".to_string(),
            condition: PartitionCondition::Eq(Datum::UInt32(4)),
        };
        let filters = vec![filter1, filter2, filter3, filter4];

        // Group
        let group = vec![0, 1, 2];

        // Expanded group
        let expanded_group = expand_partition_keys_group(group, &filters).unwrap();
        let expected = vec![
            vec![Datum::UInt32(1), Datum::UInt32(2), Datum::UInt32(3)],
            vec![Datum::UInt32(1), Datum::UInt32(22), Datum::UInt32(3)],
        ];
        assert_eq!(expanded_group, expected);
    }
}
