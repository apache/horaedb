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
use log::{debug, error};
use snafu::OptionExt;

use crate::partition::{
    rule::{filter::PartitionCondition, ColumnWithType, PartitionFilter, PartitionRule},
    Internal, LocateWritePartition, Result,
};

pub struct KeyRule {
    pub typed_key_columns: Vec<ColumnWithType>,
    pub partition_num: u64,
}

impl KeyRule {
    /// Check and do cartesian product to get candidate partition groups.
    ///
    /// for example:
    ///      key_col1: [f1, f2, f3]
    ///      key_col2: [f4, f5]
    /// will convert to:
    ///      group1: [key_col1: f1, key_col2: f4]
    ///      group2: [key_col1: f1, key_col2: f5]
    ///      group3: [key_col1: f2, key_col2: f4]
    ///      ...
    ///
    /// Above logics are preparing for implementing something like:
    ///     fa1 && fa2 && fb =  (fa1 && fb) && (fa2 && fb)
    /// Partitions about above expression will be calculated by following steps:
    ///     + partitions about "(fa1 && fb)" will be calculated first,
    ///        assume "partitions 1"
    ///     + partitions about "(fa2 && fb)"  will be calculated after,
    ///        assume "partitions 2"
    ///     + "total partitions" = "partitions 1" intersection "partitions 2"
    fn get_candidate_partition_keys_groups(
        &self,
        filters: &[PartitionFilter],
    ) -> Result<Vec<Vec<usize>>> {
        let column_name_to_idxs = self
            .typed_key_columns
            .iter()
            .enumerate()
            .map(|(col_idx, typed_col)| (typed_col.column.clone(), col_idx))
            .collect::<HashMap<_, _>>();
        let mut filter_by_columns = vec![Vec::new(); self.typed_key_columns.len()];

        // Group the filters by their columns.
        for (filter_idx, filter) in filters.iter().enumerate() {
            let col_idx = column_name_to_idxs
                .get(filter.column.as_str())
                .context(Internal {
                    msg: format!(
                        "column in filters but not in target, column:{}, targets:{:?}",
                        filter.column, self.typed_key_columns
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

    fn compute_partition_for_keys_group(
        &self,
        group: &[usize],
        filters: &[PartitionFilter],
        buf: &mut BytesMut,
    ) -> Result<HashSet<usize>> {
        buf.clear();

        let mut partitions = HashSet::new();
        let expanded_group = expand_partition_keys_group(group, filters)?;
        for partition_keys in expanded_group {
            let partition_key_refs = partition_keys.iter().collect_vec();
            let partition = compute_partition(&partition_key_refs, self.partition_num, buf);
            partitions.insert(partition);
        }

        Ok(partitions)
    }
}

impl PartitionRule for KeyRule {
    fn columns(&self) -> Vec<String> {
        self.typed_key_columns
            .iter()
            .map(|typed_col| typed_col.column.clone())
            .collect()
    }

    fn locate_partitions_for_write(&self, row_group: &RowGroup) -> Result<Vec<usize>> {
        // Extract idxs.
        // TODO: we should compare column's related data types in `typed_key_columns`
        // and the ones in `row_group`'s schema.
        let typed_idxs = self
            .typed_key_columns
            .iter()
            .map(|typed_col| row_group.schema().index_of(typed_col.column.as_str()))
            .collect::<Option<Vec<_>>>()
            .context(LocateWritePartition {
                msg: format!(
                    "not all key columns found in schema when locate partition by key strategy, key columns:{:?}",
                    self.typed_key_columns
                ),
            })?;

        // Compute partitions.
        let mut buf = BytesMut::new();
        let partitions = row_group
            .iter()
            .map(|row| self.compute_partition_for_inserted_row(row, &typed_idxs, &mut buf))
            .collect();
        Ok(partitions)
    }

    fn locate_partitions_for_read(&self, filters: &[PartitionFilter]) -> Result<Vec<usize>> {
        let all_partitions = (0..self.partition_num as usize).into_iter().collect();

        // Filters are empty.
        if filters.is_empty() {
            return Ok(all_partitions);
        }

        // Group the filters by their columns.
        // If found invalid filter, return all partitions.
        let candidate_partition_keys_groups = self
            .get_candidate_partition_keys_groups(filters)
            .map_err(|e| {
                error!("KeyRule locate partition for read, err:{}", e);
            })
            .unwrap_or_default();
        if candidate_partition_keys_groups.is_empty() {
            return Ok(all_partitions);
        }

        let mut buf = BytesMut::new();
        let (first_group, rest_groups) = candidate_partition_keys_groups.split_first().unwrap();
        let mut target_partitions =
            self.compute_partition_for_keys_group(first_group, filters, &mut buf)?;
        for group in rest_groups {
            // Same as above, if found invalid, return all partitions.
            let partitions = match self.compute_partition_for_keys_group(group, filters, &mut buf) {
                Ok(partitions) => partitions,
                Err(e) => {
                    error!("KeyRule locate partition for read, err:{}", e);
                    return Ok(all_partitions);
                }
            };

            target_partitions = target_partitions
                .intersection(&partitions)
                .copied()
                .collect::<HashSet<_>>();
        }

        Ok(target_partitions.into_iter().collect())
    }
}

fn expand_partition_keys_group(
    group: &[usize],
    filters: &[PartitionFilter],
) -> Result<Vec<Vec<Datum>>> {
    let mut datum_by_columns = Vec::with_capacity(group.len());
    for filter_idx in group {
        let filter = &filters[*filter_idx];
        let datums = match &filter.condition {
            // Only `Eq` is supported now.
            // TODO: to support `In`'s extracting.
            PartitionCondition::Eq(datum) => vec![datum.clone()],
            PartitionCondition::In(datums) => datums.clone(),
            _ => {
                return Internal {
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
pub(crate) fn compute_partition(
    partition_keys: &[&Datum],
    partition_num: u64,
    buf: &mut BytesMut,
) -> usize {
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
            typed_key_columns: vec![ColumnWithType::new("col1".to_string(), DatumKind::UInt32)],
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
            typed_key_columns: vec![
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
        let expanded_group = expand_partition_keys_group(&group, &filters).unwrap();
        let expected = vec![
            vec![Datum::UInt32(1), Datum::UInt32(2), Datum::UInt32(3)],
            vec![Datum::UInt32(1), Datum::UInt32(22), Datum::UInt32(3)],
        ];
        assert_eq!(expanded_group, expected);
    }
}
