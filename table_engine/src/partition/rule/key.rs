// Copyright 2023 The CeresDB Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Key partition rule

use std::collections::{HashMap, HashSet};

use common_types::{
    datum::{Datum, DatumView},
    row::{Row, RowGroup},
};
use hash_ext::hash64;
use itertools::Itertools;
use log::{debug, error};
use snafu::OptionExt;

use crate::partition::{
    rule::{filter::PartitionCondition, ColumnWithType, PartitionFilter, PartitionRule},
    Internal, LocateWritePartition, Result,
};

pub const DEFAULT_PARTITION_VERSION: i32 = 0;

pub struct KeyRule {
    pub typed_key_columns: Vec<ColumnWithType>,
    pub partition_num: usize,
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
    ///     + partitions about "(fa1 && fb)" will be calculated first, assume
    ///       "partitions 1"
    ///     + partitions about "(fa2 && fb)"  will be calculated after, assume
    ///       "partitions 2"
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

    fn compute_partition_for_inserted_row(&self, row: &Row, target_column_idxs: &[usize]) -> usize {
        let partition_keys = target_column_idxs
            .iter()
            .map(|col_idx| row[*col_idx].as_view());
        compute_partition(partition_keys, self.partition_num)
    }

    fn compute_partition_for_keys_group(
        &self,
        group: &[usize],
        filters: &[PartitionFilter],
    ) -> Result<HashSet<usize>> {
        let mut partitions = HashSet::new();
        let expanded_group = expand_partition_keys_group(group, filters)?;
        for partition_keys in expanded_group {
            let partition = compute_partition(partition_keys.into_iter(), self.partition_num);
            partitions.insert(partition);
        }

        Ok(partitions)
    }

    #[inline]
    fn all_partitions(&self) -> Vec<usize> {
        (0..self.partition_num).collect_vec()
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
        let partitions = row_group
            .iter()
            .map(|row| self.compute_partition_for_inserted_row(row, &typed_idxs))
            .collect();
        Ok(partitions)
    }

    fn locate_partitions_for_read(&self, filters: &[PartitionFilter]) -> Result<Vec<usize>> {
        // Filters are empty.
        if filters.is_empty() {
            return Ok(self.all_partitions());
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
            return Ok(self.all_partitions());
        }

        let (first_group, rest_groups) = candidate_partition_keys_groups.split_first().unwrap();
        let mut target_partitions = self.compute_partition_for_keys_group(first_group, filters)?;
        for group in rest_groups {
            // Same as above, if found invalid, return all partitions.
            let partitions = match self.compute_partition_for_keys_group(group, filters) {
                Ok(partitions) => partitions,
                Err(e) => {
                    error!("KeyRule locate partition for read, err:{}", e);
                    return Ok(self.all_partitions());
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

fn expand_partition_keys_group<'a>(
    group: &[usize],
    filters: &'a [PartitionFilter],
) -> Result<impl Iterator<Item = Vec<DatumView<'a>>>> {
    let mut datum_by_columns = Vec::with_capacity(group.len());
    for filter_idx in group {
        let filter = &filters[*filter_idx];
        let datums = match &filter.condition {
            // Only `Eq` is supported now.
            // TODO: to support `In`'s extracting.
            PartitionCondition::Eq(datum) => vec![datum.as_view()],
            PartitionCondition::In(datums) => datums.iter().map(Datum::as_view).collect_vec(),
            _ => {
                return Internal {
                    msg: format!("invalid partition filter found, filter:{filter:?},"),
                }
                .fail()
            }
        };

        datum_by_columns.push(datums);
    }

    Ok(datum_by_columns
        .into_iter()
        .map(|filters| filters.into_iter())
        .multi_cartesian_product())
}

/// The adapter to implement [`std::io::Read`] over the partition keys, which is
/// used for computing hash.
struct PartitionKeysReadAdapter<'a, T> {
    key_views: T,
    /// The current key for reading bytes.
    ///
    /// It can be `None` if the whole current key is exhausted.
    curr_key: Option<DatumView<'a>>,
    /// The offset in the serialized bytes from `curr_key`.
    ///
    /// This field has no meaning when the `curr_key` is `None`.
    curr_key_offset: usize,
}

impl<'a, T> PartitionKeysReadAdapter<'a, T> {
    fn new(key_views: T) -> Self {
        Self {
            key_views,
            curr_key: None,
            curr_key_offset: 0,
        }
    }
}

impl<'a, T> PartitionKeysReadAdapter<'a, T>
where
    T: Iterator<Item = DatumView<'a>>,
{
    /// Read the serialized bytes from `curr_key` to fill the `buf` as much
    /// as possible.
    ///
    /// If the Ok(None) is returned, it means the reader has been exhausted.
    fn read_once(&mut self, buf: &mut [u8]) -> std::io::Result<Option<usize>> {
        // Fetch the next key.
        if self.curr_key.is_none() {
            self.curr_key = self.key_views.next();
            self.curr_key_offset = 0;
        }

        // The `key_views` has been exhausted.
        if self.curr_key.is_none() {
            return Ok(None);
        }

        let datum_view = self.curr_key.as_ref().unwrap();
        let offset = self.curr_key_offset;
        let mut n_bytes = 0;
        let mut key_exhausted = false;
        datum_view.do_with_bytes(|source: &[u8]| {
            let end = (offset + buf.len()).min(source.len());
            let read_slice = &source[offset..end];
            buf[..read_slice.len()].copy_from_slice(read_slice);

            // Update the offset to the end.
            self.curr_key_offset = end;
            // Current key is exhausted.
            key_exhausted = end == source.len();
            // Record the number of bytes that has been read.
            n_bytes = read_slice.len();
        });

        // Clear the current key if it is exhausted.
        if key_exhausted {
            self.curr_key = None;
        }

        Ok(Some(n_bytes))
    }
}

impl<'a, T> std::io::Read for PartitionKeysReadAdapter<'a, T>
where
    T: Iterator<Item = DatumView<'a>>,
{
    /// This implementation will fill the whole buf as much as possible, and the
    /// only scenario where the buf is not fully filled is that the serialized
    /// bytes from the `self.key_views` are exhausted.
    ///
    /// NOTE: The best way is to fill the `buf` key after key rather than fill
    /// the `buf` as much as possible. And an example can illustrate the reason
    /// for this way, saying there are two `key_views`s, ['a', 'bc'], and
    /// ['ab', 'c'], in the way to fill the buf as much as possible, the two
    /// `key_views`s will output the same hash result, while the best way
    /// can generate different results. However, here the best way is not
    /// chosen for compatibility.
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let mut total_n_bytes = 0;
        loop {
            if total_n_bytes == buf.len() {
                break;
            }
            assert!(total_n_bytes < buf.len());

            let next_buf = &mut buf[total_n_bytes..];
            let n_bytes = self.read_once(next_buf)?;
            if let Some(n) = n_bytes {
                total_n_bytes += n;
            } else {
                // No more data can be pulled.
                break;
            }
        }

        Ok(total_n_bytes)
    }
}

// Compute partition
pub(crate) fn compute_partition<'a>(
    partition_keys: impl Iterator<Item = DatumView<'a>>,
    partition_num: usize,
) -> usize {
    let reader = PartitionKeysReadAdapter::new(partition_keys);
    (hash64(reader) as usize) % partition_num
}

#[cfg(test)]
mod tests {
    use std::{collections::BTreeSet, io::Read};

    use common_types::{
        datum::{Datum, DatumKind},
        string::StringBytes,
    };

    use super::*;

    /// This test ensures the output of `read` method of
    /// [PartitionKeysReadAdapter] to be the same as the provided `buf`'s length
    /// as much as possible.
    #[test]
    fn test_partition_keys_read_adapter() {
        let datums = vec![
            Datum::Int32(1),
            Datum::String(StringBytes::copy_from_str(
                "test_partition_keys_read_adapter_key0",
            )),
            Datum::String(StringBytes::copy_from_str(
                "test_partition_keys_read_adapter_key1",
            )),
            Datum::Null,
        ];
        let datum_views = datums.iter().map(Datum::as_view);
        let mut adapter = PartitionKeysReadAdapter::new(datum_views);
        let mut buf = vec![0; 16];
        let mut total_bytes = 0;
        loop {
            let n = adapter.read(&mut buf).unwrap();
            total_bytes += n;
            if n < buf.len() {
                // The reader has been exhausted. Let's check it.
                assert_eq!(adapter.read(&mut buf).unwrap(), 0);
                break;
            }

            assert_eq!(n, buf.len());
        }

        let expect_total_bytes: usize = datums.iter().map(|v| v.size()).sum();
        assert_eq!(total_bytes, expect_total_bytes);
    }

    #[test]
    fn test_compute_partition_for_inserted_row_with_empty_string() {
        let partition_num = 161709;
        let key_rule = KeyRule {
            typed_key_columns: vec![ColumnWithType::new("col1".to_string(), DatumKind::String)],
            partition_num,
        };

        let datums = vec![
            Datum::Int32(1),
            Datum::String(StringBytes::copy_from_str("")),
            Datum::String(StringBytes::copy_from_str("xtest")),
            Datum::Int64(84),
            Datum::Null,
        ];
        let row = Row::from_datums(datums);
        let defined_idxs = vec![1, 2, 3, 4];

        let partition_id = key_rule.compute_partition_for_inserted_row(&row, &defined_idxs);
        assert_eq!(partition_id, 104979);
    }

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
        let row = Row::from_datums(datums);
        let defined_idxs = vec![1, 2, 3, 4];

        // Actual
        let partition_id = key_rule.compute_partition_for_inserted_row(&row, &defined_idxs);
        assert_eq!(partition_id, 6);
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
        let expanded_group = expand_partition_keys_group(&group, &filters)
            .unwrap()
            .map(|v| v.iter().map(|view| view.to_datum()).collect_vec())
            .collect_vec();
        let expected = vec![
            vec![Datum::UInt32(1), Datum::UInt32(2), Datum::UInt32(3)],
            vec![Datum::UInt32(1), Datum::UInt32(22), Datum::UInt32(3)],
        ];
        assert_eq!(expanded_group, expected);
    }
}
