// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

// Filter for row groups.

use std::cmp::Ordering;

use arrow::datatypes::SchemaRef;
use common_types::datum::Datum;
use datafusion::{prelude::Expr, scalar::ScalarValue};
use log::debug;
use parquet::file::metadata::RowGroupMetaData;
use parquet_ext::prune::{
    equal::{self, ColumnPosition},
    min_max,
};
use snafu::ensure;

use super::meta_data::RowGroupBloomFilter;
use crate::sst::reader::error::{OtherNoCause, Result};

/// A filter to prune row groups according to the provided predicates.
///
/// Currently, two kinds of filters will be applied to such filtering:
/// min max & bloom filter.
pub struct RowGroupFilter<'a> {
    schema: &'a SchemaRef,
    row_groups: &'a [RowGroupMetaData],
    blooms: Option<&'a [RowGroupBloomFilter]>,
    predicates: &'a [Expr],
}

impl<'a> RowGroupFilter<'a> {
    pub fn try_new(
        schema: &'a SchemaRef,
        row_groups: &'a [RowGroupMetaData],
        blooms: Option<&'a [RowGroupBloomFilter]>,
        predicates: &'a [Expr],
    ) -> Result<Self> {
        if let Some(blooms) = blooms {
            ensure!(blooms.len() == row_groups.len(), OtherNoCause {
                msg: format!("expect the same number of bloom filter as the number of row groups, num_bloom_filters:{}, num_row_groups:{}", blooms.len(), row_groups.len()),
            });
        }

        Ok(Self {
            schema,
            row_groups,
            blooms,
            predicates,
        })
    }

    pub fn filter(&self) -> Vec<usize> {
        debug!(
            "Begin to filter row groups, total_row_groups:{}, bloom_filtering:{}, predicates:{:?}",
            self.row_groups.len(),
            self.blooms.is_some(),
            self.predicates,
        );

        let filtered0 = self.filter_by_min_max();
        match self.blooms {
            Some(v) => {
                // TODO: We can do continuous filtering based on the `filtered0` to reduce the
                // filtering cost.
                let filtered1 = self.filter_by_bloom(v);
                let filtered = Self::intersect_filtered_row_groups(&filtered0, &filtered1);

                debug!(
                    "Finish filtering row groups by blooms and min_max, total_row_groups:{}, filtered_by_min_max:{}, filtered_by_blooms:{}, filtered_by_both:{}",
                    self.row_groups.len(),
                    filtered0.len(),
                    filtered1.len(),
                    filtered.len(),
                );

                filtered
            }
            None => {
                debug!(
                    "Finish filtering row groups by min_max, total_row_groups:{}, filtered_row_groups:{}",
                    self.row_groups.len(),
                    filtered0.len(),
                );
                filtered0
            }
        }
    }

    fn filter_by_min_max(&self) -> Vec<usize> {
        min_max::filter_row_groups(self.schema.clone(), self.predicates, self.row_groups)
    }

    /// Filter row groups according to the bloom filter.
    fn filter_by_bloom(&self, row_group_bloom_filters: &[RowGroupBloomFilter]) -> Vec<usize> {
        let is_equal =
            |col_pos: ColumnPosition, val: &ScalarValue, negated: bool| -> Option<bool> {
                let datum = Datum::from_scalar_value(val)?;
                let exist = row_group_bloom_filters
                    .get(col_pos.row_group_idx)?
                    .contains_column_data(col_pos.column_idx, &datum.to_bytes())?;
                if exist {
                    // bloom filter has false positivity, that is to say we are unsure whether this
                    // value exists even if the bloom filter says it exists.
                    None
                } else {
                    Some(negated)
                }
            };

        equal::filter_row_groups(
            self.schema.clone(),
            self.predicates,
            self.row_groups.len(),
            is_equal,
        )
    }

    /// Compute the intersection of the two row groups which are in increasing
    /// order.
    fn intersect_filtered_row_groups(row_groups0: &[usize], row_groups1: &[usize]) -> Vec<usize> {
        let mut intersect = Vec::with_capacity(row_groups0.len().min(row_groups1.len()));

        let (mut i0, mut i1) = (0, 0);
        while i0 < row_groups0.len() && i1 < row_groups1.len() {
            let idx0 = row_groups0[i0];
            let idx1 = row_groups1[i1];

            match idx0.cmp(&idx1) {
                Ordering::Less => i0 += 1,
                Ordering::Greater => i1 += 1,
                Ordering::Equal => {
                    intersect.push(idx0);
                    i0 += 1;
                    i1 += 1;
                }
            }
        }

        intersect
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_intersect_row_groups() {
        let test_cases = vec![
            (vec![0, 1, 2, 3, 4], vec![0, 3, 4, 5], vec![0, 3, 4]),
            (vec![], vec![0, 3, 4, 5], vec![]),
            (vec![1, 2, 3], vec![4, 5, 6], vec![]),
            (vec![3], vec![1, 2, 3], vec![3]),
            (vec![4, 5, 6], vec![4, 6, 7], vec![4, 6]),
        ];

        for (row_groups0, row_groups1, expect_row_groups) in test_cases {
            let real_row_groups =
                RowGroupFilter::intersect_filtered_row_groups(&row_groups0, &row_groups1);
            assert_eq!(real_row_groups, expect_row_groups)
        }
    }
}
