// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

// Row group pruner.

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

use super::meta_data::SstFilter;
use crate::sst::reader::error::{OtherNoCause, Result};

/// RowGroupPruner is used to prune row groups according to the provided
/// predicates and filters.
///
/// Currently, two kinds of filters will be applied to such filtering:
/// min max & sst_filter.
pub struct RowGroupPruner<'a> {
    schema: &'a SchemaRef,
    row_groups: &'a [RowGroupMetaData],
    sst_filter: Option<&'a SstFilter>,
    predicates: &'a [Expr],
}

impl<'a> RowGroupPruner<'a> {
    pub fn try_new(
        schema: &'a SchemaRef,
        row_groups: &'a [RowGroupMetaData],
        sst_filter: Option<&'a SstFilter>,
        predicates: &'a [Expr],
    ) -> Result<Self> {
        if let Some(f) = sst_filter {
            ensure!(f.len() == row_groups.len(), OtherNoCause {
                msg: format!("expect the same number of ss_filter as the number of row groups, num_sst_filters:{}, num_row_groups:{}", f.len(), row_groups.len()),
            });
        }

        Ok(Self {
            schema,
            row_groups,
            sst_filter,
            predicates,
        })
    }

    pub fn prune(&self) -> Vec<usize> {
        debug!(
            "Begin to prune row groups, total_row_groups:{}, sst_filter:{}, predicates:{:?}",
            self.row_groups.len(),
            self.sst_filter.is_some(),
            self.predicates,
        );

        let pruned0 = self.prune_by_min_max();
        match self.sst_filter {
            Some(v) => {
                // TODO: We can do continuous prune based on the `pruned0` to reduce the
                // filtering cost.
                let pruned1 = self.prune_by_filters(v);
                let pruned = Self::intersect_pruned_row_groups(&pruned0, &pruned1);

                debug!(
                    "Finish prune row groups by sst_filter and min_max, total_row_groups:{}, pruned_by_min_max:{}, pruned_by_blooms:{}, pruned_by_both:{}",
                    self.row_groups.len(),
                    pruned0.len(),
                    pruned1.len(),
                    pruned.len(),
                );

                pruned
            }
            None => {
                debug!(
                    "Finish pruning row groups by min_max, total_row_groups:{}, pruned_row_groups:{}",
                    self.row_groups.len(),
                    pruned0.len(),
                );
                pruned0
            }
        }
    }

    fn prune_by_min_max(&self) -> Vec<usize> {
        min_max::prune_row_groups(self.schema.clone(), self.predicates, self.row_groups)
    }

    /// Prune row groups according to the filter.
    fn prune_by_filters(&self, sst_filter: &SstFilter) -> Vec<usize> {
        let is_equal =
            |col_pos: ColumnPosition, val: &ScalarValue, negated: bool| -> Option<bool> {
                let datum = Datum::from_scalar_value(val)?;
                let exist = sst_filter[col_pos.row_group_idx]
                    .contains_column_data(col_pos.column_idx, &datum.to_bytes())?;
                if exist {
                    // sst_filter has false positivity, that is to say we are unsure whether this
                    // value exists even if the sst_filter says it exists.
                    None
                } else {
                    Some(negated)
                }
            };

        equal::prune_row_groups(
            self.schema.clone(),
            self.predicates,
            self.row_groups.len(),
            is_equal,
        )
    }

    /// Compute the intersection of the two row groups which are in increasing
    /// order.
    fn intersect_pruned_row_groups(row_groups0: &[usize], row_groups1: &[usize]) -> Vec<usize> {
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
                RowGroupPruner::intersect_pruned_row_groups(&row_groups0, &row_groups1);
            assert_eq!(real_row_groups, expect_row_groups)
        }
    }
}
