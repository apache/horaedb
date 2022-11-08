// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

// Filter for row groups.

use std::cmp::Ordering;

use arrow::datatypes::SchemaRef;
use common_types::datum::Datum;
use datafusion::{prelude::Expr, scalar::ScalarValue};
use ethbloom::{Bloom, Input};
use parquet::file::metadata::RowGroupMetaData;
use parquet_ext::prune::{
    equal::{self, ColumnPosition},
    min_max,
};
use snafu::ensure;

use crate::sst::reader::error::{OtherNoCause, Result};

pub struct RowGroupFilter<'a> {
    schema: &'a SchemaRef,
    row_groups: &'a [RowGroupMetaData],
    blooms: &'a [Vec<Bloom>],
    predicates: &'a [Expr],
}

impl<'a> RowGroupFilter<'a> {
    pub fn try_new(
        schema: &'a SchemaRef,
        row_groups: &'a [RowGroupMetaData],
        blooms: &'a [Vec<Bloom>],
        predicates: &'a [Expr],
    ) -> Result<Self> {
        ensure!(blooms.len() == row_groups.len(), OtherNoCause {
            msg: format!("expect the same number of bloom filter as the number of row groups, num_bloom_filters:{}, num_row_groups:{}", blooms.len(), row_groups.len()),
        });

        Ok(Self {
            schema,
            row_groups,
            blooms,
            predicates,
        })
    }

    pub fn filter(&self) -> Vec<usize> {
        let filtered0 = self.filter_by_min_max();
        let filtered1 = self.filter_by_bloom();
        Self::intersect_filtered_row_groups(&filtered0, &filtered1)
    }

    fn filter_by_min_max(&self) -> Vec<usize> {
        min_max::filter_row_groups(self.schema.clone(), self.predicates, self.row_groups)
    }

    fn filter_by_bloom(&self) -> Vec<usize> {
        let is_equal =
            |col_pos: ColumnPosition, val: &ScalarValue, negated: bool| -> Option<bool> {
                let datum = Datum::from_scalar_value(val)?;
                let col_bloom = self
                    .blooms
                    .get(col_pos.row_group_idx)?
                    .get(col_pos.column_idx)?;
                let exist = col_bloom.contains_input(Input::Raw(&datum.to_bytes()));
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

    /// Compute the intersection of the two row groups.
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
