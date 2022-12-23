// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

//! Partition filter extractor

use std::collections::HashSet;

use common_types::datum::Datum;
use datafusion_expr::{Expr, Operator};
use df_operator::visitor::find_columns_by_expr;

use super::filter::{PartitionCondition, PartitionFilter};

pub trait FilterExtractor {
    fn extract(&self, filters: &[Expr], columns: &[String]) -> Vec<PartitionFilter>;
}
pub struct KeyExtractor;

impl FilterExtractor for KeyExtractor {
    fn extract(&self, filters: &[Expr], columns: &[String]) -> Vec<PartitionFilter> {
        if filters.is_empty() {
            return Vec::default();
        }

        let mut target = Vec::with_capacity(filters.len());
        for filter in filters {
            // If no target columns included in `filter`, ignore this `filter`.
            let columns_in_filter = find_columns_by_expr(filter)
                .into_iter()
                .collect::<HashSet<_>>();
            let find_result = columns
                .iter()
                .find(|col| columns_in_filter.contains(col.as_str()));

            if find_result.is_none() {
                continue;
            }

            // If target columns included, now only the situation that only targe column in
            // filter is supported. Once other type column found here, we stop
            // our scanning. TODO: targe
            if columns_in_filter.len() != 1 {
                return Vec::default();
            }

            // Finally, we try to convert `filter` to `PartitionFilter`.
            // We just support the simple situation: "colum = value" now.
            // TODO: support "colum in [value list]".
            let partition_filter = match filter.clone() {
                Expr::BinaryExpr { left, op, right } => match (*left, op, *right) {
                    (Expr::Column(col), Operator::Eq, Expr::Literal(val))
                    | (Expr::Literal(val), Operator::Eq, Expr::Column(col)) => {
                        let datum_opt = Datum::from_scalar_value(&val);
                        datum_opt.map(|d| PartitionFilter::new(col.name, PartitionCondition::Eq(d)))
                    }
                    _ => None,
                },
                _ => None,
            };

            match partition_filter {
                Some(pf) => target.push(pf),
                None => return Vec::default(),
            }
        }

        target
    }
}

#[cfg(test)]
mod tests {
    use datafusion::scalar::ScalarValue;
    use datafusion_expr::col;

    use super::{FilterExtractor, *};

    #[test]
    fn test_key_extractor_basic() {
        let extractor = KeyExtractor;

        // `Eq` expr will be accepted.
        let columns = vec!["col1".to_string()];
        let accepted_expr = col("col1").eq(Expr::Literal(ScalarValue::Int32(Some(42))));
        let partition_filter = extractor.extract(&[accepted_expr], &columns);
        let expected = PartitionFilter {
            column: "col1".to_string(),
            condition: PartitionCondition::Eq(Datum::Int32(42)),
        };
        assert_eq!(partition_filter.get(0).unwrap(), &expected);

        // Other expr will be rejected now.
        let rejected_expr = col("col1").gt(Expr::Literal(ScalarValue::Int32(Some(42))));
        let partition_filter = extractor.extract(&[rejected_expr], &columns);
        assert!(partition_filter.is_empty());
    }
}
