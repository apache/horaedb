// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

//! Partition filter extractor

use std::collections::HashSet;

use common_types::datum::Datum;
use datafusion_expr::{Expr, Operator};
use df_operator::visitor::find_columns_by_expr;

use crate::partition::rule::filter::{PartitionCondition, PartitionFilter};

/// The datafusion filter exprs extractor
///
/// It's used to extract the meaningful `Expr`s and convert them to
/// [PartitionFilter](the inner filter type in ceresdb).
///
/// NOTICE: When you implements [PartitionRule] for specific partition strategy,
/// you should implement the corresponding [FilterExtractor], too.
///
/// For example: [KeyRule] and [KeyExtractor].
/// If they are not related, [PartitionRule] may not take effect.
pub trait FilterExtractor: Send + Sync + 'static {
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

            // If target columns included, now only the situation that only target column in
            // filter is supported. Once other type column found here, we ignore it.
            // TODO: support above situation.
            if columns_in_filter.len() != 1 {
                continue;
            }

            // Finally, we try to convert `filter` to `PartitionFilter`.
            // We just support the simple situation: "colum = value" now.
            // TODO: support "colum in [value list]".
            // TODO: we need to compare and check the datatype of column and value.
            // (Actually, there is type conversion on high-level, but when converted data
            // is overflow, it may take no effect).
            let partition_filter = match filter.clone() {
                Expr::BinaryExpr(datafusion_expr::BinaryExpr { left, op, right }) => {
                    match (*left, op, *right) {
                        (Expr::Column(col), Operator::Eq, Expr::Literal(val))
                        | (Expr::Literal(val), Operator::Eq, Expr::Column(col)) => {
                            let datum_opt = Datum::from_scalar_value(&val);
                            datum_opt.map(|datum| {
                                PartitionFilter::new(col.name, PartitionCondition::Eq(datum))
                            })
                        }
                        _ => None,
                    }
                }
                Expr::InList {
                    expr,
                    list,
                    negated,
                } => {
                    if let (Expr::Column(col), list, false) = (*expr, list, negated) {
                        let mut datums = Vec::with_capacity(list.len());
                        for entry in list {
                            if let Expr::Literal(val) = entry {
                                let datum_opt = Datum::from_scalar_value(&val);
                                if let Some(datum) = datum_opt {
                                    datums.push(datum)
                                }
                            }
                        }
                        if datums.is_empty() {
                            None
                        } else {
                            Some(PartitionFilter::new(
                                col.name,
                                PartitionCondition::In(datums),
                            ))
                        }
                    } else {
                        None
                    }
                }
                _ => None,
            };

            if let Some(pf) = partition_filter {
                target.push(pf);
            }
        }

        target
    }
}

pub type FilterExtractorRef = Box<dyn FilterExtractor>;

#[cfg(test)]
mod tests {
    use datafusion::scalar::ScalarValue;
    use datafusion_expr::{col, Expr::Literal};

    use super::*;

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

    #[test]
    fn test_key_extractor_in_list_filter() {
        let extractor = KeyExtractor;

        let columns = vec!["col1".to_string()];
        let accepted_expr = col("col1").in_list(
            vec![
                Literal(ScalarValue::Int32(Some(42))),
                Literal(ScalarValue::Int32(Some(38))),
            ],
            false,
        );
        let partition_filter = extractor.extract(&[accepted_expr], &columns);
        let expected = PartitionFilter {
            column: "col1".to_string(),
            condition: PartitionCondition::In(vec![Datum::Int32(42), Datum::Int32(38)]),
        };
        assert_eq!(partition_filter.get(0).unwrap(), &expected);
    }

    #[test]
    fn test_key_extractor_in_list_filter_with_negated() {
        let extractor = KeyExtractor;

        let columns = vec!["col1".to_string()];
        let accepted_expr = col("col1").in_list(
            vec![
                Literal(ScalarValue::Int32(Some(42))),
                Literal(ScalarValue::Int32(Some(38))),
            ],
            true,
        );
        let partition_filter = extractor.extract(&[accepted_expr], &columns);
        assert!(partition_filter.is_empty())
    }
}
