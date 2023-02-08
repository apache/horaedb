// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

use arrow::datatypes::SchemaRef;
use datafusion::{common::Column, scalar::ScalarValue};
use datafusion_expr::{self, Expr, Operator};

const MAX_ELEMS_IN_LIST_FOR_FILTER: usize = 100;

/// A position used to describe the location of a column in the row groups.
#[derive(Debug, Clone, Copy)]
pub struct ColumnPosition {
    pub row_group_idx: usize,
    pub column_idx: usize,
}

/// Filter the row groups according to the `exprs`.
///
/// The return value is the filtered row group indexes. And the `is_equal`
/// closure receive three parameters:
/// - The position of the column in the row groups;
/// - The value of the column used to determine equality;
/// - Whether this compare is negated;
/// And it should return the result of this comparison, and None denotes
/// unknown.
pub fn filter_row_groups<E>(
    schema: SchemaRef,
    exprs: &[Expr],
    num_row_groups: usize,
    is_equal: E,
) -> Vec<usize>
where
    E: Fn(ColumnPosition, &ScalarValue, bool) -> Option<bool>,
{
    let mut should_reads = vec![true; num_row_groups];
    for expr in exprs {
        let pruner = EqPruner::new(expr);
        for (row_group_idx, should_read) in should_reads.iter_mut().enumerate() {
            if !*should_read {
                continue;
            }

            let f = |column: &Column, val: &ScalarValue, negated: bool| -> bool {
                match schema.column_with_name(&column.name) {
                    Some((column_idx, _)) => {
                        let pos = ColumnPosition {
                            row_group_idx,
                            column_idx,
                        };
                        // Just set the result is true to ensure not to miss any possible row group
                        // if the caller has no idea of the compare result.
                        is_equal(pos, val, negated).unwrap_or(true)
                    }
                    _ => true,
                }
            };

            *should_read = pruner.prune(&f);
        }
    }

    should_reads
        .iter()
        .enumerate()
        .filter_map(|(row_group_idx, should_read)| {
            if *should_read {
                Some(row_group_idx)
            } else {
                None
            }
        })
        .collect()
}

/// A pruner based on (not)equal predicates, including in-list predicate.
#[derive(Debug, Clone)]
pub struct EqPruner {
    /// Normalized expression for pruning.
    normalized_expr: NormalizedExpr,
}

impl EqPruner {
    pub fn new(predicate_expr: &Expr) -> Self {
        Self {
            normalized_expr: normalize_predicate_expression(predicate_expr),
        }
    }

    /// Use the prune function provided by caller to finish pruning.
    ///
    /// The prune function receives three parameters:
    /// - the column to compare;
    /// - the value of the column used to determine equality;
    /// - Whether this compare is negated;
    pub fn prune<F>(&self, f: &F) -> bool
    where
        F: Fn(&Column, &ScalarValue, bool) -> bool,
    {
        self.normalized_expr.compute(f)
    }
}

/// The normalized expression based on [`datafusion_expr::Expr`].
///
/// It only includes these kinds of `And`, `Or`, `Eq`, `NotEq` and `True`.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
enum NormalizedExpr {
    And {
        left: Box<NormalizedExpr>,
        right: Box<NormalizedExpr>,
    },
    Or {
        left: Box<NormalizedExpr>,
        right: Box<NormalizedExpr>,
    },
    Eq {
        column: Column,
        value: ScalarValue,
    },
    NotEq {
        column: Column,
        value: ScalarValue,
    },
    True,
    False,
}

impl NormalizedExpr {
    fn boxed(self) -> Box<Self> {
        Box::new(self)
    }

    fn compute<F>(&self, f: &F) -> bool
    where
        F: Fn(&Column, &ScalarValue, bool) -> bool,
    {
        match self {
            NormalizedExpr::And { left, right } => left.compute(f) && right.compute(f),
            NormalizedExpr::Or { left, right } => left.compute(f) || right.compute(f),
            NormalizedExpr::Eq { column, value } => f(column, value, false),
            NormalizedExpr::NotEq { column, value } => f(column, value, true),
            NormalizedExpr::True => true,
            NormalizedExpr::False => false,
        }
    }
}

fn normalize_predicate_expression(expr: &Expr) -> NormalizedExpr {
    // Returned for unsupported expressions, which are converted to TRUE.
    let unhandled = NormalizedExpr::True;

    match expr {
        Expr::BinaryExpr(datafusion_expr::BinaryExpr { left, op, right }) => match op {
            Operator::And => {
                let left = normalize_predicate_expression(left);
                let right = normalize_predicate_expression(right);
                NormalizedExpr::And {
                    left: left.boxed(),
                    right: right.boxed(),
                }
            }
            Operator::Or => {
                let left = normalize_predicate_expression(left);
                let right = normalize_predicate_expression(right);
                NormalizedExpr::Or {
                    left: left.boxed(),
                    right: right.boxed(),
                }
            }
            Operator::Eq => normalize_equal_expr(left, right, true),
            Operator::NotEq => normalize_equal_expr(left, right, false),
            _ => unhandled,
        },
        Expr::InList {
            expr,
            list,
            negated,
        } if list.len() < MAX_ELEMS_IN_LIST_FOR_FILTER => {
            if list.is_empty() {
                if *negated {
                    // "not in empty list" is always true
                    NormalizedExpr::True
                } else {
                    // "in empty list" is always false
                    NormalizedExpr::False
                }
            } else {
                let eq_fun = if *negated { Expr::not_eq } else { Expr::eq };
                let re_fun = if *negated { Expr::and } else { Expr::or };
                let transformed_expr = list
                    .iter()
                    .map(|e| eq_fun(*expr.clone(), e.clone()))
                    .reduce(re_fun)
                    .unwrap();
                normalize_predicate_expression(&transformed_expr)
            }
        }
        _ => unhandled,
    }
}

/// Normalize the equal expr as: `column = value` or `column != value`.
///
/// Return [`NormalizedExpr::True`] if it can't be normalized.
fn normalize_equal_expr(left: &Expr, right: &Expr, is_equal: bool) -> NormalizedExpr {
    let (column, value) = match (left, right) {
        (Expr::Column(col), Expr::Literal(val)) => (col, val),
        (Expr::Literal(val), Expr::Column(col)) => (col, val),
        _ => return NormalizedExpr::True,
    };
    let (column, value) = (column.clone(), value.clone());
    if is_equal {
        NormalizedExpr::Eq { column, value }
    } else {
        NormalizedExpr::NotEq { column, value }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::datatypes::{DataType, Field, Schema};

    use super::*;

    fn make_column_expr(name: &str) -> Expr {
        Expr::Column(make_column(name))
    }

    fn make_literal_expr(val: i32) -> Expr {
        Expr::Literal(make_scalar_value(val))
    }

    fn make_column(name: &str) -> Column {
        Column {
            relation: None,
            name: name.to_string(),
        }
    }

    fn make_scalar_value(val: i32) -> ScalarValue {
        ScalarValue::from(val)
    }

    fn make_normalized_eq_expr(column: &str, val: i32) -> Box<NormalizedExpr> {
        NormalizedExpr::Eq {
            column: make_column(column),
            value: make_scalar_value(val),
        }
        .boxed()
    }

    fn make_normalized_not_eq_expr(column: &str, val: i32) -> Box<NormalizedExpr> {
        NormalizedExpr::NotEq {
            column: make_column(column),
            value: make_scalar_value(val),
        }
        .boxed()
    }

    fn check_normalize(expr: &Expr, expect_expr: &NormalizedExpr) {
        let normalized_expr = normalize_predicate_expression(expr);
        assert_eq!(&normalized_expr, expect_expr);
    }

    #[test]
    fn test_normalize_and() {
        let expr = Expr::and(
            Expr::eq(make_column_expr("c0"), make_literal_expr(0)),
            Expr::not_eq(make_column_expr("c1"), make_literal_expr(0)),
        );
        let expect_expr = NormalizedExpr::And {
            left: make_normalized_eq_expr("c0", 0),
            right: make_normalized_not_eq_expr("c1", 0),
        };

        check_normalize(&expr, &expect_expr);
    }

    #[test]
    fn test_normalize_or() {
        let expr = Expr::or(
            Expr::eq(make_column_expr("c0"), make_literal_expr(0)),
            Expr::not_eq(make_column_expr("c1"), make_literal_expr(0)),
        );
        let expect_expr = NormalizedExpr::Or {
            left: make_normalized_eq_expr("c0", 0),
            right: make_normalized_not_eq_expr("c1", 0),
        };

        check_normalize(&expr, &expect_expr);
    }

    #[test]
    fn test_normalize_inlist() {
        let equal_list_expr = Expr::in_list(
            make_column_expr("c0"),
            vec![make_literal_expr(0), make_literal_expr(1)],
            false,
        );

        let expect_equal_expr = NormalizedExpr::Or {
            left: make_normalized_eq_expr("c0", 0),
            right: make_normalized_eq_expr("c0", 1),
        };
        check_normalize(&equal_list_expr, &expect_equal_expr);

        let not_equal_list_expr = Expr::in_list(
            make_column_expr("c0"),
            vec![make_literal_expr(0), make_literal_expr(1)],
            true,
        );

        let expect_not_equal_expr = NormalizedExpr::And {
            left: make_normalized_not_eq_expr("c0", 0),
            right: make_normalized_not_eq_expr("c0", 1),
        };
        check_normalize(&not_equal_list_expr, &expect_not_equal_expr);
    }

    #[test]
    fn test_normalize_in_empty_list() {
        let empty_list_expr = Expr::in_list(make_column_expr("c0"), vec![], false);
        check_normalize(&empty_list_expr, &NormalizedExpr::False);

        let negated_empty_list_expr = Expr::in_list(make_column_expr("c0"), vec![], true);
        check_normalize(&negated_empty_list_expr, &NormalizedExpr::True);
    }

    #[test]
    fn test_normalize_complex() {
        // (c0 in [0, 1]) or ((c1 != 0 or c2 = 1 ) and not c3))
        let expr = Expr::or(
            Expr::in_list(
                make_column_expr("c0"),
                vec![make_literal_expr(0), make_literal_expr(1)],
                false,
            ),
            Expr::and(
                Expr::or(
                    Expr::not_eq(make_literal_expr(0), make_column_expr("c1")),
                    Expr::eq(make_literal_expr(1), make_column_expr("c2")),
                ),
                Expr::not(make_column_expr("c3")),
            ),
        );

        // (c0 = 0 or c0 = 1) or ((c1 != 0 or c2 = 1) and true)
        let expect_expr = NormalizedExpr::Or {
            left: NormalizedExpr::Or {
                left: make_normalized_eq_expr("c0", 0),
                right: make_normalized_eq_expr("c0", 1),
            }
            .boxed(),
            right: NormalizedExpr::And {
                left: NormalizedExpr::Or {
                    left: make_normalized_not_eq_expr("c1", 0),
                    right: make_normalized_eq_expr("c2", 1),
                }
                .boxed(),
                right: NormalizedExpr::True.boxed(),
            }
            .boxed(),
        };

        check_normalize(&expr, &expect_expr)
    }

    #[test]
    fn test_normalize_unhandled() {
        let lt_expr = Expr::gt(make_column_expr("c0"), make_literal_expr(0));
        let empty_list_expr = Expr::in_list(make_column_expr("c0"), vec![], true);
        let not_expr = Expr::not(make_column_expr("c0"));

        let unhandled_exprs = vec![lt_expr, empty_list_expr, not_expr];
        let expect_expr = NormalizedExpr::True;
        for expr in &unhandled_exprs {
            check_normalize(expr, &expect_expr);
        }
    }

    #[test]
    fn test_prune() {
        let f = |column: &Column, val: &ScalarValue, negated: bool| -> bool {
            let val = match val {
                ScalarValue::Int32(v) => v.unwrap(),
                _ => panic!("Unexpected value type"),
            };

            let res = match column.name.as_str() {
                "c0" => val == 0,
                "c1" => val == 1,
                "c2" => val == 2,
                _ => panic!("Unexpected column"),
            };
            if negated {
                !res
            } else {
                res
            }
        };

        // (c0 in [0, 1]) or ((c1 != 0 or c2 = 1 ) and not c3))
        let true_expr = Expr::or(
            Expr::in_list(
                make_column_expr("c0"),
                vec![make_literal_expr(0), make_literal_expr(1)],
                false,
            ),
            Expr::and(
                Expr::or(
                    Expr::not_eq(make_literal_expr(0), make_column_expr("c1")),
                    Expr::eq(make_literal_expr(1), make_column_expr("c2")),
                ),
                Expr::not(make_column_expr("c3")),
            ),
        );
        assert!(EqPruner::new(&true_expr).prune(&f));

        // (c0 in [2, 3]) or (c1 != 0 and c2 = 1)
        let false_expr = Expr::or(
            Expr::in_list(
                make_column_expr("c0"),
                vec![make_literal_expr(2), make_literal_expr(3)],
                false,
            ),
            Expr::and(
                Expr::not_eq(make_literal_expr(0), make_column_expr("c1")),
                Expr::eq(make_literal_expr(1), make_column_expr("c2")),
            ),
        );
        assert!(!EqPruner::new(&false_expr).prune(&f));
    }

    #[test]
    fn test_filter_row_groups() {
        // Provide three row groups (one row in one row group).
        // | c0 | c1 | c2 |
        // | 0  | 1  | 2  |
        // | 1  | 2  | 3  |
        // | 2  | 3  | 4  |
        let row_groups = vec![vec![0, 1, 2], vec![1, 2, 3], vec![2, 3, 4]];
        let is_equal = |pos: ColumnPosition, val: &ScalarValue, negated: bool| -> Option<bool> {
            let expect_val = row_groups[pos.row_group_idx][pos.column_idx];
            let val = if let ScalarValue::Int32(v) = val {
                v.expect("Unexpected value")
            } else {
                panic!("Unexpected value type")
            };

            if negated {
                Some(expect_val != val)
            } else {
                Some(expect_val == val)
            }
        };

        // (c0 in [1, 3]) or c1 not in [1, 2]
        let predicate1 = Expr::or(
            Expr::in_list(
                make_column_expr("c0"),
                vec![make_literal_expr(1), make_literal_expr(3)],
                false,
            ),
            Expr::in_list(
                make_column_expr("c1"),
                vec![make_literal_expr(1), make_literal_expr(2)],
                true,
            ),
        );

        // c2 != 2
        let predicate2 = Expr::not_eq(make_literal_expr(2), make_column_expr("c2"));

        let schema = Schema::new(vec![
            Field::new("c0", DataType::Int32, false),
            Field::new("c1", DataType::Int32, false),
            Field::new("c2", DataType::Int32, false),
        ]);
        let filtered_row_groups =
            filter_row_groups(Arc::new(schema), &vec![predicate1, predicate2], 3, is_equal);

        assert_eq!(vec![1, 2], filtered_row_groups)
    }
}
