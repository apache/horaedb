// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

// Filter row groups with bloom filter.

use datafusion_expr::{binary_expr, lit, Expr, Operator};
use snafu::Snafu;

#[derive(Debug, Snafu)]
#[snafu(visibility = "pub")]
pub enum Error {}

define_result!(Error);

const MAX_ELEMS_IN_LIST_FOR_FILTER: usize = 100;

#[allow(unused)]
fn build_predicate_expression(expr: &Expr) -> Expr {
    // Returned for unsupported expressions. Such expressions are
    // converted to TRUE. This can still be useful when multiple
    // conditions are joined using AND such as: column > 10 AND TRUE
    let unhandled = lit(true);

    // predicate expression can only be a binary expression
    match expr {
        Expr::BinaryExpr { left, op, right } => match op {
            Operator::And | Operator::Or => {
                let left_expr = build_predicate_expression(left);
                let right_expr = build_predicate_expression(right);
                binary_expr(left_expr, *op, right_expr)
            }
            Operator::Eq => build_equal_expression(left, right, *op),
            Operator::NotEq => build_equal_expression(left, right, *op),
            _ => unhandled,
        },
        Expr::InList {
            expr,
            list,
            negated,
        } if !list.is_empty() && list.len() < MAX_ELEMS_IN_LIST_FOR_FILTER => {
            let eq_fun = if *negated { Expr::not_eq } else { Expr::eq };
            let re_fun = if *negated { Expr::and } else { Expr::or };
            let transformed_expr = list
                .iter()
                .map(|e| eq_fun(*expr.clone(), e.clone()))
                .reduce(re_fun)
                .unwrap();
            build_predicate_expression(&transformed_expr)
        }
        _ => unhandled,
    }
}

fn build_equal_expression(left: &Expr, right: &Expr, equal_op: Operator) -> Expr {
    let unhandled = lit(true);

    let (column_expr, scalar_value_expr) = match (left, right) {
        (Expr::Column(_), Expr::Literal(_)) => (left, right),
        (Expr::Literal(_), Expr::Column(_)) => (right, left),
        _ => return unhandled,
    };

    binary_expr(column_expr.clone(), equal_op, scalar_value_expr.clone())
}
