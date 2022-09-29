// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

//! Helper function and struct to find input columns for an Expr;

use datafusion::common::Result;
use datafusion_expr::{
    expr::Expr as DfLogicalExpr,
    expr_visitor::{ExprVisitable, ExpressionVisitor, Recursion},
};

#[derive(Default)]
struct ColumnCollector {
    /// columns used by the given expr
    columns: Vec<String>,
}

impl ExpressionVisitor for ColumnCollector {
    fn pre_visit(mut self, expr: &DfLogicalExpr) -> Result<Recursion<Self>>
    where
        Self: ExpressionVisitor,
    {
        if let DfLogicalExpr::Column(column) = expr {
            self.columns.push(column.name.clone())
        }
        Ok(Recursion::Continue(self))
    }
}

pub fn find_columns_by_expr(expr: &DfLogicalExpr) -> Vec<String> {
    let ColumnCollector { columns } = expr.accept(ColumnCollector::default()).unwrap();

    columns
}
