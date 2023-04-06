// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

//! Helper function and struct to find input columns for an Expr;

// use datafusion::common::{
//     tree_node::{TreeNodeVisitor, VisitRecursion},
//     Result,
// };
use datafusion_expr::expr::Expr as DfLogicalExpr;

// #[derive(Default)]
// struct ColumnCollector {
//     /// columns used by the given expr
//     columns: Vec<String>,
// }

// impl TreeNodeVisitor for ColumnCollector {
//     type N = DfLogicalExpr;

//     fn pre_visit(&mut self, expr: &DfLogicalExpr) -> Result<VisitRecursion> {
//         if let DfLogicalExpr::Column(column) = expr {
//             self.columns.push(column.name.clone())
//         }
//         Ok(VisitRecursion::Continue)
//     }
// }

pub fn find_columns_by_expr(expr: &DfLogicalExpr) -> Vec<String> {
    expr.to_columns()
        .unwrap()
        .into_iter()
        .map(|col| col.name)
        .collect()
}
