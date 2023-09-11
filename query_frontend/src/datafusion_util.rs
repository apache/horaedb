use common_types::{schema::TSID_COLUMN, time::TimeRange};
use datafusion::{
    logical_expr::Between,
    prelude::{col, lit, Expr},
};

pub fn timerange_to_expr(query_range: &TimeRange, column_name: &str) -> Expr {
    Expr::Between(Between {
        expr: Box::new(col(column_name)),
        negated: false,
        low: Box::new(lit(query_range.inclusive_start().as_i64())),
        high: Box::new(lit(query_range.exclusive_end().as_i64() - 1)),
    })
}

pub fn default_sort_exprs(timestamp_column: &str) -> Vec<Expr> {
    vec![
        col(TSID_COLUMN).sort(true, true),
        col(timestamp_column).sort(true, true),
    ]
}
