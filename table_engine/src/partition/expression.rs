// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

//! Partition expression

use std::ops::Deref;

use common_types::datum::Datum;
use common_util::define_result;
use snafu::{Backtrace, OptionExt, Snafu};

use crate::partition::PartitionInfo;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("No datums for eval.\nBacktrace:\n{}", backtrace))]
    EmptyDatums { backtrace: Backtrace },
}

define_result!(Error);

/// Partition expression
#[derive(Debug)]
pub enum Expression {
    ColumnExpr(ColumnExpr),
}

impl Expression {
    pub fn new(partition_info: &PartitionInfo) -> Self {
        Self::parse_expr(partition_info.expr.to_string())
    }

    /// Extract column name in expression
    pub fn extract_column_name(&self) -> impl Iterator<Item = &str> {
        match self {
            Expression::ColumnExpr(col_expr) => col_expr.extract_column_name(),
        }
    }

    fn parse_expr(expr_str: String) -> Expression {
        Expression::ColumnExpr(ColumnExpr::new(expr_str))
    }

    pub fn eval_uint<T: Deref<Target = Datum>>(&self, datums: &[T]) -> Result<u64> {
        match self {
            Expression::ColumnExpr(column_expr) => {
                column_expr.eval_uint(datums.get(0).context(EmptyDatums)?)
            }
        }
    }
}

/// Column
#[derive(Debug)]
pub struct ColumnExpr {
    column_name: String,
}

impl ColumnExpr {
    fn new(column_name: String) -> Self {
        Self { column_name }
    }

    fn extract_column_name(&self) -> impl Iterator<Item = &str> {
        std::iter::once(self.column_name.as_str())
    }

    // TODO: handle error
    fn eval_uint(&self, datum: &Datum) -> Result<u64> {
        Ok(datum.convert_to_uint64())
    }
}
