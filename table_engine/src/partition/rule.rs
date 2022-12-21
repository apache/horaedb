// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

//! Partition rules

use common_types::row::RowGroup;
use common_util::define_result;
use datafusion_expr::{Expr, Operator};
use snafu::Snafu;

#[derive(Debug, Snafu)]
pub enum Error {}

define_result!(Error);

/// Partition rule locate partition
pub trait PartitionRule {
    /// Locate the partition for each row in `row_group`.
    ///
    /// Len of returned value should be equal to the one of rows in `row group`.
    fn locate_partitions_for_write(&self, row_group: &RowGroup) -> Result<Vec<usize>>;

    /// Locate partitions according to `filters`.
    fn locate_partitions_for_read(&self, filters: &[PartitionFilter]) -> Result<Vec<usize>>;
}

/// Filter using for partition
///
/// Now, it is same as the `BinaryExpr`in datafusion.
#[allow(dead_code)]
pub struct PartitionFilter {
    /// Left-hand side of the expression
    left: Box<Expr>,
    /// The comparison operator
    op: Operator,
    /// Right-hand side of the expression
    right: Box<Expr>,
}
