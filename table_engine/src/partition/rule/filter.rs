// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

//! Partition filter

use common_types::datum::Datum;

/// Filter using for partition
///
/// Now, it is same as the `BinaryExpr`in datafusion.
#[allow(dead_code)]
#[derive(Debug, Clone, PartialEq)]
pub enum PartitionCondition {
    /// Expressions are equal
    Eq(Datum),
    /// IN Expressions
    In(Vec<Datum>),
    /// Left side is smaller than right side
    Lt(Datum),
    /// Left side is smaller or equal to right side
    LtEq(Datum),
    /// Left side is greater than right side
    Gt(Datum),
    /// Left side is greater or equal to right side
    GtEq(Datum),
}

#[derive(Debug, Clone, PartialEq)]
pub struct PartitionFilter {
    pub column: String,
    pub condition: PartitionCondition,
}

impl PartitionFilter {
    pub fn new(column: String, condition: PartitionCondition) -> Self {
        Self { column, condition }
    }
}
