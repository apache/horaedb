// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

//! Partitioned table supports

mod expression;
pub mod rule;

/// Partition type of table
#[derive(Clone, Debug, PartialEq)]
pub enum PartitionType {
    None = 0,
    Hash = 1,
}

/// Size type of partition num
pub type PartitionNum = u16;

/// Info for how to partition table
#[derive(Debug, Clone)]
pub struct PartitionInfo {
    /// Partition type
    pub partition_type: PartitionType,
    /// Partition expression
    pub expr: String,
    /// Partition num
    pub partition_num: PartitionNum,
}
