// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

//! Partition rules

use common_util::define_result;
use datafusion_expr::Expr;
use snafu::{ensure, Backtrace, Snafu};

use crate::partition::HashPartitionInfo;

const HASH_COLUMN_NUM: usize = 1;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("No column for hash partitioning.\nBacktrace:\n{}", backtrace))]
    NoColumnForHash { backtrace: Backtrace },

    #[snafu(display("Only support one hash column.\nBacktrace:\n{}", backtrace))]
    TooMuchHashColumn { backtrace: Backtrace },
}

define_result!(Error);

/// Partition rule locate partition by input records
pub trait PartitionRule {
    fn locate_partitions(&self, exprs: &[Expr]) -> Result<Vec<usize>>;
}

/// Partition rule based on hash
///
/// ```SQL
/// CREATE TABLE employees (
///     id INT NOT NULL,
///     fname VARCHAR(30)
/// )
/// PARTITION BY HASH(id)
/// PARTITIONS 4;
/// ```
///
/// Refer to https://dev.mysql.com/doc/refman/8.0/en/partitioning-hash.html to get more information.
// TODO: Support hash by expression such as `PARTITION BY HASH(col1 + col2)`.
#[derive(Debug)]
#[allow(dead_code)]
pub struct HashPartitionRule {
    /// Partition column
    columns: Vec<String>,
    /// Total number of partitions
    partition_num: u16,
}

impl PartitionRule for HashPartitionRule {
    /// Return the index of partition
    fn locate_partitions(&self, _exprs: &[Expr]) -> Result<Vec<usize>> {
        todo!()
    }
}

impl HashPartitionRule {
    pub fn new(partition_info: &HashPartitionInfo) -> Result<Self> {
        ensure!(!partition_info.columns.is_empty(), NoColumnForHash);
        ensure!(
            partition_info.columns.len() == HASH_COLUMN_NUM,
            TooMuchHashColumn
        );

        Ok(Self {
            columns: partition_info.columns.clone(),
            partition_num: partition_info.definitions.len() as u16,
        })
    }
}
