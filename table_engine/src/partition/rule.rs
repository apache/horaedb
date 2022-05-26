// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

//! Partition rules

use common_types::{datum::Datum, row::Row, schema::Schema};
use common_util::define_result;
use smallvec::SmallVec;
use snafu::{ensure, Backtrace, ResultExt, Snafu};

use crate::partition::{expression::Expression, PartitionInfo, PartitionType};

const HASH_COLUMN_NUM: usize = 1;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("No column for hash partitioning.\nBacktrace:\n{}", backtrace))]
    NoColumnForHash { backtrace: Backtrace },

    #[snafu(display("Only support one hash column.\nBacktrace:\n{}", backtrace))]
    TooMuchHashColumn { backtrace: Backtrace },

    #[snafu(display("Failed to eval partition expr, err:{}", source))]
    EvalExpr {
        source: crate::partition::expression::Error,
    },
}

define_result!(Error);

/// Partition rule locate partition by input records
// TODO(yingwen): Recreate partition rule once the schema of the table is changed
#[derive(Debug)]
pub enum PartitionRule {
    None,
    Hash(HashPartitionRule),
}

impl PartitionRule {
    pub fn new(partition_info: &PartitionInfo, schema: &Schema) -> Result<Self> {
        match partition_info.partition_type {
            PartitionType::None => Ok(PartitionRule::None),
            PartitionType::Hash => {
                let rule = HashPartitionRule::new(partition_info, schema)?;
                Ok(PartitionRule::Hash(rule))
            }
        }
    }

    /// Return the index of partition
    pub fn locate_partition(&self, row: &Row) -> Result<usize> {
        match self {
            // Always return the first partition
            PartitionRule::None => Ok(0),
            PartitionRule::Hash(rule) => rule.eval_partition_index(row),
        }
    }
}

/// Partition rule based on hash
#[derive(Debug)]
pub struct HashPartitionRule {
    /// Total number of partitions
    partition_num: u16,
    /// Expression to evaluate a hash value
    expression: Expression,
    /// Offsets of columns for evaluate
    // TODO(yingwen): The column index may be invalid after schema change (add/del column)
    column_index: SmallVec<[usize; HASH_COLUMN_NUM]>,
}

impl HashPartitionRule {
    pub fn new(partition_info: &PartitionInfo, schema: &Schema) -> Result<Self> {
        let expr = Expression::new(partition_info);

        let col_name_list = expr.extract_column_name();
        let mut column_index = SmallVec::with_capacity(col_name_list.size_hint().0);
        for col_name in col_name_list {
            for (i, v) in schema.columns().iter().enumerate() {
                if col_name == v.name {
                    column_index.push(i);
                    break;
                }
            }
        }

        ensure!(!column_index.is_empty(), NoColumnForHash);
        ensure!(column_index.len() == 1, TooMuchHashColumn);

        Ok(Self {
            partition_num: partition_info.partition_num,
            expression: expr,
            column_index,
        })
    }

    // TODO(yingwen): Also pass schema?
    pub fn eval_partition_index(&self, row: &Row) -> Result<usize> {
        let mut col_vals: SmallVec<[&Datum; HASH_COLUMN_NUM]> =
            SmallVec::with_capacity(self.column_index.len());
        for i in &self.column_index {
            // TODO(yingwen): Check index?
            col_vals.push(&row[*i]);
        }
        let eval_uint = self.expression.eval_uint(&col_vals).context(EvalExpr)?;

        Ok((eval_uint % self.partition_num as u64) as usize)
    }
}
