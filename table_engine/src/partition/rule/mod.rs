// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

//! Partition rules

pub mod extractor;
pub mod filter;
pub mod key;
pub mod mock;

use common_types::{datum::DatumKind, row::RowGroup};

use self::filter::PartitionFilter;
use crate::partition::Result;

/// Partition rule locate partition
pub trait PartitionRule {
    fn columns(&self) -> Vec<String>;

    /// Locate the partition for each row in `row_group`.
    ///
    /// Len of returned value should be equal to the one of rows in `row group`.
    fn locate_partitions_for_write(&self, row_group: &RowGroup) -> Result<Vec<usize>>;

    /// Locate partitions according to `filters`.
    fn locate_partitions_for_read(&self, filters: &[PartitionFilter]) -> Result<Vec<usize>>;
}

#[allow(dead_code)]
#[derive(Debug)]
pub struct ColumnWithType {
    column: String,
    datum_type: DatumKind,
}
