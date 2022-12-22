// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

//! Mock partition rule

use common_types::row::RowGroup;

use crate::partition::rule::{PartitionFilter, PartitionRule, Result};

pub struct MockRule {
    pub wanted: usize,
}

impl PartitionRule for MockRule {
    fn locate_partitions_for_write(&self, row_group: &RowGroup) -> Result<Vec<usize>> {
        Ok(vec![self.wanted; row_group.num_rows()])
    }

    fn locate_partitions_for_read(&self, _filters: &[PartitionFilter]) -> Result<Vec<usize>> {
        Ok(vec![self.wanted])
    }
}
