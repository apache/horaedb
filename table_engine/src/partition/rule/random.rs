// Copyright 2023 The CeresDB Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Random partition rule

use common_types::row::RowGroup;
use itertools::Itertools;

use crate::partition::{rule::PartitionRule, Result};

pub struct RandomRule {
    pub partition_num: usize,
}

impl PartitionRule for RandomRule {
    fn columns(&self) -> Vec<String> {
        vec![]
    }

    fn locate_partitions_for_write(&self, _row_group: &RowGroup) -> Result<Vec<usize>> {
        let value: usize = rand::random();
        Ok(vec![value % self.partition_num])
    }

    fn locate_partitions_for_read(
        &self,
        _filters: &[super::filter::PartitionFilter],
    ) -> Result<Vec<usize>> {
        Ok((0..self.partition_num).collect_vec())
    }
}
