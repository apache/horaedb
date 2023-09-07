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

//! Partition rules

pub mod df_adapter;
mod factory;
mod filter;
mod key;
mod random;

use common_types::{datum::DatumKind, row::RowGroup};

use self::filter::PartitionFilter;
use crate::partition::Result;

/// Partition rule locate partition
pub trait PartitionRule: Send + Sync + 'static {
    fn columns(&self) -> Vec<String>;

    /// Locate the partition for each row in `row_group`.
    ///
    /// Len of returned value should be equal to the one of rows in `row group`.
    fn locate_partitions_for_write(&self, row_group: &RowGroup) -> Result<Vec<usize>>;

    /// Locate partitions according to `filters`.
    ///
    /// NOTICE: Exprs which are useless for partitioning in specific partition
    /// strategy will be considered to have been filtered by corresponding
    /// [Extractor].
    ///
    /// For example:
    ///     In key partition, only filters like "a = 1", "a in [1,2,3]" can be
    /// passed here.
    ///
    /// If unexpected filters still found, all partitions will be returned.
    fn locate_partitions_for_read(&self, filters: &[PartitionFilter]) -> Result<Vec<usize>>;
}

#[allow(dead_code)]
#[derive(Debug)]
pub struct ColumnWithType {
    column: String,
    datum_type: DatumKind,
}

impl ColumnWithType {
    pub fn new(column: String, datum_type: DatumKind) -> Self {
        Self { column, datum_type }
    }
}

pub type PartitionRuleRef = Box<dyn PartitionRule>;
