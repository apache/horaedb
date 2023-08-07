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
