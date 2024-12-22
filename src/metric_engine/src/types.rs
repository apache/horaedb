// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use std::{
    fmt,
    ops::{Add, Deref, Range},
    sync::Arc,
    time::Duration,
};

use arrow_schema::SchemaRef;
use object_store::ObjectStore;
use tokio::runtime::Runtime;

use crate::sst::FileId;

// Seq column is a builtin column, and it will be appended to the end of
// user-defined schema.
pub const SEQ_COLUMN_NAME: &str = "__seq__";

pub type RuntimeRef = Arc<Runtime>;

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Timestamp(pub i64);

impl Add for Timestamp {
    type Output = Self;

    fn add(self, rhs: Self) -> Self::Output {
        Self(self.0 + rhs.0)
    }
}

impl Add<i64> for Timestamp {
    type Output = Self;

    fn add(self, rhs: i64) -> Self::Output {
        Self(self.0 + rhs)
    }
}

impl From<i64> for Timestamp {
    fn from(value: i64) -> Self {
        Self(value)
    }
}

impl Deref for Timestamp {
    type Target = i64;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl Timestamp {
    pub const MAX: Timestamp = Timestamp(i64::MAX);
    pub const MIN: Timestamp = Timestamp(i64::MIN);

    pub fn truncate_by(&self, duration: Duration) -> Self {
        let duration_millis = duration.as_millis() as i64;
        Timestamp(self.0 / duration_millis * duration_millis)
    }
}

#[derive(Clone, PartialEq, Eq)]
pub struct TimeRange(Range<Timestamp>);

impl fmt::Debug for TimeRange {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "[{}, {})", self.0.start.0, self.0.end.0)
    }
}

impl From<Range<Timestamp>> for TimeRange {
    fn from(value: Range<Timestamp>) -> Self {
        Self(value)
    }
}

impl From<Range<i64>> for TimeRange {
    fn from(value: Range<i64>) -> Self {
        Self(Range {
            start: value.start.into(),
            end: value.end.into(),
        })
    }
}

impl Deref for TimeRange {
    type Target = Range<Timestamp>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl TimeRange {
    pub fn new(start: Timestamp, end: Timestamp) -> Self {
        Self(start..end)
    }

    pub fn overlaps(&self, other: &TimeRange) -> bool {
        self.0.start < other.0.end && other.0.start < self.0.end
    }

    pub fn merge(&mut self, other: &TimeRange) {
        self.0.start = self.0.start.min(other.0.start);
        self.0.end = self.0.end.max(other.0.end);
    }
}

pub type ObjectStoreRef = Arc<dyn ObjectStore>;

pub struct WriteResult {
    pub id: FileId,
    pub seq: u64,
    pub size: usize,
}

#[derive(Debug, Default, Copy, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub enum UpdateMode {
    #[default]
    Overwrite,
    Append,
}

#[derive(Debug, Clone)]
pub struct StorageSchema {
    pub arrow_schema: SchemaRef,
    pub num_primary_keys: usize,
    pub seq_idx: usize,
    pub value_idxes: Vec<usize>,
    pub update_mode: UpdateMode,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_timestamp_truncate_by() {
        let testcases = [
            // ts, segment, expected
            (0, 20, 0),
            (10, 20, 0),
            (20, 20, 20),
            (30, 20, 20),
            (40, 20, 40),
            (41, 20, 40),
        ];
        for (ts, segment, expected) in testcases {
            let actual = Timestamp::from(ts).truncate_by(Duration::from_millis(segment));
            assert_eq!(actual.0, expected);
        }
    }
}
