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

use anyhow::Context;
use arrow::{
    array::{RecordBatch, UInt64Array},
    datatypes::{DataType, Field, FieldRef, Schema, SchemaRef},
};
use object_store::ObjectStore;
use tokio::runtime::Runtime;

use crate::{config::UpdateMode, ensure, sst::FileId, Result};

pub const BUILTIN_COLUMN_NUM: usize = 2;
/// Seq column is a builtin column, and it will be appended to the end of
/// user-defined schema.
pub const SEQ_COLUMN_NAME: &str = "__seq__";
/// This column is reserved for internal use, and it can be used to store
/// tombstone/expiration bit-flags.
pub const RESERVED_COLUMN_NAME: &str = "__reserved__";

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

/// The schema is like:
/// ```plaintext
/// primary_key1, primary_key2, ..., primary_keyN, value1, value2, ..., valueM, seq, reserved
/// ```
/// seq and reserved are builtin columns, and they will be appended to the end
/// of the original schema.
#[derive(Debug, Clone)]
pub struct StorageSchema {
    pub arrow_schema: SchemaRef,
    pub num_primary_keys: usize,
    pub seq_idx: usize,
    pub reserved_idx: usize,
    pub value_idxes: Vec<usize>,
    pub update_mode: UpdateMode,
}

impl StorageSchema {
    pub fn try_new(
        arrow_schema: SchemaRef,
        num_primary_keys: usize,
        update_mode: UpdateMode,
    ) -> Result<Self> {
        ensure!(num_primary_keys > 0, "num_primary_keys should large than 0");

        let fields = arrow_schema.fields();
        ensure!(
            !fields.iter().any(Self::is_builtin_field),
            "schema should not use builtin columns name"
        );

        let value_idxes = (num_primary_keys..arrow_schema.fields.len()).collect::<Vec<_>>();
        ensure!(!value_idxes.is_empty(), "no value column found");

        let mut new_fields = arrow_schema.fields().clone().to_vec();
        new_fields.extend_from_slice(&[
            Arc::new(Field::new(SEQ_COLUMN_NAME, DataType::UInt64, true)),
            Arc::new(Field::new(RESERVED_COLUMN_NAME, DataType::UInt64, true)),
        ]);
        let seq_idx = new_fields.len() - 2;
        let reserved_idx = new_fields.len() - 1;

        let arrow_schema = Arc::new(Schema::new_with_metadata(
            new_fields,
            arrow_schema.metadata.clone(),
        ));
        Ok(Self {
            arrow_schema,
            num_primary_keys,
            seq_idx,
            reserved_idx,
            value_idxes,
            update_mode,
        })
    }

    pub fn is_builtin_field(f: &FieldRef) -> bool {
        f.name() == SEQ_COLUMN_NAME || f.name() == RESERVED_COLUMN_NAME
    }

    /// Primary keys and builtin columns are required when query.
    pub fn fill_required_projections(&self, projection: &mut Option<Vec<usize>>) {
        if let Some(proj) = projection.as_mut() {
            for i in 0..self.num_primary_keys {
                if !proj.contains(&i) {
                    proj.push(i);
                }
            }
            // For builtin columns, reserved column is not used for now,
            // so only add seq column.
            if !proj.contains(&self.seq_idx) {
                proj.push(self.seq_idx);
            }
        }
    }

    /// Builtin columns are always appended to the end of the schema.
    pub fn fill_builtin_columns(
        &self,
        record_batch: RecordBatch,
        sequence: u64,
    ) -> Result<RecordBatch> {
        let num_rows = record_batch.num_rows();
        if num_rows == 0 {
            return Ok(record_batch);
        }

        let mut columns = record_batch.columns().to_vec();
        let seq_array = UInt64Array::from_iter_values((0..num_rows).map(|_| sequence));
        columns.push(Arc::new(seq_array));
        let reserved_array = UInt64Array::new_null(num_rows);
        columns.push(Arc::new(reserved_array));

        let new_batch = RecordBatch::try_new(self.arrow_schema.clone(), columns)
            .context("construct record batch with seq column")?;

        Ok(new_batch)
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    use crate::{arrow_schema, record_batch};

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

    #[test]
    fn test_build_storage_schema() {
        let arrow_schema = arrow_schema!(("pk1", UInt8), ("pk2", UInt8), ("value", Int64));
        let schema = StorageSchema::try_new(arrow_schema.clone(), 2, UpdateMode::Append).unwrap();
        assert_eq!(schema.value_idxes, vec![2]);
        assert_eq!(schema.seq_idx, 3);
        assert_eq!(schema.reserved_idx, 4);

        // No value column exists
        assert!(StorageSchema::try_new(arrow_schema, 3, UpdateMode::Append).is_err());

        let batch = record_batch!(
            ("pk1", UInt8, vec![11, 11, 9, 10]),
            ("pk2", UInt8, vec![100, 99, 1, 2]),
            ("value", Int64, vec![22, 77, 44, 66])
        )
        .unwrap();
        let sequence = 999;
        let new_batch = schema.fill_builtin_columns(batch, sequence).unwrap();
        let expected_batch = record_batch!(
            ("pk1", UInt8, vec![11, 11, 9, 10]),
            ("pk2", UInt8, vec![100, 99, 1, 2]),
            ("value", Int64, vec![22, 77, 44, 66]),
            (SEQ_COLUMN_NAME, UInt64, vec![sequence; 4]),
            (RESERVED_COLUMN_NAME, UInt64, vec![None; 4])
        )
        .unwrap();
        assert_eq!(new_batch, expected_batch);

        let mut testcases = [
            (None, None),
            (Some(vec![]), Some(vec![0, 1, 3])),
            (Some(vec![1]), Some(vec![1, 0, 3])),
            (Some(vec![2]), Some(vec![2, 0, 1, 3])),
        ];
        for (input, expected) in testcases.iter_mut() {
            schema.fill_required_projections(input);
            assert_eq!(input, expected);
        }
    }
}
