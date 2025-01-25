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

use std::{collections::BTreeSet, io::Write, time::Duration};

pub type Result<T> = common::Result<T>;

#[derive(Copy, Clone, PartialEq, Eq, Hash, Debug)]
pub struct MetricId(pub u64);
#[derive(Copy, Clone, Eq, PartialEq, Hash, Debug)]
pub struct SeriesId(pub u64);
pub type MetricName = Vec<u8>;
pub type FieldName = Vec<u8>;
pub type FieldType = u8;
pub type TagName = Vec<u8>;
pub type TagValue = Vec<u8>;
pub type TagNames = Vec<Vec<u8>>;
pub type TagValues = Vec<Vec<u8>>;

pub const METRIC_NAME: &str = "__name__";
pub const DEFAULT_FIELD_NAME: &str = "value";
pub const DEFAULT_FIELD_TYPE: u8 = 0;

#[derive(Copy, Clone, Eq, PartialEq, Hash, Debug)]
pub struct SegmentDuration(Duration);

impl SegmentDuration {
    const ONE_DAY_SECOND: u64 = 24 * 60 * 60;

    pub fn date(time: Duration) -> Self {
        let now = time.as_secs();
        Self(Duration::from_secs(
            now / SegmentDuration::ONE_DAY_SECOND * SegmentDuration::ONE_DAY_SECOND,
        ))
    }

    pub fn same_segment(lhs: Duration, rhs: Duration) -> bool {
        lhs.as_secs() / SegmentDuration::ONE_DAY_SECOND
            == rhs.as_secs() / SegmentDuration::ONE_DAY_SECOND
    }
}
#[derive(PartialEq, PartialOrd, Eq, Ord, Clone)]
pub struct Label {
    pub name: Vec<u8>,
    pub value: Vec<u8>,
}

impl std::fmt::Debug for Label {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Label {{ name: {:?}, value: {:?} }}",
            String::from_utf8(self.name.clone()).unwrap(),
            String::from_utf8(self.value.clone()).unwrap(),
        )
    }
}

/// This is the main struct used for write, optional values will be filled in
/// different modules.
pub struct Sample {
    pub name: Vec<u8>,
    pub lables: Vec<Label>,
    pub timestamp: i64,
    pub value: f64,
    /// hash of name
    pub name_id: Option<MetricId>,
    /// hash of labels(sorted)
    pub series_id: Option<SeriesId>,
}

#[derive(Clone, Debug)]
pub struct SeriesKey {
    pub names: TagNames,
    pub values: TagValues,
}

impl SeriesKey {
    pub fn new(metric_name: Option<&[u8]>, lables: &[Label]) -> Self {
        let mut sorted_key: BTreeSet<&Label> = BTreeSet::new();
        lables.iter().for_each(|item| {
            sorted_key.insert(item);
        });

        let mut names = sorted_key
            .iter()
            .map(|item| item.name.clone())
            .collect::<Vec<_>>();
        let mut values = sorted_key
            .iter()
            .map(|item| item.value.clone())
            .collect::<Vec<_>>();
        if let Some(metric_name) = metric_name {
            names.insert(0, METRIC_NAME.as_bytes().to_vec());
            values.insert(0, metric_name.to_vec());
        }
        Self { names, values }
    }

    pub fn make_bytes(&self) -> Vec<u8> {
        let mut series_bytes: Vec<u8> = Vec::new();
        self.names
            .iter()
            .zip(self.values.iter())
            .for_each(|(name, value)| {
                series_bytes
                    .write_all(name.as_slice())
                    .expect("could write");
                series_bytes
                    .write_all(value.as_slice())
                    .expect("could write");
            });
        series_bytes
    }
}

pub fn hash(buf: &[u8]) -> u64 {
    seahash::hash(buf)
}
