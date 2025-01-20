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

#[derive(Copy, Clone)]
pub struct MetricId(pub u64);
#[derive(Copy, Clone)]
pub struct SeriesId(pub u64);
pub type SegmentDuration = Duration;
pub type MetricName = Vec<u8>;
pub type FieldName = Vec<u8>;
pub type FieldType = usize;
pub type TagName = Vec<u8>;
pub type TagValue = Vec<u8>;

#[derive(PartialEq, PartialOrd, Eq, Ord, Clone)]
pub struct Label {
    pub name: Vec<u8>,
    pub value: Vec<u8>,
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

pub struct SeriesKey(Vec<Label>);

impl SeriesKey {
    pub fn new(metric_name: &[u8], lables: &[Label]) -> Self {
        let mut set: BTreeSet<Label> = BTreeSet::new();
        lables.iter().for_each(|item| {
            set.insert(item.clone());
        });
        set.insert(Label {
            name: String::from("__name__").into(),
            value: metric_name.to_vec(),
        });

        Self(set.into_iter().collect::<Vec<Label>>())
    }

    pub fn make_bytes(&self) -> Vec<u8> {
        let mut series_bytes: Vec<u8> = Vec::new();
        self.0.iter().for_each(|item| {
            series_bytes
                .write_all(item.name.as_slice())
                .expect("can write");
            series_bytes
                .write_all(item.value.as_slice())
                .expect("can write");
        });
        series_bytes
    }
}

pub fn hash(buf: &[u8]) -> u64 {
    seahash::hash(buf)
}
