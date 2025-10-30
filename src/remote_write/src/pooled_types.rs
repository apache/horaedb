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

use std::sync::Arc;

use bytes::Bytes;
use object_pool::Pool;
use once_cell::sync::Lazy;

use crate::repeated_field::{Clear, RepeatedField};

#[derive(Debug, Clone)]
pub struct Label {
    pub name: Bytes,
    pub value: Bytes,
}

impl Default for Label {
    fn default() -> Self {
        Self {
            name: Bytes::new(),
            value: Bytes::new(),
        }
    }
}

impl Clear for Label {
    fn clear(&mut self) {
        self.name.clear();
        self.value.clear();
    }
}

#[derive(Debug, Clone)]
pub struct Sample {
    pub value: f64,
    pub timestamp: i64,
}

impl Default for Sample {
    fn default() -> Self {
        Self {
            value: 0.0,
            timestamp: 0,
        }
    }
}

impl Clear for Sample {
    fn clear(&mut self) {
        self.value = 0.0;
        self.timestamp = 0;
    }
}

#[derive(Debug, Clone)]
pub struct Exemplar {
    pub labels: RepeatedField<Label>,
    pub value: f64,
    pub timestamp: i64,
}

impl Default for Exemplar {
    fn default() -> Self {
        Self {
            labels: RepeatedField::default(),
            value: 0.0,
            timestamp: 0,
        }
    }
}

impl Clear for Exemplar {
    fn clear(&mut self) {
        self.labels.clear();
        self.value = 0.0;
        self.timestamp = 0;
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Default)]
pub enum MetricType {
    #[default]
    Unknown = 0,
    Counter = 1,
    Gauge = 2,
    Histogram = 3,
    GaugeHistogram = 4,
    Summary = 5,
    Info = 6,
    StateSet = 7,
}

#[derive(Debug, Clone)]
pub struct MetricMetadata {
    pub metric_type: MetricType,
    pub metric_family_name: Bytes,
    pub help: Bytes,
    pub unit: Bytes,
}

impl Default for MetricMetadata {
    fn default() -> Self {
        Self {
            metric_type: MetricType::Unknown,
            metric_family_name: Bytes::new(),
            help: Bytes::new(),
            unit: Bytes::new(),
        }
    }
}

impl Clear for MetricMetadata {
    fn clear(&mut self) {
        self.metric_type = MetricType::Unknown;
        self.metric_family_name.clear();
        self.help.clear();
        self.unit.clear();
    }
}

#[derive(Debug, Clone, Default)]
pub struct TimeSeries {
    pub labels: RepeatedField<Label>,
    pub samples: RepeatedField<Sample>,
    pub exemplars: RepeatedField<Exemplar>,
}

impl Clear for TimeSeries {
    fn clear(&mut self) {
        self.labels.clear();
        self.samples.clear();
        self.exemplars.clear();
    }
}

#[derive(Debug, Clone, Default)]
pub struct WriteRequest {
    pub timeseries: RepeatedField<TimeSeries>,
    pub metadata: RepeatedField<MetricMetadata>,
}

impl Clear for WriteRequest {
    fn clear(&mut self) {
        self.timeseries.clear();
        self.metadata.clear();
    }
}

const POOL_SIZE: usize = 16; // Maximum number of objects in the pool.

/// Global thread-safe object pool for `WriteRequest`.
pub static POOL: Lazy<Arc<Pool<WriteRequest>>> =
    Lazy::new(|| Arc::new(Pool::new(POOL_SIZE, WriteRequest::default)));
