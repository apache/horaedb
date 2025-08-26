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

use async_trait::async_trait;
use bytes::Bytes;
use deadpool::managed::{Manager, Metrics, Pool, RecycleResult};
use once_cell::sync::Lazy;

use crate::repeated_field::{Clear, RepeatedField};

#[derive(Debug, Clone)]
pub struct PooledLabel {
    pub name: Bytes,
    pub value: Bytes,
}

impl Default for PooledLabel {
    fn default() -> Self {
        Self {
            name: Bytes::new(),
            value: Bytes::new(),
        }
    }
}

impl Clear for PooledLabel {
    fn clear(&mut self) {
        self.name.clear();
        self.value.clear();
    }
}

#[derive(Debug, Clone)]
pub struct PooledSample {
    pub value: f64,
    pub timestamp: i64,
}

impl Default for PooledSample {
    fn default() -> Self {
        Self {
            value: 0.0,
            timestamp: 0,
        }
    }
}

impl Clear for PooledSample {
    fn clear(&mut self) {
        self.value = 0.0;
        self.timestamp = 0;
    }
}

#[derive(Debug, Clone)]
pub struct PooledExemplar {
    pub labels: RepeatedField<PooledLabel>,
    pub value: f64,
    pub timestamp: i64,
}

impl Default for PooledExemplar {
    fn default() -> Self {
        Self {
            labels: RepeatedField::default(),
            value: 0.0,
            timestamp: 0,
        }
    }
}

impl Clear for PooledExemplar {
    fn clear(&mut self) {
        self.labels.clear();
        self.value = 0.0;
        self.timestamp = 0;
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Default)]
pub enum PooledMetricType {
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
pub struct PooledMetricMetadata {
    pub metric_type: PooledMetricType,
    pub metric_family_name: Bytes,
    pub help: Bytes,
    pub unit: Bytes,
}

impl Default for PooledMetricMetadata {
    fn default() -> Self {
        Self {
            metric_type: PooledMetricType::Unknown,
            metric_family_name: Bytes::new(),
            help: Bytes::new(),
            unit: Bytes::new(),
        }
    }
}

impl Clear for PooledMetricMetadata {
    fn clear(&mut self) {
        self.metric_type = PooledMetricType::Unknown;
        self.metric_family_name.clear();
        self.help.clear();
        self.unit.clear();
    }
}

#[derive(Debug, Clone, Default)]
pub struct PooledTimeSeries {
    pub labels: RepeatedField<PooledLabel>,
    pub samples: RepeatedField<PooledSample>,
    pub exemplars: RepeatedField<PooledExemplar>,
}

impl Clear for PooledTimeSeries {
    fn clear(&mut self) {
        self.labels.clear();
        self.samples.clear();
        self.exemplars.clear();
    }
}

#[derive(Debug, Clone, Default)]
pub struct PooledWriteRequest {
    pub timeseries: RepeatedField<PooledTimeSeries>,
    pub metadata: RepeatedField<PooledMetricMetadata>,
}

impl Clear for PooledWriteRequest {
    fn clear(&mut self) {
        self.timeseries.clear();
        self.metadata.clear();
    }
}

/// A deadpool manager for PooledWriteRequest.
pub struct WriteRequestManager;

#[async_trait]
impl Manager for WriteRequestManager {
    type Error = ();
    type Type = PooledWriteRequest;

    async fn create(&self) -> Result<Self::Type, Self::Error> {
        Ok(PooledWriteRequest::default())
    }

    async fn recycle(
        &self,
        _obj: &mut Self::Type,
        _metrics: &Metrics,
    ) -> RecycleResult<Self::Error> {
        // We will reset the object after acquiring it.
        Ok(())
    }
}

const POOL_SIZE: usize = 64; // Maximum number of objects in the pool.

pub static POOL: Lazy<Pool<WriteRequestManager>> = Lazy::new(|| {
    Pool::builder(WriteRequestManager)
        .max_size(POOL_SIZE)
        .build()
        .unwrap()
});
