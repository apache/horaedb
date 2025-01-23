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

mod cache;
use std::sync::Arc;

use cache::CacheManager;
use horaedb_storage::storage::{StorageRuntimes, TimeMergeStorageRef};
use object_store::local::LocalFileSystem;
use tokio::runtime::Runtime;

use crate::{
    types::{
        hash, FieldName, FieldType, MetricId, MetricName, Sample, SegmentDuration, SeriesId,
        SeriesKey, TagName, TagValue,
    },
    Result,
};

pub struct IndexManager {
    inner: Arc<Inner>,
}

impl IndexManager {
    pub async fn new() -> Self {
        // TODO: maybe inialize runtime and store by config?
        let rt = Arc::new(Runtime::new().unwrap());
        let runtimes = StorageRuntimes::new(rt.clone(), rt);
        let store = Arc::new(LocalFileSystem::new());
        let root_dir = "/tmp/horaedb".to_string();

        Self {
            inner: Arc::new(Inner {
                cache: CacheManager::new(runtimes, store, root_dir.as_str()).await,
            }),
        }
    }

    /// Populate series ids from labels.
    /// It will also build inverted index for labels.
    pub async fn populate_series_ids(&self, samples: &mut [Sample]) -> Result<()> {
        // 1. create metric id and series id
        let metric_ids = samples
            .iter()
            .map(|s| MetricId(hash(s.name.as_slice())))
            .collect::<Vec<_>>();

        let series_keys = samples
            .iter()
            .map(|s| SeriesKey::new(Some(s.name.as_slice()), s.lables.as_slice()))
            .collect::<Vec<_>>();
        let series_ids = series_keys
            .iter()
            .map(|e| SeriesId(hash(e.make_bytes().as_slice())))
            .collect::<Vec<_>>();

        samples.iter_mut().enumerate().for_each(|(i, sample)| {
            sample.name_id = Some(metric_ids[i]);
            sample.series_id = Some(series_ids[i]);
        });
        // 2. cache metrics
        samples
            .iter()
            .for_each(|s| self.inner.create_metrics(s.name.as_slice()));

        // 3. cache series
        series_ids
            .iter()
            .zip(series_keys.iter())
            .for_each(|(id, key)| self.inner.create_series(id, key));

        // 4. cache tag index
        series_ids
            .iter()
            .zip(series_keys.iter())
            .zip(metric_ids.iter())
            .for_each(|((series_id, series_key), metric_id)| {
                self.inner
                    .create_tag_index(series_id, series_key, metric_id)
            });

        Ok(())
    }
}

struct Inner {
    cache: CacheManager,
}

impl Inner {
    pub fn create_metrics(&self, name: &[u8]) {
        todo!()
    }

    pub fn create_series(&self, id: &SeriesId, key: &SeriesKey) {
        todo!()
    }

    pub fn create_tag_index(
        &self,
        series_id: &SeriesId,
        series_key: &SeriesKey,
        metric_id: &MetricId,
    ) {
        todo!()
    }
}
