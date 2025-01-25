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
    collections::{HashMap, HashSet},
    path::Path,
    sync::Arc,
    time::Duration,
};

use anyhow::Context;
use arrow::{
    array::{
        Array, ArrayRef, BinaryArray, BinaryBuilder, ListArray, UInt64Array, UInt64Builder,
        UInt8Array,
    },
    buffer::OffsetBuffer,
    datatypes::{DataType, Field, Schema, ToByteSlice},
    record_batch::RecordBatch,
};
use dashmap::DashMap;
use futures::StreamExt;
use horaedb_storage::{
    config::StorageConfig,
    storage::{
        CloudObjectStorage, ScanRequest, StorageRuntimes, TimeMergeStorageRef, WriteRequest,
    },
    types::{ObjectStoreRef, TimeRange, Timestamp},
};
use tokio::sync::{
    mpsc::{self, Receiver, Sender},
    RwLock,
};
use tracing::error;

use crate::types::{
    hash, FieldName, FieldType, Label, MetricId, MetricName, Result, SegmentDuration, SeriesId,
    SeriesKey, TagName, TagNames, TagValue, TagValues, DEFAULT_FIELD_NAME, DEFAULT_FIELD_TYPE,
};

const COLUMN_METRIC_NAME: &str = "metric_name";
const COLUMN_METRIC_ID: &str = "metric_id";
const COLUMN_SERIES_ID: &str = "series_id";
const COLUMN_FIELD_ID: &str = "field_id";
const COLUMN_FIELD_NAME: &str = "field_name";
const COLUMN_FIELD_TYPE: &str = "field_type";
const COLUMN_TAG_NAMES: &str = "tag_names";
const COLUMN_TAG_VALUES: &str = "tag_values";
const COLUMN_TAG_NAME: &str = "tag_name";
const COLUMN_TAG_VALUE: &str = "tag_value";
const COLUMN_TAG_ITEM: &str = "item";

type ConcurrentMetricMap = RwLock<HashMap<MetricName, (FieldName, FieldType)>>;
type ConcurrentSeriesMap = RwLock<HashMap<SeriesId, SeriesKey>>;
type ConcurrentTagKVMap =
    RwLock<HashMap<TagName, HashMap<TagValue, HashMap<MetricId, HashSet<SeriesId>>>>>;

struct MetricsCache {
    cache: DashMap<SegmentDuration, ConcurrentMetricMap>,
    pub storage: TimeMergeStorageRef,
    sender: Sender<Task>,
}
struct SeriesCache {
    cache: DashMap<SegmentDuration, ConcurrentSeriesMap>,
    pub storage: TimeMergeStorageRef,
    sender: Sender<Task>,
}

#[derive(PartialEq, Eq, Hash, Debug)]
struct SegmentSeries {
    segment: SegmentDuration,
    series_id: SeriesId,
}

struct TagIndexCache {
    cache: DashMap<SegmentDuration, ConcurrentTagKVMap>,
    series_records: RwLock<HashSet<SegmentSeries>>,
    storage: TimeMergeStorageRef,
    sender: Sender<Task>,
}

impl MetricsCache {
    fn new(storage: TimeMergeStorageRef, sender: Sender<Task>) -> Self {
        Self {
            cache: DashMap::new(),
            storage,
            sender,
        }
    }

    fn parse_record_batch(
        batch: &RecordBatch,
        index: usize,
    ) -> Result<(&[u8], &[u8], u8, u64, u64)> {
        let metric_name = batch
            .column_by_name(COLUMN_METRIC_NAME)
            .context("get column failed")?
            .as_any()
            .downcast_ref::<BinaryArray>()
            .context("parse column failed")?
            .value(index);

        let field_name = batch
            .column_by_name(COLUMN_FIELD_NAME)
            .context("get column failed")?
            .as_any()
            .downcast_ref::<BinaryArray>()
            .context("parse column failed")?
            .value(index);

        let field_type = batch
            .column_by_name(COLUMN_FIELD_TYPE)
            .context("get column failed")?
            .as_any()
            .downcast_ref::<UInt8Array>()
            .context("parse column failed")?
            .value(index);

        let filed_id = batch
            .column_by_name(COLUMN_FIELD_ID)
            .context("get column failed")?
            .as_any()
            .downcast_ref::<UInt64Array>()
            .context("parse column failed")?
            .value(index);

        let metric_id = batch
            .column_by_name(COLUMN_METRIC_ID)
            .context("get column failed")?
            .as_any()
            .downcast_ref::<UInt64Array>()
            .context("parse column failed")?
            .value(index);

        Ok((metric_name, field_name, field_type, filed_id, metric_id))
    }

    async fn load_from_storage(&mut self) -> Result<()> {
        let mut result_stream = self
            .storage
            .scan(ScanRequest {
                range: TimeRange::new(Timestamp(0), Timestamp::MAX),
                predicate: vec![],
                projections: None,
            })
            .await?;
        while let Some(item) = result_stream.next().await {
            let batch = item.context("get next batch failed")?;
            for index in 0..batch.num_rows() {
                let (metric_name, field_name, field_type, _, _) =
                    MetricsCache::parse_record_batch(&batch, index)?;
                self.update(metric_name, field_name, field_type).await?;
            }
        }
        Ok(())
    }

    fn schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new(COLUMN_METRIC_NAME, DataType::Binary, true),
            Field::new(COLUMN_METRIC_ID, DataType::UInt64, true),
            Field::new(COLUMN_FIELD_NAME, DataType::Binary, true),
            Field::new(COLUMN_FIELD_ID, DataType::UInt64, true),
            Field::new(COLUMN_FIELD_TYPE, DataType::UInt8, true),
        ]))
    }

    async fn update(&self, name: &[u8], field_name: &[u8], field_type: u8) -> Result<bool> {
        let current = SegmentDuration::current_date();
        if self.cache.contains_key(&current)
            && self
                .cache
                .get(&current)
                .context("get key failed")?
                .read()
                .await
                .contains_key(name)
        {
            Ok(false)
        } else {
            let result = self
                .cache
                .entry(current)
                .or_default()
                .write()
                .await
                .insert(name.to_vec(), (field_name.to_vec(), field_type));

            Ok(result.is_none())
        }
    }

    async fn notify_write(&self, name: &[u8], field_name: &[u8], field_type: u8) -> Result<()> {
        self.sender
            .send(Task::Metric(name.to_vec(), field_name.to_vec(), field_type))
            .await
            .context("notify write failed.")?;
        Ok(())
    }
}

impl SeriesCache {
    fn new(storage: TimeMergeStorageRef, sender: Sender<Task>) -> Self {
        Self {
            cache: DashMap::new(),
            storage,
            sender,
        }
    }

    async fn parse_record_batch(
        batch: &RecordBatch,
        index: usize,
    ) -> Result<(u64, Vec<Vec<u8>>, Vec<Vec<u8>>)> {
        let series_id = batch
            .column_by_name(COLUMN_SERIES_ID)
            .context("get column failed")?
            .as_any()
            .downcast_ref::<UInt64Array>()
            .context("parse column failed")?
            .value(index);

        let tag_names = {
            let tag_name_array = batch
                .column_by_name(COLUMN_TAG_NAMES)
                .context("get column failed")?
                .as_any()
                .downcast_ref::<ListArray>()
                .context("parse column failed")?
                .value(index);
            let tag_names = tag_name_array
                .as_any()
                .downcast_ref::<BinaryArray>()
                .context("parse column failed")?;
            tag_names
                .iter()
                .map(|item| item.unwrap_or(b"").to_vec())
                .collect::<Vec<_>>()
        };

        let tag_values = {
            let tag_value_array = batch
                .column_by_name(COLUMN_TAG_VALUES)
                .context("get column failed")?
                .as_any()
                .downcast_ref::<ListArray>()
                .context("parse column failed")?
                .value(index);
            let tag_values = tag_value_array
                .as_any()
                .downcast_ref::<BinaryArray>()
                .expect("List elements should be BinaryArray");
            tag_values
                .iter()
                .map(|item| item.unwrap_or(b"").to_vec())
                .collect::<Vec<_>>()
        };
        Ok((series_id, tag_names, tag_values))
    }

    async fn load_from_storage(&mut self) -> Result<()> {
        let mut result_stream = self
            .storage
            .scan(ScanRequest {
                range: TimeRange::new(Timestamp(0), Timestamp::MAX),
                predicate: vec![],
                projections: None,
            })
            .await?;
        while let Some(item) = result_stream.next().await {
            let batch = item.context("get next batch failed.")?;
            for index in 0..batch.num_rows() {
                let (series_id, tag_names, tag_values) =
                    SeriesCache::parse_record_batch(&batch, index).await?;
                let labels = tag_names
                    .into_iter()
                    .zip(tag_values.into_iter())
                    .map(|(name, value)| Label { name, value })
                    .collect::<Vec<_>>();
                let key = SeriesKey::new(None, labels.as_slice());
                self.update(&SeriesId(series_id), &key).await?;
            }
        }
        Ok(())
    }

    pub fn schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new(COLUMN_METRIC_ID, DataType::UInt64, true),
            Field::new(COLUMN_SERIES_ID, DataType::UInt64, true),
            Field::new(
                COLUMN_TAG_NAMES,
                DataType::List(Arc::new(Field::new(
                    COLUMN_TAG_ITEM,
                    DataType::Binary,
                    true,
                ))),
                true,
            ),
            Field::new(
                COLUMN_TAG_VALUES,
                DataType::List(Arc::new(Field::new(
                    COLUMN_TAG_ITEM,
                    DataType::Binary,
                    true,
                ))),
                true,
            ),
        ]))
    }

    async fn update(&self, id: &SeriesId, key: &SeriesKey) -> Result<bool> {
        let current = SegmentDuration::current_date();
        if self.cache.contains_key(&current)
            && self
                .cache
                .get(&current)
                .context("get key failed")?
                .read()
                .await
                .contains_key(id)
        {
            Ok(false)
        } else {
            let result = self
                .cache
                .entry(current)
                .or_default()
                .write()
                .await
                .insert(*id, key.clone());

            Ok(result.is_none())
        }
    }

    async fn notify_write(
        &self,
        id: &SeriesId,
        key: &SeriesKey,
        metric_id: &MetricId,
    ) -> Result<()> {
        self.sender
            .send(Task::Series(*id, key.clone(), *metric_id))
            .await
            .context("notify write failed.")?;
        Ok(())
    }
}

impl TagIndexCache {
    fn new(storage: TimeMergeStorageRef, sender: Sender<Task>) -> Self {
        Self {
            cache: DashMap::new(),
            series_records: RwLock::new(HashSet::new()),
            storage,
            sender,
        }
    }

    async fn parse_record_batch(
        batch: &RecordBatch,
        index: usize,
    ) -> Result<(u64, &[u8], &[u8], u64)> {
        let metric_id = batch
            .column_by_name(COLUMN_METRIC_ID)
            .context("get column failed")?
            .as_any()
            .downcast_ref::<UInt64Array>()
            .context("parse column failed")?
            .value(index);

        let tag_name = batch
            .column_by_name(COLUMN_TAG_NAME)
            .context("get column failed")?
            .as_any()
            .downcast_ref::<BinaryArray>()
            .context("parse column failed")?
            .value(index);

        let tag_value = batch
            .column_by_name(COLUMN_TAG_VALUE)
            .context("get column failed")?
            .as_any()
            .downcast_ref::<BinaryArray>()
            .context("parse column failed")?
            .value(index);

        let series_id = batch
            .column_by_name(COLUMN_SERIES_ID)
            .context("get column failed")?
            .as_any()
            .downcast_ref::<UInt64Array>()
            .context("parse column failed")?
            .value(index);

        Ok((metric_id, tag_name, tag_value, series_id))
    }

    async fn load_from_storage(&mut self) -> Result<()> {
        let mut result_stream = self
            .storage
            .scan(ScanRequest {
                range: TimeRange::new(Timestamp(0), Timestamp::MAX),
                predicate: vec![],
                projections: None,
            })
            .await
            .unwrap();
        while let Some(item) = result_stream.next().await {
            let batch = item.context("get next batch failed.")?;
            for index in 0..batch.num_rows() {
                let (series_id, tag_name, tag_value, metric_id) =
                    TagIndexCache::parse_record_batch(&batch, index).await?;
                self.update(
                    &SeriesId(series_id),
                    &vec![tag_name.to_vec()],
                    &vec![tag_value.to_vec()],
                    &MetricId(metric_id),
                )
                .await?;
            }
        }
        Ok(())
    }

    fn schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new(COLUMN_METRIC_ID, DataType::UInt64, true),
            Field::new(COLUMN_TAG_NAME, DataType::Binary, true),
            Field::new(COLUMN_TAG_VALUE, DataType::Binary, true),
            Field::new(COLUMN_SERIES_ID, DataType::UInt64, true),
        ]))
    }

    async fn update(
        &self,
        series_id: &SeriesId,
        tag_names: &TagNames,
        tag_values: &TagValues,
        metric_id: &MetricId,
    ) -> Result<bool> {
        let current = SegmentDuration::current_date();
        let segment_series = SegmentSeries {
            segment: current,
            series_id: *series_id,
        };
        if self.series_records.read().await.contains(&segment_series) {
            Ok(false)
        } else {
            let mut series_records = self.series_records.write().await;
            if series_records.contains(&segment_series) {
                Ok(false)
            } else {
                series_records.insert(segment_series);
                let cache_lock = self.cache.entry(current).or_default();
                let mut cache_guard = cache_lock.write().await;

                let (names, values) = remove_default_tag(tag_names.clone(), tag_values.clone());
                names.iter().zip(values.iter()).for_each(|(name, value)| {
                    cache_guard
                        .entry(name.clone())
                        .or_default()
                        .entry(value.clone())
                        .or_default()
                        .entry(*metric_id)
                        .or_default()
                        .insert(*series_id);
                });
                Ok(true)
            }
        }
    }

    async fn notify_write(
        &self,
        series_id: &SeriesId,
        tag_names: &TagNames,
        tag_values: &TagValues,
        metric_id: &MetricId,
    ) -> Result<()> {
        self.sender
            .send(Task::TagIndex(
                *series_id,
                tag_names.clone(),
                tag_values.clone(),
                *metric_id,
            ))
            .await
            .context("notify write failed.")?;
        Ok(())
    }
}

pub struct CacheManager {
    metrics: MetricsCache,
    series: SeriesCache,
    tag_index: TagIndexCache,
}

enum Task {
    Metric(MetricName, FieldName, FieldType),
    Series(SeriesId, SeriesKey, MetricId),
    TagIndex(SeriesId, TagNames, TagValues, MetricId),
}

struct CacheWriter {
    pub receiver: Receiver<Task>,
    pub storage: TimeMergeStorageRef,
    pub schema: Arc<Schema>,
}

impl CacheWriter {
    pub fn new(
        receiver: Receiver<Task>,
        storage: TimeMergeStorageRef,
        schema: Arc<Schema>,
    ) -> Self {
        Self {
            receiver,
            storage,
            schema,
        }
    }
}

async fn make_storage(
    runtimes: StorageRuntimes,
    store: ObjectStoreRef,
    root_dir: String,
    num_primary_keys: usize,
    schema: Arc<Schema>,
) -> Result<TimeMergeStorageRef> {
    Ok(Arc::new(
        CloudObjectStorage::try_new(
            root_dir,
            Duration::from_secs(3600 * 24), // 1 day
            store.clone(),
            schema,
            num_primary_keys,
            StorageConfig::default(),
            runtimes.clone(),
        )
        .await?,
    ))
}

impl CacheManager {
    pub async fn try_new(
        runtimes: StorageRuntimes,
        store: ObjectStoreRef,
        root_dir: &str,
    ) -> Result<Self> {
        let metrics = {
            let path = Path::new(root_dir).join("metrics");
            let root_dir = path.to_string_lossy().to_string();
            let schema = MetricsCache::schema();
            let storage =
                make_storage(runtimes.clone(), store.clone(), root_dir, 2, schema.clone()).await?;
            let (sender, receiver) = mpsc::channel(1024);
            let writer = CacheWriter::new(receiver, storage.clone(), schema.clone());
            tokio::spawn(async move { CacheManager::execute_write(writer).await });
            let mut cache = MetricsCache::new(storage, sender);
            cache.load_from_storage().await?;
            cache
        };
        let series = {
            let path = Path::new(root_dir).join("series");
            let root_dir = path.to_string_lossy().to_string();
            let schema = SeriesCache::schema();
            let storage =
                make_storage(runtimes.clone(), store.clone(), root_dir, 2, schema.clone()).await?;
            let (sender, receiver) = mpsc::channel(1024);
            let writer = CacheWriter::new(receiver, storage.clone(), schema.clone());
            tokio::spawn(async move { CacheManager::execute_write(writer).await });
            let mut cache = SeriesCache::new(storage, sender);
            cache.load_from_storage().await?;
            cache
        };

        let tag_index = {
            let path = Path::new(root_dir).join("tag_index");
            let root_dir = path.to_string_lossy().to_string();
            let schema = TagIndexCache::schema();
            let storage = make_storage(runtimes, store, root_dir, 3, schema.clone()).await?;
            let (sender, receiver) = mpsc::channel(1024);
            let writer = CacheWriter::new(receiver, storage.clone(), schema);
            tokio::spawn(async move { CacheManager::execute_write(writer).await });
            let mut cache = TagIndexCache::new(storage, sender);
            cache.load_from_storage().await?;
            cache
        };

        Ok(Self {
            metrics,
            series,
            tag_index,
        })
    }

    pub async fn update_metric(&self, name: &[u8]) -> Result<()> {
        let updated = self
            .metrics
            .update(name, DEFAULT_FIELD_NAME.as_bytes(), DEFAULT_FIELD_TYPE)
            .await?;
        if updated {
            self.metrics
                .notify_write(name, DEFAULT_FIELD_NAME.as_bytes(), DEFAULT_FIELD_TYPE)
                .await?;
        }
        Ok(())
    }

    pub async fn update_series(
        &self,
        id: &SeriesId,
        key: &SeriesKey,
        metric_id: &MetricId,
    ) -> Result<()> {
        let updated = self.series.update(id, key).await?;
        if updated {
            self.series.notify_write(id, key, metric_id).await?;
        }
        Ok(())
    }

    pub async fn update_tag_index(
        &self,
        series_id: &SeriesId,
        series_key: &SeriesKey,
        metric_id: &MetricId,
    ) -> Result<()> {
        let updated = self
            .tag_index
            .update(series_id, &series_key.names, &series_key.values, metric_id)
            .await?;
        if updated {
            self.tag_index
                .notify_write(series_id, &series_key.names, &series_key.values, metric_id)
                .await?;
        }
        Ok(())
    }

    async fn execute_write(mut writer: CacheWriter) {
        // TODO: use try_recv to accumulated a number of tasks and handle them in one
        // batch
        while let Some(task) = writer.receiver.recv().await {
            match task {
                Task::Metric(name, field_name, field_type) => {
                    let arrays: Vec<ArrayRef> = vec![
                        Arc::new(BinaryArray::from(vec![name.to_byte_slice()])),
                        Arc::new(UInt64Array::from(vec![hash(&name)])),
                        Arc::new(BinaryArray::from(vec![field_name.to_byte_slice()])),
                        Arc::new(UInt64Array::from(vec![hash(field_name.to_byte_slice())])),
                        Arc::new(UInt8Array::from(vec![field_type])),
                    ];
                    let batch = RecordBatch::try_new(writer.schema.clone(), arrays).unwrap();
                    writer
                        .storage
                        .write(WriteRequest {
                            batch,
                            time_range: (0..10).into(),
                            enable_check: true,
                        })
                        .await
                        .unwrap_or_else(|e| {
                            error!("write metrics failed: {:?}", e);
                        });
                }
                Task::Series(id, key, metric_id) => {
                    let offsets: Vec<i32> = vec![0, key.names.len() as i32];
                    let tag_names_array = {
                        let name_binary_values = key
                            .names
                            .iter()
                            .map(|item| item.as_slice())
                            .collect::<Vec<_>>();
                        let name_binary_array = BinaryArray::from_vec(name_binary_values);
                        ListArray::try_new(
                            Arc::new(Field::new(COLUMN_TAG_ITEM, DataType::Binary, true)),
                            OffsetBuffer::new(offsets.clone().into()),
                            Arc::new(name_binary_array),
                            None,
                        )
                        .unwrap()
                    };

                    let tag_values_array = {
                        let value_binary_values = key
                            .values
                            .iter()
                            .map(|item| item.as_slice())
                            .collect::<Vec<_>>();
                        let value_binary_array = BinaryArray::from_vec(value_binary_values);
                        ListArray::try_new(
                            Arc::new(Field::new(COLUMN_TAG_ITEM, DataType::Binary, true)),
                            OffsetBuffer::new(offsets.into()),
                            Arc::new(value_binary_array),
                            None,
                        )
                        .unwrap()
                    };

                    let arrays: Vec<ArrayRef> = vec![
                        Arc::new(UInt64Array::from(vec![metric_id.0])),
                        Arc::new(UInt64Array::from(vec![id.0])),
                        Arc::new(tag_names_array),
                        Arc::new(tag_values_array),
                    ];
                    let batch = RecordBatch::try_new(writer.schema.clone(), arrays).unwrap();
                    writer
                        .storage
                        .write(WriteRequest {
                            batch,
                            time_range: (0..10).into(),
                            enable_check: true,
                        })
                        .await
                        .unwrap_or_else(|e| {
                            error!("write metrics failed: {:?}", e);
                        });
                }
                Task::TagIndex(series_id, names, values, metric_id) => {
                    let mut metrics_id_builder = UInt64Builder::new();
                    let mut series_id_builder = UInt64Builder::new();
                    let mut tag_name_builder = BinaryBuilder::new();
                    let mut tag_value_builder = BinaryBuilder::new();

                    let (names, values) = remove_default_tag(names, values);

                    names.iter().zip(values.iter()).for_each(|(name, value)| {
                        metrics_id_builder.append_value(metric_id.0);
                        tag_name_builder.append_value(name.to_byte_slice());
                        tag_value_builder.append_value(value.to_byte_slice());
                        series_id_builder.append_value(series_id.0);
                    });
                    let arrays: Vec<ArrayRef> = vec![
                        Arc::new(metrics_id_builder.finish()),
                        Arc::new(tag_name_builder.finish()),
                        Arc::new(tag_value_builder.finish()),
                        Arc::new(series_id_builder.finish()),
                    ];
                    let batch = RecordBatch::try_new(writer.schema.clone(), arrays).unwrap();
                    writer
                        .storage
                        .write(WriteRequest {
                            batch,
                            time_range: (0..10).into(),
                            enable_check: true,
                        })
                        .await
                        .unwrap_or_else(|e| {
                            error!("write metrics failed: {:?}", e);
                        });
                }
            }
        }
    }
}

fn remove_default_tag(
    mut names: Vec<Vec<u8>>,
    mut values: Vec<Vec<u8>>,
) -> (Vec<Vec<u8>>, Vec<Vec<u8>>) {
    let mut to_remove_index = HashSet::new();
    let mut index = 0;
    names.retain(|item| {
        let keep = !item.starts_with(b"__");
        if !keep {
            to_remove_index.insert(index);
        }
        index += 1;
        keep
    });
    let mut index = 0;
    values.retain(|_| {
        let keep = !to_remove_index.contains(&index);
        index += 1;
        keep
    });
    (names, values)
}

#[cfg(test)]
mod tests {
    use horaedb_storage::{
        storage::ScanRequest,
        types::{TimeRange, Timestamp},
    };
    use object_store::local::LocalFileSystem;
    use tokio::runtime::Runtime;

    use super::*;
    use crate::types::{hash, Label};

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_cache_manager_updates() {
        let rt = Arc::new(Runtime::new().unwrap());
        let runtimes = StorageRuntimes::new(rt.clone(), rt);
        let store = Arc::new(LocalFileSystem::new());
        let root_dir = "/tmp/horaedb".to_string();
        let cache_manager = CacheManager::try_new(runtimes, store, root_dir.as_str())
            .await
            .unwrap();

        {
            // Test update_metric
            let metric_name = "metric_neo".as_bytes();
            let metric_id = MetricId(hash(metric_name));
            cache_manager.update_metric(metric_name).await.unwrap();

            let series_id = SeriesId(11);
            let lables = vec![
                Label {
                    name: b"label_a".to_vec(),
                    value: b"111".to_vec(),
                },
                Label {
                    name: b"label_b".to_vec(),
                    value: b"222".to_vec(),
                },
            ];
            let series_key = SeriesKey::new(Some(metric_name), lables.as_slice());
            // Test update_series
            cache_manager
                .update_series(&series_id, &series_key, &metric_id)
                .await
                .unwrap();
            // Test update_tag_index
            cache_manager
                .update_tag_index(&series_id, &series_key, &metric_id)
                .await
                .unwrap();
        }

        {
            // Test update_metric
            let metric_name = "metric_neo2".as_bytes();
            let metric_id = MetricId(hash(metric_name));
            cache_manager.update_metric(metric_name).await.unwrap();

            let series_id = SeriesId(22);
            let lables = vec![
                Label {
                    name: b"label_a".to_vec(),
                    value: b"111".to_vec(),
                },
                Label {
                    name: b"label_c".to_vec(),
                    value: b"333".to_vec(),
                },
            ];
            let series_key = SeriesKey::new(Some(metric_name), lables.as_slice());
            // Test update_series
            cache_manager
                .update_series(&series_id, &series_key, &metric_id)
                .await
                .unwrap();
            // Test update_tag_index
            cache_manager
                .update_tag_index(&series_id, &series_key, &metric_id)
                .await
                .unwrap();
        }

        tokio::time::sleep(std::time::Duration::from_secs(3)).await;

        // scan and test file data
        {
            let mut result_stream = cache_manager
                .metrics
                .storage
                .scan(ScanRequest {
                    range: TimeRange::new(Timestamp(0), Timestamp::MAX),
                    predicate: vec![],
                    projections: None,
                })
                .await
                .unwrap();

            {
                let item = result_stream.next().await;
                let batch = item.unwrap().unwrap();
                let (metric_name, field_name, field_type, filed_id, metric_id) =
                    MetricsCache::parse_record_batch(&batch, 0).unwrap();
                assert_eq!(metric_name, b"metric_neo");
                assert_eq!(field_name, b"value");
                assert_eq!(field_type, 0);
                assert_eq!(filed_id, 17612580310495814266);
                assert_eq!(metric_id, 12417319948205937109);
            }

            {
                let item = result_stream.next().await;
                let batch = item.unwrap().unwrap();
                let (metric_name, field_name, field_type, filed_id, metric_id) =
                    MetricsCache::parse_record_batch(&batch, 0).unwrap();
                assert_eq!(metric_name, b"metric_neo2");
                assert_eq!(field_name, b"value");
                assert_eq!(field_type, 0);
                assert_eq!(filed_id, 17612580310495814266);
                assert_eq!(metric_id, 17578343207158939466);
            }
        }

        {
            let mut result_stream = cache_manager
                .series
                .storage
                .scan(ScanRequest {
                    range: TimeRange::new(Timestamp(0), Timestamp::MAX),
                    predicate: vec![],
                    projections: None,
                })
                .await
                .unwrap();
            {
                let item = result_stream.next().await;
                let batch = item.unwrap().unwrap();
                let (series_id, tag_names, tag_values) =
                    SeriesCache::parse_record_batch(&batch, 0).await.unwrap();

                assert_eq!(series_id, 11);
                assert_eq!(
                    tag_names,
                    vec![
                        b"__name__".to_vec(),
                        b"label_a".to_vec(),
                        b"label_b".to_vec()
                    ]
                );
                assert_eq!(
                    tag_values,
                    vec![b"metric_neo".to_vec(), b"111".to_vec(), b"222".to_vec()]
                );
            }
            {
                let item = result_stream.next().await;
                let batch = item.unwrap().unwrap();
                let (series_id, tag_names, tag_values) =
                    SeriesCache::parse_record_batch(&batch, 0).await.unwrap();

                assert_eq!(series_id, 22);
                assert_eq!(
                    tag_names,
                    vec![
                        b"__name__".to_vec(),
                        b"label_a".to_vec(),
                        b"label_c".to_vec()
                    ]
                );
                assert_eq!(
                    tag_values,
                    vec![b"metric_neo2".to_vec(), b"111".to_vec(), b"333".to_vec()]
                );
            }
        }
        {
            let mut result_stream = cache_manager
                .tag_index
                .storage
                .scan(ScanRequest {
                    range: TimeRange::new(Timestamp(0), Timestamp::MAX),
                    predicate: vec![],
                    projections: None,
                })
                .await
                .unwrap();
            {
                let item = result_stream.next().await;
                let batch = item.unwrap().unwrap();
                assert_eq!(batch.num_rows(), 3);
                {
                    let (metric_id, tag_name, tag_value, series_id) =
                        TagIndexCache::parse_record_batch(&batch, 0).await.unwrap();
                    assert_eq!(metric_id, 12417319948205937109);
                    assert_eq!(tag_name, b"label_a");
                    assert_eq!(tag_value, b"111");
                    assert_eq!(series_id, 11);
                }
                {
                    let (metric_id, tag_name, tag_value, series_id) =
                        TagIndexCache::parse_record_batch(&batch, 1).await.unwrap();
                    assert_eq!(metric_id, 12417319948205937109);
                    assert_eq!(tag_name, b"label_b");
                    assert_eq!(tag_value, b"222");
                    assert_eq!(series_id, 11);
                }
                {
                    let (metric_id, tag_name, tag_value, series_id) =
                        TagIndexCache::parse_record_batch(&batch, 2).await.unwrap();
                    assert_eq!(metric_id, 17578343207158939466);
                    assert_eq!(tag_name, b"label_a");
                    assert_eq!(tag_value, b"111");
                    assert_eq!(series_id, 22);
                }
            }
            {
                let item = result_stream.next().await;
                let batch = item.unwrap().unwrap();
                {
                    let (metric_id, tag_name, tag_value, series_id) =
                        TagIndexCache::parse_record_batch(&batch, 0).await.unwrap();
                    assert_eq!(metric_id, 17578343207158939466);
                    assert_eq!(tag_name, b"label_c");
                    assert_eq!(tag_value, b"333");
                    assert_eq!(series_id, 22);
                }
            }
        }
    }
}
