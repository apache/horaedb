use core::num;
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
    sync::{Arc, RwLock},
    time::Duration,
};

use arrow::{
    array::{
        Array, ArrayRef, BinaryArray, BinaryBuilder, Int32BufferBuilder, ListArray, ListBuilder,
        UInt64Array, UInt64Builder, UInt8Array,
    },
    buffer::OffsetBuffer,
    datatypes::{BinaryType, DataType, Field, Schema, ToByteSlice},
    record_batch::RecordBatch,
};
use dashmap::DashMap;
use futures::{Stream, StreamExt};
use horaedb_storage::{
    config::StorageConfig,
    storage::{
        CloudObjectStorage, ScanRequest, StorageRuntimes, TimeMergeStorage, TimeMergeStorageRef,
        WriteRequest,
    },
    types::{ObjectStoreRef, TimeRange, Timestamp},
};
use tokio::{
    runtime::Runtime,
    sync::mpsc::{self, Receiver, Sender},
};

use crate::types::{
    default_field_name, default_field_type, hash, FieldName, FieldType, Label, MetricId,
    MetricName, SegmentDuration, SeriesId, SeriesKey, TagName, TagNames, TagValue, TagValues,
};

struct MetricsCache {
    cache: DashMap<SegmentDuration, RwLock<HashMap<MetricName, (FieldName, FieldType)>>>,
    pub storage: TimeMergeStorageRef,
    sender: Sender<Task>,
}
struct SeriesCache {
    cache: DashMap<SegmentDuration, RwLock<HashMap<SeriesId, SeriesKey>>>,
    pub storage: TimeMergeStorageRef,
    sender: Sender<Task>,
}

#[derive(PartialEq, Eq, Hash, Debug)]
struct SegmentSeries {
    segment: SegmentDuration,
    series_id: SeriesId,
}

struct TagIndexCache {
    cache: DashMap<
        SegmentDuration,
        RwLock<HashMap<TagName, HashMap<TagValue, HashMap<MetricId, HashSet<SeriesId>>>>>,
    >,
    write_guard: RwLock<HashSet<SegmentSeries>>,
    pub storage: TimeMergeStorageRef,
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

    async fn load_from_storage(&mut self) {
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
            let batch = item.unwrap();
            for index in 0..batch.num_rows() {
                let metric_name = batch
                    .column_by_name("metric_name")
                    .unwrap()
                    .as_any()
                    .downcast_ref::<BinaryArray>()
                    .unwrap()
                    .value(index);

                let field_name = batch
                    .column_by_name("field_name")
                    .unwrap()
                    .as_any()
                    .downcast_ref::<BinaryArray>()
                    .unwrap()
                    .value(index);

                let field_type = batch
                    .column_by_name("field_type")
                    .unwrap()
                    .as_any()
                    .downcast_ref::<UInt8Array>()
                    .unwrap()
                    .value(index);

                // TODO: why not put in cache?
                let _filed_id = batch
                    .column_by_name("field_id")
                    .unwrap()
                    .as_any()
                    .downcast_ref::<UInt64Array>()
                    .unwrap()
                    .value(index);

                // TODO: why not put in cache?
                let _metric_id = batch
                    .column_by_name("metric_id")
                    .unwrap()
                    .as_any()
                    .downcast_ref::<UInt64Array>()
                    .unwrap()
                    .value(index);

                self.update(metric_name, field_name, field_type, None).await;
            }
        }
    }

    fn schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("metric_name", DataType::Binary, true),
            Field::new("metric_id", DataType::UInt64, true),
            Field::new("field_name", DataType::Binary, true),
            Field::new("field_id", DataType::UInt64, true),
            Field::new("field_type", DataType::UInt8, true),
        ]))
    }

    async fn update(&self, name: &[u8], field_name: &[u8], field_type: u8, notify: Option<()>) {
        let current = SegmentDuration::current();
        let task = if self.cache.contains_key(&current)
            && self
                .cache
                .get(&current)
                .unwrap()
                .read()
                .unwrap()
                .contains_key(name)
        {
            None
        } else {
            let result = self
                .cache
                .entry(current)
                .or_default()
                .write()
                .unwrap()
                .insert(name.to_vec(), (field_name.to_vec(), field_type));

            match result {
                Some(_) => None,
                None => Some(Task::Metric(name.to_vec())),
            }
        };

        if let (Some(task), Some(_)) = (task, notify) {
            self.sender.send(task).await.unwrap();
        }
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

    async fn load_from_storage(&mut self) {
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
            let batch = item.unwrap();
            for index in 0..batch.num_rows() {
                let metric_id = batch
                    .column_by_name("metric_id")
                    .unwrap()
                    .as_any()
                    .downcast_ref::<UInt64Array>()
                    .unwrap()
                    .value(index);

                let series_id = batch
                    .column_by_name("series_id")
                    .unwrap()
                    .as_any()
                    .downcast_ref::<UInt64Array>()
                    .unwrap()
                    .value(index);

                let tag_name_array = batch
                    .column_by_name("tag_names")
                    .unwrap()
                    .as_any()
                    .downcast_ref::<ListArray>()
                    .unwrap()
                    .value(index);
                let tag_names = tag_name_array
                    .as_any()
                    .downcast_ref::<BinaryArray>()
                    .expect("List elements should be BinaryArray");

                let names = tag_names
                    .iter()
                    .map(|item| item.unwrap().to_vec())
                    .collect::<Vec<_>>();

                let tag_value_array = batch
                    .column_by_name("tag_values")
                    .unwrap()
                    .as_any()
                    .downcast_ref::<ListArray>()
                    .unwrap()
                    .value(index);
                let tag_values = tag_value_array
                    .as_any()
                    .downcast_ref::<BinaryArray>()
                    .expect("List elements should be BinaryArray");

                let values = tag_values
                    .iter()
                    .map(|item| item.unwrap().to_vec())
                    .collect::<Vec<_>>();

                let labels = names
                    .into_iter()
                    .zip(values.into_iter())
                    .map(|(name, value)| Label { name, value })
                    .collect::<Vec<_>>();
                let key = SeriesKey::new(None, labels.as_slice());
                self.update(&SeriesId(series_id), &key, &MetricId(metric_id), None)
                    .await;
            }
        }
    }

    pub fn schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("metric_id", DataType::UInt64, true),
            Field::new("series_id", DataType::UInt64, true),
            Field::new(
                "tag_names",
                DataType::List(Arc::new(Field::new("item", DataType::Binary, true))),
                true,
            ),
            Field::new(
                "tag_values",
                DataType::List(Arc::new(Field::new("item", DataType::Binary, true))),
                true,
            ),
        ]))
    }

    async fn update(
        &self,
        id: &SeriesId,
        key: &SeriesKey,
        metric_id: &MetricId,
        notify: Option<()>,
    ) {
        let current = SegmentDuration::current();
        let task = if self.cache.contains_key(&current)
            && self
                .cache
                .get(&current)
                .unwrap()
                .read()
                .unwrap()
                .contains_key(id)
        {
            None
        } else {
            let result = self
                .cache
                .entry(current)
                .or_default()
                .write()
                .unwrap()
                .insert(*id, key.clone());

            match result {
                Some(_) => None,
                None => Some(Task::Series(*id, key.clone(), *metric_id)),
            }
        };
        if let (Some(task), Some(_)) = (task, notify) {
            self.sender.send(task).await.unwrap();
        }
    }
}

impl TagIndexCache {
    fn new(storage: TimeMergeStorageRef, sender: Sender<Task>) -> Self {
        Self {
            cache: DashMap::new(),
            write_guard: RwLock::new(HashSet::new()),
            storage,
            sender,
        }
    }

    async fn load_from_storage(&mut self) {
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
            let batch = item.unwrap();
            for index in 0..batch.num_rows() {
                let metric_id = batch
                    .column_by_name("metric_id")
                    .unwrap()
                    .as_any()
                    .downcast_ref::<UInt64Array>()
                    .unwrap()
                    .value(index);

                let tag_name = batch
                    .column_by_name("tag_name")
                    .unwrap()
                    .as_any()
                    .downcast_ref::<BinaryArray>()
                    .unwrap()
                    .value(index);

                let tag_value = batch
                    .column_by_name("tag_value")
                    .unwrap()
                    .as_any()
                    .downcast_ref::<BinaryArray>()
                    .unwrap()
                    .value(index);

                let series_id = batch
                    .column_by_name("series_id")
                    .unwrap()
                    .as_any()
                    .downcast_ref::<UInt64Array>()
                    .unwrap()
                    .value(index);
                self.update(
                    &SeriesId(series_id),
                    &vec![tag_name.to_vec()],
                    &vec![tag_value.to_vec()],
                    &MetricId(metric_id),
                    None,
                )
                .await;
            }
        }
    }

    fn schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("metric_id", DataType::UInt64, true),
            Field::new("tag_name", DataType::Binary, true),
            Field::new("tag_value", DataType::Binary, true),
            Field::new("series_id", DataType::UInt64, true),
        ]))
    }

    async fn update(
        &self,
        series_id: &SeriesId,
        tag_names: &TagNames,
        tag_values: &TagValues,
        metric_id: &MetricId,
        notify: Option<()>,
    ) {
        let current = SegmentDuration::current();
        let segment_series = SegmentSeries {
            segment: current,
            series_id: *series_id,
        };
        let task = if self.write_guard.read().unwrap().contains(&segment_series) {
            None
        } else {
            let mut write_guard = self.write_guard.write().unwrap();
            if write_guard.contains(&segment_series) {
                None
            } else {
                write_guard.insert(segment_series);
                let cache_lock = self
                    .cache
                    .entry(current)
                    .or_insert_with(|| RwLock::new(HashMap::new()));
                let mut cache_guard = cache_lock.write().unwrap();

                // __name__ is always at the first
                let mut names = tag_names.clone();
                let mut values = tag_values.clone();
                names.remove(0);
                values.remove(0);
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
                Some(Task::TagIndex(*series_id, names, values, *metric_id))
            }
        };
        if let (Some(task), Some(_)) = (task, notify) {
            self.sender.send(task).await.unwrap();
        }
    }
}

pub struct CacheManager {
    metrics: MetricsCache,
    series: SeriesCache,
    tag_index: TagIndexCache,
}

enum Task {
    Metric(MetricName),
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
) -> TimeMergeStorageRef {
    Arc::new(
        CloudObjectStorage::try_new(
            root_dir,
            Duration::from_secs(3600 * 24), // 1 day
            store.clone(),
            schema,
            num_primary_keys, // num_primary_keys
            StorageConfig::default(),
            runtimes.clone(),
        )
        .await
        .unwrap(),
    )
}

impl CacheManager {
    pub async fn new(runtimes: StorageRuntimes, store: ObjectStoreRef, root_dir: &str) -> Self {
        let metrics = {
            let path = Path::new(root_dir).join("metrics");
            let root_dir = path.to_string_lossy().to_string();
            let schema = MetricsCache::schema();
            let storage =
                make_storage(runtimes.clone(), store.clone(), root_dir, 2, schema.clone()).await;
            let (sender, receiver) = mpsc::channel(32);
            let writer = CacheWriter::new(receiver, storage.clone(), schema.clone());
            tokio::spawn(async move { CacheManager::execute_write(writer).await });
            let mut cache = MetricsCache::new(storage, sender);
            cache.load_from_storage().await;
            cache
        };
        let series = {
            let path = Path::new(root_dir).join("series");
            let root_dir = path.to_string_lossy().to_string();
            let schema = SeriesCache::schema();
            let storage =
                make_storage(runtimes.clone(), store.clone(), root_dir, 2, schema.clone()).await;
            let (sender, receiver) = mpsc::channel(32);
            let writer = CacheWriter::new(receiver, storage.clone(), schema.clone());
            tokio::spawn(async move { CacheManager::execute_write(writer).await });
            let mut cache = SeriesCache::new(storage, sender);
            cache.load_from_storage().await;
            cache
        };

        let tag_index = {
            let path = Path::new(root_dir).join("tag_index");
            let root_dir = path.to_string_lossy().to_string();
            let schema = TagIndexCache::schema();
            let storage = make_storage(runtimes, store, root_dir, 3, schema.clone()).await;
            let (sender, receiver) = mpsc::channel(32);
            let writer = CacheWriter::new(receiver, storage.clone(), schema);
            tokio::spawn(async move { CacheManager::execute_write(writer).await });
            let mut cache = TagIndexCache::new(storage, sender);
            cache.load_from_storage().await;
            cache
        };

        Self {
            metrics,
            series,
            tag_index,
        }
    }

    pub async fn update_metric(&self, name: &[u8]) {
        self.metrics
            .update(name, &default_field_name(), default_field_type(), Some(()))
            .await;
    }

    pub async fn update_series(&self, id: &SeriesId, key: &SeriesKey, metric_id: &MetricId) {
        self.series.update(id, key, metric_id, Some(())).await;
    }

    pub async fn update_tag_index(
        &self,
        series_id: &SeriesId,
        series_key: &SeriesKey,
        metric_id: &MetricId,
    ) {
        self.tag_index
            .update(
                series_id,
                &series_key.names,
                &series_key.values,
                metric_id,
                Some(()),
            )
            .await;
    }

    async fn execute_write(mut writer: CacheWriter) {
        while let Some(task) = writer.receiver.recv().await {
            match task {
                Task::Metric(name) => {
                    let arrays: Vec<ArrayRef> = vec![
                        Arc::new(BinaryArray::from(vec![name.to_byte_slice()])),
                        Arc::new(UInt64Array::from(vec![hash(&name)])),
                        Arc::new(BinaryArray::from(
                            vec![default_field_name().to_byte_slice()],
                        )),
                        Arc::new(UInt64Array::from(vec![hash(
                            default_field_name().to_byte_slice(),
                        )])),
                        Arc::new(UInt8Array::from(vec![default_field_type()])),
                    ];
                    // Step 4: 构造 RecordBatch
                    let batch = RecordBatch::try_new(writer.schema.clone(), arrays).unwrap();
                    writer
                        .storage
                        .write(WriteRequest {
                            batch,
                            time_range: (0..10).into(),
                            enable_check: true,
                        })
                        .await
                        .unwrap();
                }
                Task::Series(id, key, metric_id) => {
                    let offsets: Vec<i32> = vec![0, key.names.len() as i32];
                    let name_binary_values = key
                        .names
                        .iter()
                        .map(|item| item.as_slice())
                        .collect::<Vec<_>>();
                    let name_binary_array = BinaryArray::from_vec(name_binary_values);
                    let tag_names = ListArray::try_new(
                        Arc::new(Field::new("item", DataType::Binary, true)),
                        OffsetBuffer::new(offsets.clone().into()),
                        Arc::new(name_binary_array),
                        None,
                    )
                    .unwrap();

                    let value_binary_values = key
                        .names
                        .iter()
                        .map(|item| item.as_slice())
                        .collect::<Vec<_>>();
                    let value_binary_array = BinaryArray::from_vec(value_binary_values);
                    let tag_values = ListArray::try_new(
                        Arc::new(Field::new("item", DataType::Binary, true)),
                        OffsetBuffer::new(offsets.into()),
                        Arc::new(value_binary_array),
                        None,
                    )
                    .unwrap();

                    let arrays: Vec<ArrayRef> = vec![
                        Arc::new(UInt64Array::from(vec![metric_id.0])),
                        Arc::new(UInt64Array::from(vec![id.0])),
                        Arc::new(tag_names),
                        Arc::new(tag_values),
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
                        .unwrap();
                }
                Task::TagIndex(series_id, names, values, metric_id) => {
                    let mut metrics_id_builder = UInt64Builder::new();
                    let mut series_id_builder = UInt64Builder::new();
                    let mut tag_name_builder = BinaryBuilder::new();
                    let mut tag_value_builder = BinaryBuilder::new();

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
                        .unwrap();
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use horaedb_storage::{
        storage::ScanRequest,
        types::{TimeRange, Timestamp},
    };
    use object_store::local::LocalFileSystem;

    use super::*;
    use crate::types::{hash, Label};

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_cache_manager_updates() {
        let rt = Arc::new(Runtime::new().unwrap());
        let runtimes = StorageRuntimes::new(rt.clone(), rt);
        let store = Arc::new(LocalFileSystem::new());
        let root_dir = "/tmp/horaedb".to_string();
        let cache_manager = CacheManager::new(runtimes, store, root_dir.as_str()).await;

        {
            // Test update_metric
            let metric_name = "metric_neo".as_bytes();
            cache_manager.update_metric(metric_name).await;

            let series_id = SeriesId(11);
            let lables = vec![
                Label {
                    name: b"l1".to_vec(),
                    value: b"111".to_vec(),
                },
                Label {
                    name: b"l2".to_vec(),
                    value: b"222".to_vec(),
                },
            ];
            let series_key = SeriesKey::new(Some(metric_name), lables.as_slice());

            // Test update_series
            cache_manager
                .update_series(&series_id, &series_key, &MetricId(hash(metric_name)))
                .await;

            // Test update_tag_index
            cache_manager
                .update_tag_index(&series_id, &series_key, &MetricId(hash(metric_name)))
                .await;
        }

        {
            // Test update_metric
            let metric_name = "metric_neo2".as_bytes();
            cache_manager.update_metric(metric_name).await;

            let series_id = SeriesId(22);
            let lables = vec![
                Label {
                    name: b"l1".to_vec(),
                    value: b"111".to_vec(),
                },
                Label {
                    name: b"l2".to_vec(),
                    value: b"222".to_vec(),
                },
            ];
            let series_key = SeriesKey::new(Some(metric_name), lables.as_slice());

            // Test update_series
            cache_manager
                .update_series(&series_id, &series_key, &MetricId(hash(metric_name)))
                .await;

            // Test update_tag_index
            cache_manager
                .update_tag_index(&series_id, &series_key, &MetricId(hash(metric_name)))
                .await;
        }

        // sleep 30s
        tokio::time::sleep(std::time::Duration::from_secs(5)).await;

        {
            println!("metricsssssss.....");
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
            while let Some(item) = result_stream.next().await {
                let batch = item.unwrap();
                println!("=====");
                println!("batch: {:?}", batch);
            }
        }
        {
            println!("series.....");
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
            while let Some(item) = result_stream.next().await {
                let batch = item.unwrap();
                println!("=====");
                println!("batch: {:?}", batch);
            }
        }
        {
            println!("tags.....");
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
            while let Some(item) = result_stream.next().await {
                let batch = item.unwrap();
                println!("=====");
                println!("batch: {:?}", batch);
            }
        }
    }
}

// write a test for me
