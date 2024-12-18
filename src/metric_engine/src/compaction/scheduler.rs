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

use std::{sync::Arc, time::Duration};

use parquet::file::properties::WriterProperties;
use tokio::{
    sync::mpsc::{self, Receiver, Sender},
    task::JoinHandle,
    time::sleep,
};
use tracing::warn;

use super::{executor::Executor, picker::Picker};
use crate::{
    compaction::Task,
    manifest::ManifestRef,
    read::ParquetReader,
    sst::SstPathGenerator,
    types::{ObjectStoreRef, RuntimeRef, StorageSchema},
};

#[allow(dead_code)]
pub struct Scheduler {
    runtime: RuntimeRef,

    task_tx: Sender<Task>,
    task_handle: JoinHandle<()>,
    picker_handle: JoinHandle<()>,
}

impl Scheduler {
    pub fn new(
        runtime: RuntimeRef,
        manifest: ManifestRef,
        store: ObjectStoreRef,
        schema: StorageSchema,
        segment_duration: Duration,
        sst_path_gen: Arc<SstPathGenerator>,
        parquet_reader: Arc<ParquetReader>,
        config: SchedulerConfig,
    ) -> Self {
        let (task_tx, task_rx) = mpsc::channel(config.max_pending_compaction_tasks);
        let task_handle = {
            let store = store.clone();
            let manifest = manifest.clone();
            let write_props = config.write_props.clone();
            let executor = Executor::new(
                runtime.clone(),
                store,
                schema,
                manifest,
                sst_path_gen,
                parquet_reader,
                write_props,
                config.memory_limit,
            );

            runtime.spawn(async move {
                Self::recv_task_loop(task_rx, executor).await;
            })
        };
        let picker_handle = {
            let task_tx = task_tx.clone();
            runtime.spawn(async move {
                let picker = Picker::new(
                    manifest,
                    config.ttl,
                    segment_duration,
                    config.new_sst_max_size,
                    config.input_sst_max_num,
                );
                Self::generate_task_loop(task_tx, picker, config.schedule_interval).await;
            })
        };

        Self {
            runtime,
            task_tx,
            task_handle,
            picker_handle,
        }
    }

    async fn recv_task_loop(mut task_rx: Receiver<Task>, executor: Executor) {
        while let Some(task) = task_rx.recv().await {
            executor.spawn(task);
        }
    }

    async fn generate_task_loop(
        task_tx: Sender<Task>,
        picker: Picker,
        schedule_interval: Duration,
    ) {
        loop {
            if let Some(task) = picker.pick_candidate().await {
                if let Err(e) = task_tx.try_send(task) {
                    warn!("Send task failed, err:{e}");
                }
            }

            sleep(schedule_interval).await;
        }
    }
}

#[derive(Clone)]
pub struct SchedulerConfig {
    pub schedule_interval: Duration,
    pub max_pending_compaction_tasks: usize,
    // Runner config
    pub memory_limit: u64,
    pub write_props: WriterProperties,
    // Picker config
    pub ttl: Option<Duration>,
    pub new_sst_max_size: u64,
    pub input_sst_max_num: usize,
}

impl Default for SchedulerConfig {
    fn default() -> Self {
        Self {
            schedule_interval: Duration::from_secs(30),
            max_pending_compaction_tasks: 10,
            memory_limit: bytesize::gb(3_u64),
            write_props: WriterProperties::default(),
            ttl: None,
            new_sst_max_size: bytesize::gb(1_u64),
            input_sst_max_num: 10,
        }
    }
}
