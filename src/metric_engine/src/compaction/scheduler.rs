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
    sync::{atomic::AtomicU64, Arc},
    time::Duration,
};

use anyhow::Context;
use parquet::file::properties::WriterProperties;
use tokio::{
    sync::mpsc::{self, Receiver, Sender},
    task::JoinHandle,
    time::sleep,
};
use tracing::warn;

use super::runner::Runner;
use crate::{
    compaction::{picker::TimeWindowCompactionStrategy, Task},
    manifest::ManifestRef,
    read::ParquetReader,
    sst::SstPathGenerator,
    types::{ObjectStoreRef, RuntimeRef, StorageSchema},
    Result,
};

pub struct Scheduler {
    runtime: RuntimeRef,

    task_tx: Sender<Task>,
    inused_memory: AtomicU64,
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
            let rt = runtime.clone();
            let store = store.clone();
            let manifest = manifest.clone();
            let write_props = config.write_props.clone();
            runtime.spawn(async move {
                Self::recv_task_loop(
                    rt,
                    task_rx,
                    store,
                    schema,
                    manifest,
                    sst_path_gen,
                    parquet_reader,
                    config.memory_limit,
                    write_props,
                )
                .await;
            })
        };
        let picker_handle = {
            let task_tx = task_tx.clone();
            runtime.spawn(async move {
                Self::generate_task_loop(manifest, task_tx, segment_duration, config).await;
            })
        };

        Self {
            runtime,
            task_tx,
            task_handle,
            picker_handle,
            inused_memory: AtomicU64::new(0),
        }
    }

    pub fn try_send(&self, task: Task) -> Result<()> {
        self.task_tx
            .try_send(task)
            .context("failed to send task to scheduler")?;

        Ok(())
    }

    async fn recv_task_loop(
        rt: RuntimeRef,
        mut task_rx: Receiver<Task>,
        store: ObjectStoreRef,
        schema: StorageSchema,
        manifest: ManifestRef,
        sst_path_gen: Arc<SstPathGenerator>,
        parquet_reader: Arc<ParquetReader>,
        _mem_limit: u64,
        write_props: WriterProperties,
    ) {
        let runner = Runner::new(
            store,
            schema,
            manifest,
            sst_path_gen,
            parquet_reader,
            write_props,
        );
        while let Some(task) = task_rx.recv().await {
            let runner = runner.clone();
            rt.spawn(async move {
                if let Err(e) = runner.do_compaction(task).await {
                    warn!("Do compaction failed, err:{e}");
                }
            });
        }
    }

    async fn generate_task_loop(
        manifest: ManifestRef,
        task_tx: Sender<Task>,
        segment_duration: Duration,
        config: SchedulerConfig,
    ) {
        let schedule_interval = config.schedule_interval;
        let compactor = TimeWindowCompactionStrategy::new(segment_duration, config);
        // TODO: obtain expire time
        let expire_time = None;
        loop {
            let ssts = manifest.all_ssts().await;
            if let Some(task) = compactor.pick_candidate(ssts, expire_time) {
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
    pub memory_limit: u64,
    pub max_pending_compaction_tasks: usize,
    pub compaction_files_limit: usize,
    pub write_props: WriterProperties,
}

impl Default for SchedulerConfig {
    fn default() -> Self {
        Self {
            schedule_interval: Duration::from_secs(30),
            memory_limit: bytesize::gb(2_u64),
            max_pending_compaction_tasks: 10,
            compaction_files_limit: 10,
            write_props: WriterProperties::default(),
        }
    }
}