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
use tokio::{
    sync::mpsc::{self, Receiver, Sender},
    task::JoinHandle,
    time::sleep,
};
use tracing::warn;

use crate::{
    compaction::{picker::TimeWindowCompactionStrategy, Task},
    manifest::ManifestRef,
    sst::SstPathGenerator,
    types::{ObjectStoreRef, RuntimeRef},
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
        segment_duration: Duration,
        sst_path_gen: Arc<SstPathGenerator>,
        config: SchedulerConfig,
    ) -> Self {
        let (task_tx, task_rx) = mpsc::channel(config.max_pending_compaction_tasks);
        let task_handle = {
            let rt = runtime.clone();
            let store = store.clone();
            let manifest = manifest.clone();
            runtime.spawn(async move {
                Self::recv_task_loop(
                    rt,
                    task_rx,
                    store,
                    manifest,
                    sst_path_gen,
                    config.memory_limit,
                )
                .await;
            })
        };
        let picker_handle = {
            let task_tx = task_tx.clone();
            let interval = config.schedule_interval;
            runtime.spawn(async move {
                Self::generate_task_loop(manifest, task_tx, interval, segment_duration).await;
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
        manifest: ManifestRef,
        _sst_path_gen: Arc<SstPathGenerator>,
        _mem_limit: u64,
    ) {
        while let Some(task) = task_rx.recv().await {
            let store = store.clone();
            let manifest = manifest.clone();
            rt.spawn(async move {
                let runner = Runner { store, manifest };
                if let Err(e) = runner.do_compaction(task).await {
                    warn!("Do compaction failed, err:{e}");
                }
            });
        }
    }

    async fn generate_task_loop(
        manifest: ManifestRef,
        task_tx: Sender<Task>,
        schedule_interval: Duration,
        segment_duration: Duration,
    ) {
        let compactor = TimeWindowCompactionStrategy::new(segment_duration);
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

pub struct SchedulerConfig {
    pub schedule_interval: Duration,
    pub memory_limit: u64,
    pub max_pending_compaction_tasks: usize,
}

impl Default for SchedulerConfig {
    fn default() -> Self {
        Self {
            schedule_interval: Duration::from_secs(30),
            memory_limit: bytesize::gb(2_u64),
            max_pending_compaction_tasks: 10,
        }
    }
}

pub struct Runner {
    store: ObjectStoreRef,
    manifest: ManifestRef,
}

impl Runner {
    // TODO: Merge input sst files into one new sst file
    // and delete the expired sst files
    async fn do_compaction(&self, _task: Task) -> Result<()> {
        todo!()
    }
}
