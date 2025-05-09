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

use anyhow::Context;
use parquet::file::properties::WriterProperties;
use tokio::{
    sync::mpsc::{self, Receiver, Sender},
    task::JoinHandle,
    time::sleep,
};
use tracing::{info, warn};

use super::{executor::Executor, picker::Picker};
use crate::{
    compaction::Task,
    config::SchedulerConfig,
    manifest::ManifestRef,
    read::ParquetReader,
    sst::SstPathGenerator,
    types::{ObjectStoreRef, RuntimeRef, StorageSchema},
    Result,
};

#[allow(dead_code)]
pub struct Scheduler {
    runtime: RuntimeRef,

    trigger_tx: Sender<()>,
    task_handle: JoinHandle<()>,
    picker_handle: JoinHandle<()>,
}

impl Scheduler {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        runtime: RuntimeRef,
        manifest: ManifestRef,
        store: ObjectStoreRef,
        schema: StorageSchema,
        segment_duration: Duration,
        sst_path_gen: Arc<SstPathGenerator>,
        parquet_reader: Arc<ParquetReader>,
        config: SchedulerConfig,
        write_props: WriterProperties,
    ) -> Self {
        let (task_tx, task_rx) = mpsc::channel(config.max_pending_compaction_tasks);
        let (trigger_tx, trigger_rx) = mpsc::channel::<()>(1);
        let task_handle = {
            let store = store.clone();
            let manifest = manifest.clone();
            let executor = Executor::new(
                runtime.clone(),
                store,
                schema,
                manifest,
                sst_path_gen,
                parquet_reader,
                write_props,
                config.memory_limit.0,
                trigger_tx.clone(),
            );

            runtime.spawn(async move {
                Self::recv_task_loop(task_rx, executor).await;
            })
        };
        let picker_handle = {
            runtime.spawn(async move {
                let picker = Picker::new(
                    manifest,
                    config.ttl.map(|v| v.0),
                    segment_duration,
                    config.new_sst_max_size.0,
                    config.input_sst_max_num,
                    config.input_sst_min_num,
                );
                Self::generate_task_loop(task_tx, trigger_rx, picker, config.schedule_interval.0)
                    .await;
            })
        };

        Self {
            runtime,
            trigger_tx,
            task_handle,
            picker_handle,
        }
    }

    pub fn trigger_compaction(&self) -> Result<()> {
        self.trigger_tx
            .try_send(())
            .context("send trigger signal failed")?;

        Ok(())
    }

    async fn recv_task_loop(mut task_rx: Receiver<Task>, executor: Executor) {
        info!("Scheduler receive task started");
        while let Some(task) = task_rx.recv().await {
            executor.submit(task);
        }
    }

    async fn generate_task_loop(
        task_tx: Sender<Task>,
        mut trigger_rx: Receiver<()>,
        mut picker: Picker,
        schedule_interval: Duration,
    ) {
        info!(
            schedule_interval = ?schedule_interval,
            "Scheduler generate task loop started"
        );
        let send_task = |task| {
            if let Err(e) = task_tx.try_send(task) {
                warn!("Send task failed, err:{e:?}");
            }
        };

        // Generate one task immediately
        if let Some(task) = picker.pick_candidate().await {
            send_task(task);
        }
        loop {
            tokio::select! {
                _ = sleep(schedule_interval) => {
                    if let Some(task) = picker.pick_candidate().await {
                        send_task(task);
                    }
                }
                signal = trigger_rx.recv() => {
                    if signal.is_none() {
                        info!("Scheduler generate task loop stopped");
                        return;
                    }
                    if let Some(task) = picker.pick_candidate().await {
                        send_task(task);
                    }
                }
            }
        }
    }
}
