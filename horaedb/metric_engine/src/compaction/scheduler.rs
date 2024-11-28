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

use std::{sync::atomic::AtomicU64, time::Duration};

use anyhow::Context;
use tokio::{
    select,
    sync::mpsc::{self, Receiver, Sender},
    task::JoinHandle,
    time::sleep,
};

use crate::{compaction::Task, types::RuntimeRef, Result};

pub struct Runner {
    runtime: RuntimeRef,
    segment_duration: Duration,
}

impl Runner {}

pub struct Scheduler {
    runtime: RuntimeRef,

    task_tx: Sender<Task>,
    inused_memory: AtomicU64,
    task_handle: JoinHandle<()>,
    picker_handle: JoinHandle<()>,
}

impl Scheduler {
    pub fn new(runtime: RuntimeRef, config: SchedulerConfig) -> Self {
        let (task_tx, task_rx) = mpsc::channel(config.max_pending_compaction_tasks);
        let task_handle = {
            let rt = runtime.clone();
            runtime.spawn(async move {
                Self::loop_task(rt, task_rx, config.memory_limit).await;
            })
        };
        let picker_handle = {
            let task_tx = task_tx.clone();
            let interval = config.schedule_interval;
            runtime.spawn(async move {
                tokio::select! {
                    _ = sleep(interval) => {
                        // let task = picker.pick_candidate(&ssts).await;
                        // task_tx.send(task).await;
                    }
                }
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

    async fn loop_task(rt: RuntimeRef, mut task_rx: Receiver<Task>, mem_limit: u64) {
        while let Some(i) = task_rx.recv().await {
            println!("got = {:?}", i);
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
            memory_limit: bytesize::gb(2 as u64),
            max_pending_compaction_tasks: 10,
        }
    }
}
