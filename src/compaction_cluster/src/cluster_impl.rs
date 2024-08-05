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
use async_trait::async_trait;

use analytic_engine::{compaction::runner::{CompactionRunnerRef, CompactionRunnerResult, CompactionRunnerTask}, instance::flush_compaction};
use runtime::Runtime;

use crate::{CompactionCluster, Result};

/// CompactionClusterImpl is an implementation of [`CompactionCluster`].
///
/// Its functions are to:
///  - Handle the action from the HoraeDB;
///  - Handle the heartbeat between CompactionServer and HoraeMeta;
pub struct CompactionClusterImpl {
    // Runtime is to be used for processing heartbeat.
    _runtime: Arc<Runtime>,
    compaction_task_runner: CompactionRunnerRef,
}

#[async_trait]
impl CompactionCluster for CompactionClusterImpl {
    async fn start(&self) -> Result<()> {
        unimplemented!()
    }

    async fn stop(&self) -> Result<()> {
        unimplemented!()
    }

    async fn compact(&self, task: CompactionRunnerTask) -> flush_compaction::Result<CompactionRunnerResult> {
        self.compaction_task_runner.run(task).await
    }
}
