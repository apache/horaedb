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
use cluster::ClusterRef;
use generic_error::BoxError;
use snafu::ResultExt;

use crate::{
    compaction::runner::{CompactionRunner, CompactionRunnerResult, CompactionRunnerTask},
    instance::flush_compaction::{ConvertCompactionTaskResponse, RemoteCompact, Result},
};

pub struct RemoteCompactionRunner {
    pub cluster: ClusterRef,
}

#[async_trait]
impl CompactionRunner for RemoteCompactionRunner {
    async fn run(&self, task: CompactionRunnerTask) -> Result<CompactionRunnerResult> {
        let pb_resp = self
            .cluster
            .compact(task.into())
            .await
            .context(RemoteCompact)?;

        let resp = pb_resp
            .try_into()
            .box_err()
            .context(ConvertCompactionTaskResponse)?;

        Ok(resp)
    }
}
