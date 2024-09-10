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
use compaction_client::{
    compaction_impl::{build_compaction_client, CompactionClientConfig},
    CompactionClientRef,
};
use generic_error::BoxError;
use snafu::ResultExt;

use super::node_picker::RemoteCompactionNodePickerRef;
use crate::{
    compaction::runner::{CompactionRunner, CompactionRunnerResult, CompactionRunnerTask},
    instance::flush_compaction::{
        BuildCompactionClientFailed, ConvertCompactionTaskResponse, GetCompactionClientFailed,
        PickCompactionNodeFailed, RemoteCompactFailed, Result,
    },
};

pub struct RemoteCompactionRunner {
    pub node_picker: RemoteCompactionNodePickerRef,
}

impl RemoteCompactionRunner {
    async fn get_compaction_client(&self) -> Result<CompactionClientRef> {
        let mut config = CompactionClientConfig::default();
        let node_addr = self
            .node_picker
            .get_compaction_node()
            .await
            .context(PickCompactionNodeFailed)?;
        config.compaction_server_addr = node_addr;

        let client = build_compaction_client(config)
            .await
            .context(BuildCompactionClientFailed)?;
        Ok(client)
    }
}

#[async_trait]
impl CompactionRunner for RemoteCompactionRunner {
    async fn run(&self, task: CompactionRunnerTask) -> Result<CompactionRunnerResult> {
        let client = self
            .get_compaction_client()
            .await
            .box_err()
            .context(GetCompactionClientFailed)?;
        let pb_resp = client
            .execute_compaction_task(task.into())
            .await
            .context(RemoteCompactFailed)?;

        let resp = pb_resp
            .try_into()
            .box_err()
            .context(ConvertCompactionTaskResponse)?;

        Ok(resp)
    }
}
