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
use generic_error::BoxError;
use logger::info;
use snafu::ResultExt;

use super::{local_runner::LocalCompactionRunner, node_picker::RemoteCompactionNodePickerRef};
use crate::{
    compaction::runner::{
        remote_client::{build_compaction_client, CompactionClientConfig, CompactionClientRef},
        CompactionRunner, CompactionRunnerResult, CompactionRunnerTask,
    },
    instance::flush_compaction::{
        self, BuildCompactionClientFailed, ConvertCompactionTaskResponse,
        GetCompactionClientFailed, PickCompactionNodeFailed, Result,
    },
};

pub struct RemoteCompactionRunner {
    pub node_picker: RemoteCompactionNodePickerRef,

    pub fallback_local_when_failed: bool,
    /// Responsible for executing compaction task locally if fail to remote
    /// compact when `fallback_local_when_failed` is true, used for better fault
    /// tolerance.
    pub local_compaction_runner: LocalCompactionRunner,
}

impl RemoteCompactionRunner {
    async fn get_compaction_client(&self) -> Result<CompactionClientRef> {
        let mut config = CompactionClientConfig::default();
        let endpoint = self
            .node_picker
            .get_compaction_node()
            .await
            .context(PickCompactionNodeFailed)?;
        config.compaction_server_addr = make_formatted_endpoint(&endpoint);

        let client = build_compaction_client(config)
            .await
            .context(BuildCompactionClientFailed)?;
        Ok(client)
    }

    async fn local_compact(&self, task: CompactionRunnerTask) -> Result<CompactionRunnerResult> {
        self.local_compaction_runner.run(task).await
    }
}

#[async_trait]
impl CompactionRunner for RemoteCompactionRunner {
    /// Run the compaction task either on a remote node or fall back to local
    /// compaction.
    async fn run(&self, task: CompactionRunnerTask) -> Result<CompactionRunnerResult> {
        let client = self
            .get_compaction_client()
            .await
            .box_err()
            .context(GetCompactionClientFailed);

        let pb_resp = match client {
            Ok(client) => match client.execute_compaction_task(task.clone().into()).await {
                Ok(resp) => resp,
                Err(e) => {
                    if !self.fallback_local_when_failed {
                        return Err(flush_compaction::Error::RemoteCompactFailed { source: e });
                    }

                    info!(
                        "The compaction task falls back to local because of error:{}",
                        e
                    );
                    return self.local_compact(task).await;
                }
            },
            Err(e) => {
                if !self.fallback_local_when_failed {
                    return Err(e);
                }

                info!(
                    "The compaction task falls back to local because of error:{}",
                    e
                );
                return self.local_compact(task).await;
            }
        };

        let resp = pb_resp
            .try_into()
            .box_err()
            .context(ConvertCompactionTaskResponse)?;

        Ok(resp)
    }
}

fn make_formatted_endpoint(endpoint: &str) -> String {
    format!("http://{endpoint}")
}
