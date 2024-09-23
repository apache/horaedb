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

//! Remote compaction node picker.

use std::sync::Arc;

use async_trait::async_trait;
use macros::define_result;
use meta_client::{types::FetchCompactionNodeRequest, MetaClientRef};
use serde::{Deserialize, Serialize};
use snafu::{ResultExt, Snafu};

#[derive(Clone, Debug, Deserialize, Serialize)]
pub enum NodePicker {
    // Local node picker that specifies the local endpoint.
    // The endpoint in the form `addr:port`.
    Local(String),
    Remote,
}

#[async_trait]
pub trait CompactionNodePicker: Send + Sync {
    /// Get the addr of the remote compaction node.
    async fn get_compaction_node(&self) -> Result<String>;
}

pub type RemoteCompactionNodePickerRef = Arc<dyn CompactionNodePicker>;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Meta client fetch compaciton node failed, err:{source}."))]
    FetchCompactionNodeFailure { source: meta_client::Error },
}

define_result!(Error);

/// RemoteCompactionNodePickerImpl is an implementation of
/// [`CompactionNodePicker`] based [`MetaClient`].
pub struct RemoteCompactionNodePickerImpl {
    pub meta_client: MetaClientRef,
}

#[async_trait]
impl CompactionNodePicker for RemoteCompactionNodePickerImpl {
    /// Get proper remote compaction node info for compaction offload with meta
    /// client.
    async fn get_compaction_node(&self) -> Result<String> {
        let req = FetchCompactionNodeRequest::default();
        let resp = self
            .meta_client
            .fetch_compaction_node(req)
            .await
            .context(FetchCompactionNodeFailure)?;

        let compaction_node_addr = resp.endpoint;
        Ok(compaction_node_addr)
    }
}

/// LocalCompactionNodePickerImpl is an implementation of
/// [`CompactionNodePicker`] mainly used for testing.
pub struct LocalCompactionNodePickerImpl {
    pub endpoint: String,
}

#[async_trait]
impl CompactionNodePicker for LocalCompactionNodePickerImpl {
    /// Return the local addr and port of grpc service.
    async fn get_compaction_node(&self) -> Result<String> {
        Ok(self.endpoint.clone())
    }
}
