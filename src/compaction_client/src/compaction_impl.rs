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
use generic_error::BoxError;
use horaedbproto::{
    common::ResponseHeader, compaction_service::compaction_service_client::CompactionServiceClient,
};
use logger::info;
use serde::{Deserialize, Serialize};
use snafu::{OptionExt, ResultExt};
use time_ext::ReadableDuration;

use crate::{
    BadResponse, CompactionClient, CompactionClientRef, FailConnect, FailExecuteCompactionTask,
    MissingHeader, Result,
};

type CompactionServiceGrpcClient = CompactionServiceClient<tonic::transport::Channel>;

#[derive(Debug, Deserialize, Clone, Serialize)]
#[serde(default)]
pub struct CompactionClientConfig {
    pub compaction_server_addr: String,
    pub timeout: ReadableDuration,
}

impl Default for CompactionClientConfig {
    fn default() -> Self {
        Self {
            compaction_server_addr: "127.0.0.1:7878".to_string(),
            timeout: ReadableDuration::secs(5),
        }
    }
}

/// Default compaction client impl, will interact with the remote compaction
/// node.
pub struct CompactionClientImpl {
    client: CompactionServiceGrpcClient,
}

impl CompactionClientImpl {
    pub async fn connect(config: CompactionClientConfig) -> Result<Self> {
        let client = {
            let endpoint =
                tonic::transport::Endpoint::from_shared(config.compaction_server_addr.to_string())
                    .box_err()
                    .context(FailConnect {
                        addr: &config.compaction_server_addr,
                    })?
                    .timeout(config.timeout.0);
            CompactionServiceGrpcClient::connect(endpoint)
                .await
                .box_err()
                .context(FailConnect {
                    addr: &config.compaction_server_addr,
                })?
        };

        Ok(Self { client })
    }

    #[inline]
    fn client(&self) -> CompactionServiceGrpcClient {
        self.client.clone()
    }
}

#[async_trait]
impl CompactionClient for CompactionClientImpl {
    async fn execute_compaction_task(
        &self,
        pb_req: horaedbproto::compaction_service::ExecuteCompactionTaskRequest,
    ) -> Result<horaedbproto::compaction_service::ExecuteCompactionTaskResponse> {
        // TODO(leslie): Add request header for ExecuteCompactionTaskRequest.

        info!(
            "Compaction client try to execute compaction task in remote compaction node, req:{:?}",
            pb_req
        );

        let pb_resp = self
            .client()
            .execute_compaction_task(pb_req)
            .await
            .box_err()
            .context(FailExecuteCompactionTask)?
            .into_inner();

        info!(
            "Compaction client finish executing compaction task in remote compaction node, req:{:?}",
            pb_resp
        );

        check_response_header(&pb_resp.header)?;
        Ok(pb_resp)
    }
}

// TODO(leslie): Consider to refactor and reuse the similar function in
// meta_client.
fn check_response_header(header: &Option<ResponseHeader>) -> Result<()> {
    let header = header.as_ref().context(MissingHeader)?;
    if header.code == 0 {
        Ok(())
    } else {
        BadResponse {
            code: header.code,
            msg: header.error.clone(),
        }
        .fail()
    }
}

pub async fn build_compaction_client(
    config: CompactionClientConfig,
) -> Result<CompactionClientRef> {
    let compaction_client = CompactionClientImpl::connect(config).await?;
    Ok(Arc::new(compaction_client))
}
