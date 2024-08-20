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

// Compaction rpc service implementation.

use std::sync::Arc;

use analytic_engine::compaction::runner::{CompactionRunnerRef, CompactionRunnerTask};
use async_trait::async_trait;
use cluster::ClusterRef;
use error::{build_err_header, build_ok_header, ErrWithCause, StatusCode};
use generic_error::BoxError;
use horaedbproto::compaction_service::{
    compaction_service_server::CompactionService, ExecResult, ExecuteCompactionTaskRequest,
    ExecuteCompactionTaskResponse,
};
use proxy::instance::InstanceRef;
use runtime::Runtime;
use snafu::ResultExt;
use tonic::{Request, Response, Status};

mod error;

/// Builder for [CompactionServiceImpl]
pub struct Builder {
    pub cluster: ClusterRef,
    pub instance: InstanceRef,
    pub runtime: Arc<Runtime>,
    pub compaction_runner: CompactionRunnerRef,
}

impl Builder {
    pub fn build(self) -> CompactionServiceImpl {
        let Self {
            cluster,
            instance,
            runtime,
            compaction_runner,
        } = self;

        CompactionServiceImpl {
            cluster,
            instance,
            runtime,
            compaction_runner,
        }
    }
}

#[derive(Clone)]
pub struct CompactionServiceImpl {
    pub cluster: ClusterRef,
    pub instance: InstanceRef,
    pub runtime: Arc<Runtime>,
    pub compaction_runner: CompactionRunnerRef,
}

#[async_trait]
impl CompactionService for CompactionServiceImpl {
    async fn execute_compaction_task(
        &self,
        request: Request<ExecuteCompactionTaskRequest>,
    ) -> Result<Response<ExecuteCompactionTaskResponse>, Status> {
        let request: Result<CompactionRunnerTask, error::Error> = request
            .into_inner()
            .try_into()
            .box_err()
            .context(ErrWithCause {
                code: StatusCode::BadRequest,
                msg: "fail to convert the execute compaction task request",
            });

        let mut resp: ExecuteCompactionTaskResponse = ExecuteCompactionTaskResponse::default();
        match request {
            Ok(task) => {
                let request_id = task.request_id.clone();
                let res = self
                    .compaction_runner
                    .run(task)
                    .await
                    .box_err()
                    .with_context(|| ErrWithCause {
                        code: StatusCode::Internal,
                        msg: format!("fail to compact task, request:{request_id}"),
                    });

                match res {
                    Ok(res) => {
                        resp.header = Some(build_ok_header());
                        resp.result = Some(ExecResult {
                            output_file_path: res.output_file_path.into(),
                            sst_info: Some(res.sst_info.into()),
                            sst_meta: Some(res.sst_meta.into()),
                        });
                        // TODO(leslie): Add status.
                    }
                    Err(e) => {
                        resp.header = Some(build_err_header(e));
                    }
                }
            }
            Err(e) => {
                resp.header = Some(build_err_header(e));
            }
        }

        Ok(Response::new(resp))
    }
}
