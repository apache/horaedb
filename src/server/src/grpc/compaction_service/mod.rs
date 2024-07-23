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

#![allow(dead_code)]

use std::sync::Arc;

use analytic_engine::compaction::runner::{local_runner::LocalCompactionRunner, CompactionRunner, CompactionRunnerTask};
use async_trait::async_trait;
use generic_error::BoxError;
use horaedbproto::compaction_service::{compaction_service_server::CompactionService, ExecuteCompactionTaskRequest, ExecuteCompactionTaskResponse};
use runtime::Runtime;
use snafu::ResultExt;
use tonic::{Request, Response, Status};
use error::{ErrWithCause, StatusCode};

mod error;

#[derive(Clone)]
pub struct CompactionServiceImpl {
    runtime: Arc<Runtime>,
    runner: LocalCompactionRunner,
}

#[async_trait]
impl CompactionService for CompactionServiceImpl {
    async fn execute_compaction_task(
        &self,
        request: Request<ExecuteCompactionTaskRequest>,
    ) -> Result<Response<ExecuteCompactionTaskResponse>, Status> {
        let request: Result<CompactionRunnerTask, error::Error> = request.into_inner().try_into().box_err().context(ErrWithCause { 
            code: StatusCode::BadRequest,
            msg: "fail to convert the execute compaction task request",
        });

        match request {
            Ok(request) => {
                let request_id = request.request_id.clone();
                let _res = self.runner.run(request).await
                    .box_err().with_context(|| {
                        ErrWithCause {
                            code: StatusCode::Internal,
                            msg: format!("fail to compact task, request:{request_id}")
                        }
                });
            },
            Err(_e) => {}
        }

        unimplemented!()
    }
}
