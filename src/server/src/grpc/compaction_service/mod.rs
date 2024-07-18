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

use std::{intrinsics::mir::UnwindResume, sync::Arc};

use analytic_engine::compaction::runner::{local_runner::LocalCompactionRunner, CompactionRunner, CompactionRunnerResult, CompactionRunnerTask};
use async_trait::async_trait;
use common_types::request_id::RequestId;
use horaedbproto::compaction_service::{compaction_service_server::CompactionService, ExecuteCompactionTaskRequest, ExecuteCompactionTaskResponse};
use runtime::Runtime;

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
        request: tonic::Request<super::ExecuteCompactionTaskRequest>,
    ) -> Result<
        tonic::Response<super::ExecuteCompactionTaskResponse>,
        tonic::Status,
    > {
        let execution_response = {
            let compaction_task = generate_compaction_task(request.get_ref());
            let execution_result = self.runner.run(compaction_task).await?;
            generate_execution_response(&execution_result)
        };

        unimplemented!()
    }
}

// Transform request into compaction task
fn generate_compaction_task(request: &ExecuteCompactionTaskRequest) 
        -> CompactionRunnerTask {
    unimplemented!()
}

// Transform compaction result into response
fn generate_execution_response(result: &CompactionRunnerResult) 
        -> ExecuteCompactionTaskResponse {
    unimplemented!()
}
