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
use common_types::request_id::RequestId;

use crate::{
    compaction::{
        executor::{
            CompactionExecutor, CompactionExecutorResult, CompactionExecutorTask, InputContext,
            OutputContext,
        },
        CompactionInputFiles,
    },
    instance::flush_compaction::Result,
    row_iter::IterOptions,
    sst::{
        factory::{ScanOptions, SstWriteOptions},
        writer::{MetaData, SstInfo},
    },
    table::data::TableData,
};

pub(crate) struct CompactionRunnerBuilder;

impl CompactionRunnerBuilder {
    pub fn build(&self, executor: Arc<CompactionExecutor>) -> CompactionRunnerPtr {
        Box::new(LocalCompactionRunner { executor })
    }
}

/// Compaction runner
#[async_trait]
pub(crate) trait CompactionRunner: Send + Sync + 'static {
    async fn run(&self, task: CompactionRunnerTask) -> Result<CompactionRunnerResult>;
}

pub(crate) type CompactionRunnerPtr = Box<dyn CompactionRunner>;

/// Compaction runner task
#[derive(Debug, Clone)]
pub struct CompactionRunnerTask {
    task_key: String,
    executor_task: CompactionExecutorTask,
}

impl CompactionRunnerTask {
    pub fn new(
        request_id: RequestId,
        input_files: CompactionInputFiles,
        table_data: &TableData,
        file_id: u64,
        sst_write_options: SstWriteOptions,
    ) -> Self {
        // Create task key.
        let task_key = table_data.compaction_task_key(file_id);

        // Create executor task.
        let table_options = table_data.table_options();

        let input_ctx = {
            let iter_options = IterOptions {
                batch_size: table_options.num_rows_per_row_group,
            };

            InputContext {
                files: input_files,
                num_rows_per_row_group: table_options.num_rows_per_row_group,
                merge_iter_options: iter_options,
                need_dedup: table_options.need_dedup(),
            }
        };

        let output_ctx = {
            let file_path = table_data.sst_file_path(file_id);
            OutputContext {
                file_path,
                write_options: sst_write_options,
            }
        };

        let executor_task = CompactionExecutorTask {
            request_id,
            schema: table_data.schema(),
            space_id: table_data.space_id,
            table_id: table_data.id,
            sequence: table_data.last_sequence(),
            input_ctx,
            output_ctx,
        };

        Self {
            task_key,
            executor_task,
        }
    }
}

pub struct CompactionRunnerResult(pub CompactionExecutorResult);

/// Local compaction runner impl
pub(crate) struct LocalCompactionRunner {
    executor: Arc<CompactionExecutor>,
}

#[async_trait]
impl CompactionRunner for LocalCompactionRunner {
    async fn run(&self, task: CompactionRunnerTask) -> Result<CompactionRunnerResult> {
        let exec_result = self.executor.execute(task.executor_task).await?;

        Ok(CompactionRunnerResult(exec_result))
    }
}
