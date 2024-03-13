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

pub mod local_runner;

use std::sync::Arc;

use async_trait::async_trait;
use common_types::{request_id::RequestId, schema::Schema, SequenceNumber};
use object_store::Path;
use table_engine::table::TableId;

use crate::{
    compaction::CompactionInputFiles,
    instance::flush_compaction::Result,
    row_iter::IterOptions,
    space::SpaceId,
    sst::{
        factory::SstWriteOptions,
        writer::{MetaData, SstInfo},
    },
    table::data::TableData,
};

/// Compaction runner
#[async_trait]
pub trait CompactionRunner: Send + Sync + 'static {
    async fn run(&self, task: CompactionRunnerTask) -> Result<CompactionRunnerResult>;
}

pub type CompactionRunnerPtr = Box<dyn CompactionRunner>;
pub type CompactionRunnerRef = Arc<dyn CompactionRunner>;

/// Compaction runner task
#[derive(Debug, Clone)]
pub struct CompactionRunnerTask {
    // TODO: unused now, will be used in remote compaction.
    #[allow(unused)]
    pub task_key: String,
    /// Trace id for this operation
    pub request_id: RequestId,

    pub schema: Schema,
    pub space_id: SpaceId,
    pub table_id: TableId,
    pub sequence: SequenceNumber,

    /// Input context
    pub input_ctx: InputContext,
    /// Output context
    pub output_ctx: OutputContext,
}

impl CompactionRunnerTask {
    pub(crate) fn new(
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

        Self {
            task_key,
            request_id,
            schema: table_data.schema(),
            space_id: table_data.space_id,
            table_id: table_data.id,
            sequence: table_data.last_sequence(),
            input_ctx,
            output_ctx,
        }
    }
}

pub struct CompactionRunnerResult {
    pub output_file_path: Path,
    pub sst_info: SstInfo,
    pub sst_meta: MetaData,
}

#[derive(Debug, Clone)]
pub struct InputContext {
    /// Input sst files in this compaction
    pub files: CompactionInputFiles,
    pub num_rows_per_row_group: usize,
    pub merge_iter_options: IterOptions,
    pub need_dedup: bool,
}

#[derive(Debug, Clone)]
pub struct OutputContext {
    /// Output sst file path
    pub file_path: Path,
    /// Output sst write context
    pub write_options: SstWriteOptions,
}
