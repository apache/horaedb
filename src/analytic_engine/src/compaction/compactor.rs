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

use std::cmp;

use common_types::request_id::RequestId;
use logger::{debug, info};
use snafu::ResultExt;

use crate::{
    compaction::{
        executor::CompactionExecutorResult,
        runner::{CompactionRunner, CompactionRunnerResult, CompactionRunnerTask},
        CompactionInputFiles, CompactionTask, ExpiredFiles,
    },
    instance::flush_compaction::{AllocFileId, Other, Result, StoreVersionEdit},
    manifest::{
        meta_edit::{MetaEdit, MetaEditRequest, MetaUpdate, VersionEditMeta},
        ManifestRef,
    },
    sst::{factory::SstWriteOptions, file::FileMeta},
    table::{
        data::TableData,
        version_edit::{AddFile, DeleteFile},
    },
};

pub(crate) struct Compactor {
    /// Sst files compaction runner
    runner: Box<dyn CompactionRunner>,

    /// Manifest (or meta) stores meta data of the engine instance.
    manifest: ManifestRef,
}

impl Compactor {
    pub fn new(runner: Box<dyn CompactionRunner>, manifest: ManifestRef) -> Self {
        Self { runner, manifest }
    }

    pub async fn compact_table(
        &self,
        request_id: RequestId,
        table_data: &TableData,
        task: &CompactionTask,
        sst_write_options: &SstWriteOptions,
    ) -> Result<()> {
        debug!(
            "Begin compact table, table_name:{}, id:{}, task:{:?}",
            table_data.name, table_data.id, task
        );

        if task.is_empty() {
            // Nothing to compact.
            debug!(
                "Nothing to compact, table_name:{}, id:{}, task:{:?}",
                table_data.name, table_data.id, task
            );

            return Ok(());
        }

        let inputs = task.inputs();
        let mut edit_meta = VersionEditMeta {
            space_id: table_data.space_id,
            table_id: table_data.id,
            flushed_sequence: 0,
            // Use the number of compaction inputs as the estimated number of files to add.
            files_to_add: Vec::with_capacity(inputs.len()),
            files_to_delete: vec![],
            mems_to_remove: vec![],
            max_file_id: 0,
        };

        for files in task.expired() {
            self.delete_expired_files(table_data, &request_id, files, &mut edit_meta);
        }

        for input in inputs {
            self.compact_input_files(
                request_id.clone(),
                table_data,
                input,
                sst_write_options,
                &mut edit_meta,
            )
            .await?;
        }

        if !table_data.allow_compaction() {
            return Other {
                msg: format!(
                    "Table status is not ok, unable to update manifest, table:{}, table_id:{}",
                    table_data.name, table_data.id
                ),
            }
            .fail();
        }

        let edit_req = {
            let meta_update = MetaUpdate::VersionEdit(edit_meta.clone());
            MetaEditRequest {
                shard_info: table_data.shard_info,
                meta_edit: MetaEdit::Update(meta_update),
                table_catalog_info: table_data.table_catalog_info.clone(),
            }
        };
        self.manifest
            .apply_edit(edit_req)
            .await
            .context(StoreVersionEdit)?;

        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn compact_input_files(
        &self,
        request_id: RequestId,
        table_data: &TableData,
        input: &CompactionInputFiles,
        sst_write_options: &SstWriteOptions,
        edit_meta: &mut VersionEditMeta,
    ) -> Result<()> {
        debug!(
            "Compact input files, table_name:{}, id:{}, input::{:?}, edit_meta:{:?}",
            table_data.name, table_data.id, input, edit_meta
        );

        if input.files.is_empty() {
            return Ok(());
        }

        // Metrics
        let _timer = table_data.metrics.start_compaction_timer();
        table_data
            .metrics
            .compaction_observe_sst_num(input.files.len());
        let mut sst_size = 0;
        let mut sst_row_num = 0;
        for file in &input.files {
            sst_size += file.size();
            sst_row_num += file.row_num();
        }
        table_data
            .metrics
            .compaction_observe_input_sst_size(sst_size);
        table_data
            .metrics
            .compaction_observe_input_sst_row_num(sst_row_num);

        // TODO: seems should be debug log
        info!(
            "Begin to compact files of table, request_id:{}, table:{}, table_id:{}, input_files:{:?}",
            request_id, table_data.name, table_data.id, input.files,
        );

        // Alloc file id for the merged sst.
        let file_id = table_data
            .alloc_file_id(&self.manifest)
            .await
            .context(AllocFileId)?;

        let task = CompactionRunnerTask::new(
            request_id.clone(),
            input.clone(),
            table_data,
            file_id,
            sst_write_options.clone(),
        );

        let task_result = self.runner.run(task).await?;
        let CompactionRunnerResult(CompactionExecutorResult {
            sst_info,
            sst_meta,
            output_file_path,
        }) = task_result;

        let sst_file_size = sst_info.file_size as u64;
        let sst_row_num = sst_info.row_num as u64;
        table_data
            .metrics
            .compaction_observe_output_sst_size(sst_file_size);
        table_data
            .metrics
            .compaction_observe_output_sst_row_num(sst_row_num);

        // TODO: seems should be debug log
        info!(
            "Finish to compact files of table, request_id:{}, table:{}, table_id:{}, output_path:{}, input_files:{:?}, sst_meta:{:?}, sst_info:{:?}",
            request_id,
            table_data.name,
            table_data.id,
            output_file_path,
            input.files,
            sst_meta,
            sst_info,
        );

        // Update the flushed sequence number.
        edit_meta.flushed_sequence = cmp::max(sst_meta.max_sequence, edit_meta.flushed_sequence);

        // Store updates to edit_meta.
        edit_meta.files_to_delete.reserve(input.files.len());
        // The compacted file can be deleted later.
        for file in &input.files {
            edit_meta.files_to_delete.push(DeleteFile {
                level: input.level,
                file_id: file.id(),
            });
        }

        // Add the newly created file to meta.
        edit_meta.files_to_add.push(AddFile {
            level: input.output_level,
            file: FileMeta {
                id: file_id,
                size: sst_file_size,
                row_num: sst_row_num,
                max_seq: sst_meta.max_sequence,
                time_range: sst_meta.time_range,
                storage_format: sst_info.storage_format,
                associated_files: vec![sst_info.meta_path],
            },
        });

        Ok(())
    }

    pub fn delete_expired_files(
        &self,
        table_data: &TableData,
        request_id: &RequestId,
        expired: &ExpiredFiles,
        edit_meta: &mut VersionEditMeta,
    ) {
        if !expired.files.is_empty() {
            info!(
                "Instance try to delete expired files, table:{}, table_id:{}, request_id:{}, level:{}, files:{:?}",
                table_data.name, table_data.id, request_id, expired.level, expired.files,
            );
        }

        let files = &expired.files;
        edit_meta.files_to_delete.reserve(files.len());
        for file in files {
            edit_meta.files_to_delete.push(DeleteFile {
                level: expired.level,
                file_id: file.id(),
            });
        }
    }
}
