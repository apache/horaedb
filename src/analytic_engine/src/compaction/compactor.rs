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

use std::{cmp, sync::Arc};

use common_types::{
    projected_schema::{ProjectedSchema, RowProjectorBuilder},
    request_id::RequestId,
};
use logger::{debug, info};
use runtime::Runtime;
use snafu::ResultExt;
use table_engine::predicate::Predicate;
use wal::manager::WalManagerRef;

use crate::{
    compaction::{
        executor::CompactionExecutorResult,
        runner::{CompactionRunner, CompactionRunnerResult, CompactionRunnerTask},
        CompactionInputFiles, CompactionTask, ExpiredFiles,
    },
    instance::flush_compaction::{
        AllocFileId, CreateSstWriter, Other, Result, StoreVersionEdit, WriteSst,
    },
    manifest::{
        meta_edit::{MetaEdit, MetaEditRequest, MetaUpdate, VersionEditMeta},
        ManifestRef,
    },
    row_iter::IterOptions,
    sst::{
        factory::{FactoryRef, ObjectStorePickerRef, ScanOptions, SstWriteOptions},
        file::FileMeta,
        meta_data::cache::MetaCacheRef,
    },
    table::{
        data::TableData,
        version_edit::{AddFile, DeleteFile},
    },
    ScanType, SstReadOptionsBuilder,
};

struct Compactor {
    /// Manifest (or meta) stores meta data of the engine instance.
    manifest: ManifestRef,

    runner: Box<dyn CompactionRunner>,

    /// Object store picker for persisting data.
    store_picker: ObjectStorePickerRef,

    /// Sst factory.
    sst_factory: FactoryRef,

    meta_cache: Option<MetaCacheRef>,
}

impl Compactor {
    pub(crate) async fn compact_table(
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

        if task.is_empty() {
            // Nothing to compact.
            return Ok(());
        }

        for files in task.expired() {
            self.delete_expired_files(table_data, &request_id, files, &mut edit_meta);
        }

        info!(
            "Try do compaction for table:{}#{}, estimated input files size:{}, input files number:{}",
            table_data.name,
            table_data.id,
            task.estimated_total_input_file_size(),
            task.num_compact_files(),
        );

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
    pub(crate) async fn compact_input_files(
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

        // metrics
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

        // Alloc file id for the merged sst.
        let file_id = table_data
            .alloc_file_id(&self.manifest)
            .await
            .context(AllocFileId)?;

        let task = CompactionRunnerTask::new(
            request_id,
            input.clone(),
            table_data,
            file_id,
            sst_write_options.clone(),
        );

        let task_result = self.runner.run(task).await?;
        let CompactionRunnerResult(CompactionExecutorResult { sst_info, sst_meta }) = task_result;

        let sst_file_size = sst_info.file_size as u64;
        let sst_row_num = sst_info.row_num as u64;
        table_data
            .metrics
            .compaction_observe_output_sst_size(sst_file_size);
        table_data
            .metrics
            .compaction_observe_output_sst_row_num(sst_row_num);

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

    pub(crate) fn delete_expired_files(
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
