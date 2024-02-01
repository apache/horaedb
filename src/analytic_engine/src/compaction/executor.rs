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

use std::{collections::HashMap, sync::Arc};

use common_types::{
    projected_schema::{ProjectedSchema, RowProjectorBuilder},
    request_id::RequestId,
    schema::Schema,
    SequenceNumber,
};
use generic_error::BoxError;
use object_store::Path;
use runtime::Runtime;
use snafu::ResultExt;
use table_engine::{predicate::Predicate, table::TableId};

use crate::{
    compaction::CompactionInputFiles,
    instance::flush_compaction::{
        BuildMergeIterator, CreateSstWriter, ReadSstMeta, Result, WriteSst,
    },
    row_iter::{
        self,
        dedup::DedupIterator,
        merge::{MergeBuilder, MergeConfig},
        IterOptions,
    },
    space::SpaceId,
    sst::{
        factory::{ColumnStats, FactoryRef, ObjectStorePickerRef, ScanOptions, SstWriteOptions},
        meta_data::{SstMetaData, SstMetaReader},
        writer::{MetaData, SstInfo},
    },
    table::data::TableData,
    Config, ScanType, SstReadOptionsBuilder,
};

const MAX_RECORD_BATCHES_IN_FLIGHT_WHEN_COMPACTION_READ: usize = 64;

/// Executor carrying for actual compaction work
pub struct CompactionExecutor {
    runtime: Arc<Runtime>,
    scan_options: ScanOptions,
    /// Sst factory
    sst_factory: FactoryRef,
    /// Store picker for persisting sst
    store_picker: ObjectStorePickerRef,
}

impl CompactionExecutor {
    pub fn new(
        runtime: Arc<Runtime>,
        config: &Config,
        sst_factory: FactoryRef,
        store_picker: ObjectStorePickerRef,
    ) -> Self {
        let scan_options = ScanOptions {
            background_read_parallelism: 1,
            max_record_batches_in_flight: MAX_RECORD_BATCHES_IN_FLIGHT_WHEN_COMPACTION_READ,
            num_streams_to_prefetch: config.num_streams_to_prefetch,
        };

        Self {
            runtime,
            scan_options,
            sst_factory,
            store_picker,
        }
    }

    pub async fn execute(&self, task: CompactionExecutorTask) -> Result<CompactionExecutorResult> {
        let projected_schema = ProjectedSchema::no_projection(task.schema.clone());
        let predicate = Arc::new(Predicate::empty());
        // FIXME: draft tmp
        let sst_read_options_builder = SstReadOptionsBuilder::new(
            ScanType::Compaction,
            self.scan_options.clone(),
            None,
            task.input_ctx.num_rows_per_row_group,
            predicate,
            None,
            self.runtime.clone(),
        );
        let fetched_schema = projected_schema.to_record_schema_with_key();
        let primary_key_indexes = fetched_schema.primary_key_idx().to_vec();
        let fetched_schema = fetched_schema.into_record_schema();
        let table_schema = projected_schema.table_schema().clone();
        let row_projector_builder =
            RowProjectorBuilder::new(fetched_schema, table_schema, Some(primary_key_indexes));

        let request_id = RequestId::from(task.request_id);
        let merge_iter = {
            let mut builder = MergeBuilder::new(MergeConfig {
                request_id: request_id.clone(),
                metrics_collector: None,
                // no need to set deadline for compaction
                deadline: None,
                space_id: task.space_id,
                table_id: task.table_id,
                sequence: task.sequence,
                projected_schema,
                predicate: Arc::new(Predicate::empty()),
                sst_read_options_builder: sst_read_options_builder.clone(),
                sst_factory: &self.sst_factory,
                store_picker: &self.store_picker,
                merge_iter_options: task.input_ctx.merge_iter_options.clone(),
                need_dedup: task.input_ctx.need_dedup,
                reverse: false,
            });
            // Add all ssts in compaction input to builder.
            builder
                .mut_ssts_of_level(task.input_ctx.files.level)
                .extend_from_slice(&task.input_ctx.files.files);
            builder.build().await.context(BuildMergeIterator {
                msg: format!("table_id:{}, space_id:{}", task.table_id, task.space_id),
            })?
        };

        let record_batch_stream = if task.input_ctx.need_dedup {
            row_iter::record_batch_with_key_iter_to_stream(DedupIterator::new(
                request_id.clone(),
                merge_iter,
                task.input_ctx.merge_iter_options,
            ))
        } else {
            row_iter::record_batch_with_key_iter_to_stream(merge_iter)
        };

        // TODO: eliminate the duplicated building of `SstReadOptions`.
        let sst_read_options = sst_read_options_builder.build(row_projector_builder);
        let (sst_meta, column_stats) = {
            let meta_reader = SstMetaReader {
                space_id: task.space_id,
                table_id: task.table_id,
                factory: self.sst_factory.clone(),
                read_opts: sst_read_options,
                store_picker: self.store_picker.clone(),
            };
            let sst_metas = meta_reader
                .fetch_metas(&task.input_ctx.files.files)
                .await
                .context(ReadSstMeta)?;

            let column_stats = collect_column_stats_from_meta_datas(&sst_metas);
            let merged_meta =
                MetaData::merge(sst_metas.into_iter().map(MetaData::from), task.schema);
            (merged_meta, column_stats)
        };

        let mut sst_writer = self
            .sst_factory
            .create_writer(
                &task.output_ctx.write_options,
                &task.output_ctx.file_path,
                &self.store_picker,
                task.input_ctx.files.output_level,
            )
            .await
            .context(CreateSstWriter {
                storage_format_hint: task.output_ctx.write_options.storage_format_hint,
            })?;

        let sst_info = sst_writer
            .write(request_id, &sst_meta, record_batch_stream)
            .await
            .box_err()
            .with_context(|| WriteSst {
                path: task.output_ctx.file_path.to_string(),
            })?;

        Ok(CompactionExecutorResult { sst_info, sst_meta })
    }
}

/// Compaction executor task
#[derive(Debug, Clone)]
pub struct CompactionExecutorTask {
    /// Trace id for this operation
    pub(crate) request_id: RequestId,

    pub(crate) schema: Schema,
    pub(crate) space_id: SpaceId,
    pub(crate) table_id: TableId,
    pub(crate) sequence: SequenceNumber,

    /// Input context
    pub(crate) input_ctx: InputContext,
    /// Output context
    pub(crate) output_ctx: OutputContext,
}

impl CompactionExecutorTask {
    fn new(
        request_id: RequestId,
        input_files: CompactionInputFiles,
        table_data: &TableData,
        file_id: u64,
        scan_options: &ScanOptions,
        sst_write_options: SstWriteOptions,
    ) -> Self {
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

pub struct CompactionExecutorResult {
    pub sst_info: SstInfo,
    pub sst_meta: MetaData,
}

#[derive(Debug, Clone)]
pub(crate) struct InputContext {
    /// Input sst files in this compaction
    pub(crate) files: CompactionInputFiles,
    pub(crate) num_rows_per_row_group: usize,
    pub(crate) merge_iter_options: IterOptions,
    pub(crate) need_dedup: bool,
}

#[derive(Debug, Clone)]
pub(crate) struct OutputContext {
    /// Output sst file path
    pub(crate) file_path: Path,
    /// Output sst write context
    pub(crate) write_options: SstWriteOptions,
}

/// Collect the column stats from a batch of sst meta data.
fn collect_column_stats_from_meta_datas(metas: &[SstMetaData]) -> HashMap<String, ColumnStats> {
    let mut low_cardinality_counts: HashMap<String, usize> = HashMap::new();
    for meta_data in metas {
        let SstMetaData::Parquet(meta_data) = meta_data;
        if let Some(column_values) = &meta_data.column_values {
            for (col_idx, val_set) in column_values.iter().enumerate() {
                let low_cardinality = val_set.is_some();
                if low_cardinality {
                    let col_name = meta_data.schema.column(col_idx).name.clone();
                    low_cardinality_counts
                        .entry(col_name)
                        .and_modify(|v| *v += 1)
                        .or_insert(1);
                }
            }
        }
    }

    // Only the column whose cardinality is low in all the metas is a
    // low-cardinality column.
    // TODO: shall we merge all the distinct values of the column to check whether
    // the cardinality is still thought to be low?
    let low_cardinality_cols = low_cardinality_counts
        .into_iter()
        .filter_map(|(col_name, cnt)| {
            (cnt == metas.len()).then_some((
                col_name,
                ColumnStats {
                    low_cardinality: true,
                },
            ))
        });
    HashMap::from_iter(low_cardinality_cols)
}
