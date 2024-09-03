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

// Flush and compaction logic of instance

use std::{cmp, collections::Bound, fmt};

use common_types::{
    projected_schema::{ProjectedSchema, RowProjectorBuilder},
    record_batch::{FetchedRecordBatch, FetchedRecordBatchBuilder},
    request_id::RequestId,
    row::RowViewOnBatch,
    time::TimeRange,
    SequenceNumber,
};
use futures::{
    channel::{mpsc, mpsc::channel},
    stream, SinkExt, StreamExt, TryStreamExt,
};
use generic_error::{BoxError, GenericError};
use logger::{debug, error, info};
use macros::define_result;
use runtime::RuntimeRef;
use snafu::{Backtrace, ResultExt, Snafu};
use time_ext::{self, ReadableDuration};
use tokio::{sync::oneshot, time::Instant};
use wal::manager::WalLocation;

use crate::{
    instance::{
        self, reorder_memtable::Reorder, serial_executor::TableFlushScheduler, SpaceStoreRef,
    },
    manifest::meta_edit::{
        AlterOptionsMeta, AlterSchemaMeta, MetaEdit, MetaEditRequest, MetaUpdate, VersionEditMeta,
    },
    memtable::{ColumnarIterPtr, MemTableRef, ScanContext, ScanRequest},
    sst::{
        factory::{self, SstWriteOptions},
        file::{FileMeta, Level},
        writer::MetaData,
    },
    table::{
        data::{self, TableDataRef},
        version::{FlushableMemTables, MemTableState, SamplingMemTable},
        version_edit::AddFile,
    },
    table_options::StorageFormatHint,
};

const DEFAULT_CHANNEL_SIZE: usize = 5;

#[derive(Debug, Snafu)]
#[snafu(visibility = "pub")]
pub enum Error {
    #[snafu(display("Failed to store version edit, err:{}", source))]
    StoreVersionEdit { source: GenericError },

    #[snafu(display("Failed to store schema edit, err:{}", source))]
    StoreSchemaEdit { source: GenericError },

    #[snafu(display(
        "Failed to purge wal, wal_location:{:?}, sequence:{}",
        wal_location,
        sequence
    ))]
    PurgeWal {
        wal_location: WalLocation,
        sequence: SequenceNumber,
        source: wal::manager::Error,
    },

    #[snafu(display("Failed to build mem table iterator, source:{}", source))]
    InvalidMemIter { source: GenericError },

    #[snafu(display("Failed to reorder mem table iterator, source:{}", source))]
    ReorderMemIter {
        source: crate::instance::reorder_memtable::Error,
    },

    #[snafu(display(
        "Failed to create sst writer, storage_format_hint:{:?}, err:{}",
        storage_format_hint,
        source,
    ))]
    CreateSstWriter {
        storage_format_hint: StorageFormatHint,
        source: factory::Error,
    },

    #[snafu(display("Failed to write sst, file_path:{}, source:{}", path, source))]
    WriteSst { path: String, source: GenericError },

    #[snafu(display(
        "Background flush failed, cannot write more data, retry_count:{}, err:{}.\nBacktrace:\n{}",
        retry_count,
        msg,
        backtrace
    ))]
    BackgroundFlushFailed {
        msg: String,
        retry_count: usize,
        backtrace: Backtrace,
    },

    #[snafu(display("Failed to build merge iterator, mgs:{}, err:{}", msg, source))]
    BuildMergeIterator {
        msg: String,
        source: crate::row_iter::merge::Error,
    },

    #[snafu(display("Failed to do manual compaction, err:{}", source))]
    ManualCompactFailed {
        source: crate::compaction::WaitError,
    },

    #[snafu(display("Failed to split record batch, source:{}", source))]
    SplitRecordBatch { source: GenericError },

    #[snafu(display("Failed to read sst meta, source:{}", source))]
    ReadSstMeta {
        source: crate::sst::meta_data::Error,
    },

    #[snafu(display("Failed to send to channel, source:{}", source))]
    ChannelSend { source: mpsc::SendError },

    #[snafu(display("Runtime join error, source:{}", source))]
    RuntimeJoin { source: runtime::Error },

    #[snafu(display("Other failure, msg:{}.\nBacktrace:\n{:?}", msg, backtrace))]
    Other { msg: String, backtrace: Backtrace },

    #[snafu(display("Failed to run flush job, msg:{:?}, err:{}", msg, source))]
    FlushJobWithCause {
        msg: Option<String>,
        source: GenericError,
    },

    #[snafu(display("Failed to run flush job, msg:{:?}.\nBacktrace:\n{}", msg, backtrace))]
    FlushJobNoCause {
        msg: Option<String>,
        backtrace: Backtrace,
    },

    #[snafu(display("Failed to alloc file id, err:{}", source))]
    AllocFileId { source: data::Error },

    #[snafu(display("Failed to convert compaction task response, err:{}", source))]
    ConvertCompactionTaskResponse { source: GenericError },

    #[snafu(display("Failed to execute compaction task remotely, err:{}", source))]
    RemoteCompact { source: cluster::Error },
}

define_result!(Error);

/// Options to flush single table.
#[derive(Default)]
pub struct TableFlushOptions {
    /// Flush result sender.
    ///
    /// Default is None.
    pub res_sender: Option<oneshot::Sender<Result<()>>>,
    /// Max retry limit After flush failed
    ///
    /// Default is 0
    pub max_retry_flush_limit: usize,
}

impl fmt::Debug for TableFlushOptions {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TableFlushOptions")
            .field("res_sender", &self.res_sender.is_some())
            .finish()
    }
}

/// Request to flush single table.
pub struct TableFlushRequest {
    /// Table to flush.
    pub table_data: TableDataRef,
    /// Max sequence number to flush (inclusive).
    pub max_sequence: SequenceNumber,

    /// We may suggest new primary keys in preflush. if suggestion happened, we
    /// need to ensure data is in new order.
    need_reorder: bool,
}

#[derive(Clone)]
pub struct Flusher {
    pub space_store: SpaceStoreRef,

    pub runtime: RuntimeRef,
    pub write_sst_max_buffer_size: usize,
    /// If the interval is set, it will generate a [`FlushTask`] with min flush
    /// interval check.
    pub min_flush_interval_ms: Option<u64>,
}

struct FlushTask {
    space_store: SpaceStoreRef,
    table_data: TableDataRef,
    runtime: RuntimeRef,
    write_sst_max_buffer_size: usize,
    // If the interval is set, it will be used to check whether flush is too frequent.
    min_flush_interval_ms: Option<u64>,
}

/// The checker to determine whether a flush is frequent.
struct FrequentFlushChecker {
    min_flush_interval_ms: u64,
    last_flush_time_ms: u64,
}

impl FrequentFlushChecker {
    #[inline]
    fn is_frequent_flush(&self) -> bool {
        let now = time_ext::current_time_millis();
        self.last_flush_time_ms + self.min_flush_interval_ms > now
    }
}

impl Flusher {
    /// Schedule a flush request.
    pub async fn schedule_flush(
        &self,
        flush_scheduler: &mut TableFlushScheduler,
        table_data: &TableDataRef,
        opts: TableFlushOptions,
    ) -> Result<()> {
        debug!(
            "Instance flush table, table_data:{:?}, flush_opts:{:?}",
            table_data, opts
        );

        self.schedule_table_flush(flush_scheduler, table_data.clone(), opts, false)
            .await
    }

    /// Do flush and wait for it to finish.
    pub async fn do_flush(
        &self,
        flush_scheduler: &mut TableFlushScheduler,
        table_data: &TableDataRef,
        opts: TableFlushOptions,
    ) -> Result<()> {
        info!(
            "Instance flush table, table_data:{:?}, flush_opts:{:?}",
            table_data, opts
        );

        self.schedule_table_flush(flush_scheduler, table_data.clone(), opts, true)
            .await
    }

    /// Schedule table flush request to background workers
    async fn schedule_table_flush(
        &self,
        flush_scheduler: &mut TableFlushScheduler,
        table_data: TableDataRef,
        opts: TableFlushOptions,
        block_on: bool,
    ) -> Result<()> {
        let flush_task = FlushTask {
            table_data: table_data.clone(),
            space_store: self.space_store.clone(),
            runtime: self.runtime.clone(),
            write_sst_max_buffer_size: self.write_sst_max_buffer_size,
            min_flush_interval_ms: self.min_flush_interval_ms,
        };
        let flush_job = async move { flush_task.run().await };

        flush_scheduler
            .flush_sequentially(flush_job, block_on, opts, &self.runtime, table_data.clone())
            .await
    }
}

impl FlushTask {
    /// Each table can only have one running flush task at the same time, which
    /// should be ensured by the caller.
    async fn run(&self) -> Result<()> {
        let large_enough = self.table_data.should_flush_table(false);
        if !large_enough && self.is_frequent_flush() {
            debug!(
                "Ignore flush task for too frequent flush of small memtable, table:{}",
                self.table_data.name
            );

            return Ok(());
        }

        let instant = Instant::now();
        let flush_req = self.preprocess_flush(&self.table_data).await?;

        let current_version = self.table_data.current_version();
        let mems_to_flush = current_version.pick_memtables_to_flush(flush_req.max_sequence);

        if mems_to_flush.is_empty() {
            return Ok(());
        }

        let request_id = RequestId::next_id();

        // Start flush duration timer.
        let local_metrics = self.table_data.metrics.local_flush_metrics();
        let _timer = local_metrics.start_flush_timer();
        self.dump_memtables(request_id.clone(), &mems_to_flush, flush_req.need_reorder)
            .await
            .box_err()
            .context(FlushJobWithCause {
                msg: Some(format!(
                    "table:{}, table_id:{}, request_id:{request_id}",
                    self.table_data.name, self.table_data.id
                )),
            })?;

        self.table_data
            .set_last_flush_time(time_ext::current_time_millis());

        info!(
            "Instance flush memtables done, table:{}, table_id:{}, request_id:{}, cost:{}ms",
            self.table_data.name,
            self.table_data.id,
            request_id,
            instant.elapsed().as_millis()
        );

        Ok(())
    }

    fn is_frequent_flush(&self) -> bool {
        if let Some(min_flush_interval_ms) = self.min_flush_interval_ms {
            let checker = FrequentFlushChecker {
                min_flush_interval_ms,
                last_flush_time_ms: self.table_data.last_flush_time(),
            };
            checker.is_frequent_flush()
        } else {
            false
        }
    }

    async fn preprocess_flush(&self, table_data: &TableDataRef) -> Result<TableFlushRequest> {
        let current_version = table_data.current_version();
        let mut last_sequence = table_data.last_sequence();
        // Switch (freeze) all mutable memtables. And update segment duration if
        // suggestion is returned.
        let mut need_reorder = table_data.enable_layered_memtable;
        if let Some(suggest_segment_duration) = current_version.suggest_duration() {
            info!(
                "Update segment duration, table:{}, table_id:{}, segment_duration:{:?}",
                table_data.name, table_data.id, suggest_segment_duration
            );
            assert!(!suggest_segment_duration.is_zero());

            if let Some(pk_idx) = current_version.suggest_primary_key() {
                need_reorder = true;
                let mut schema = table_data.schema();
                info!(
                    "Update primary key, table:{}, table_id:{}, old:{:?}, new:{:?}",
                    table_data.name,
                    table_data.id,
                    schema.primary_key_indexes(),
                    pk_idx,
                );

                schema.reset_primary_key_indexes(pk_idx);
                let pre_schema_version = schema.version();
                let meta_update = MetaUpdate::AlterSchema(AlterSchemaMeta {
                    space_id: table_data.space_id,
                    table_id: table_data.id,
                    schema,
                    pre_schema_version,
                });
                let edit_req = MetaEditRequest {
                    shard_info: table_data.shard_info,
                    meta_edit: MetaEdit::Update(meta_update),
                    table_catalog_info: table_data.table_catalog_info.clone(),
                };
                self.space_store
                    .manifest
                    .apply_edit(edit_req)
                    .await
                    .context(StoreSchemaEdit)?;
            }

            let mut new_table_opts = (*table_data.table_options()).clone();
            new_table_opts.segment_duration = Some(ReadableDuration(suggest_segment_duration));

            let edit_req = {
                let meta_update = MetaUpdate::AlterOptions(AlterOptionsMeta {
                    space_id: table_data.space_id,
                    table_id: table_data.id,
                    options: new_table_opts.clone(),
                });
                MetaEditRequest {
                    shard_info: table_data.shard_info,
                    meta_edit: MetaEdit::Update(meta_update),
                    table_catalog_info: table_data.table_catalog_info.clone(),
                }
            };
            self.space_store
                .manifest
                .apply_edit(edit_req)
                .await
                .context(StoreVersionEdit)?;

            // Now the segment duration is applied, we can stop sampling and freeze the
            // sampling memtable.
            if let Some(seq) = current_version.freeze_sampling_memtable() {
                last_sequence = seq.max(last_sequence);
            }
        } else if let Some(seq) = current_version.switch_memtables() {
            last_sequence = seq.max(last_sequence);
        }

        info!("Try to trigger memtable flush of table, table:{}, table_id:{}, max_memtable_id:{}, last_sequence:{last_sequence}",
            table_data.name, table_data.id, table_data.last_memtable_id());

        // Try to flush all memtables of current table
        Ok(TableFlushRequest {
            table_data: table_data.clone(),
            max_sequence: last_sequence,
            need_reorder,
        })
    }

    /// This will write picked memtables [FlushableMemTables] to level 0 sst
    /// files. Sampling memtable may be dumped into multiple sst file according
    /// to the sampled segment duration.
    ///
    /// Memtables will be removed after all of them are dumped. The max sequence
    /// number in dumped memtables will be sent to the [WalManager].
    async fn dump_memtables(
        &self,
        request_id: RequestId,
        mems_to_flush: &FlushableMemTables,
        need_reorder: bool,
    ) -> Result<()> {
        let local_metrics = self.table_data.metrics.local_flush_metrics();
        let mut files_to_level0 = Vec::with_capacity(mems_to_flush.memtables.len());
        let mut flushed_sequence = 0;
        let mut sst_num = 0;

        // process sampling memtable and frozen memtable
        if let Some(sampling_mem) = &mems_to_flush.sampling_mem {
            if let Some(seq) = self
                .dump_sampling_memtable(
                    request_id.clone(),
                    sampling_mem,
                    &mut files_to_level0,
                    need_reorder,
                )
                .await?
            {
                flushed_sequence = seq;
                sst_num += files_to_level0.len();
                for add_file in &files_to_level0 {
                    local_metrics.observe_sst_size(add_file.file.size);
                }
            }
        }
        for mem in &mems_to_flush.memtables {
            let file = self
                .dump_normal_memtable(request_id.clone(), mem, need_reorder)
                .await?;
            if let Some(file) = file {
                let sst_size = file.size;
                files_to_level0.push(AddFile {
                    level: Level::MIN,
                    file,
                });

                // Set flushed sequence to max of the last_sequence of memtables.
                flushed_sequence = cmp::max(flushed_sequence, mem.last_sequence());

                sst_num += 1;
                // Collect sst size metrics.
                local_metrics.observe_sst_size(sst_size);
            }
        }

        // Collect sst num metrics.
        local_metrics.observe_sst_num(sst_num);

        info!(
            "Instance flush memtables to output, table:{}, table_id:{}, request_id:{}, mems_to_flush:{:?}, files_to_level0:{:?}, flushed_sequence:{}",
            self.table_data.name,
            self.table_data.id,
            request_id,
            mems_to_flush,
            files_to_level0,
            flushed_sequence
        );

        // Persist the flush result to manifest.
        let edit_req = {
            let edit_meta = VersionEditMeta {
                space_id: self.table_data.space_id,
                table_id: self.table_data.id,
                flushed_sequence,
                files_to_add: files_to_level0.clone(),
                files_to_delete: vec![],
                mems_to_remove: mems_to_flush.ids(),
                max_file_id: 0,
            };
            let meta_update = MetaUpdate::VersionEdit(edit_meta);
            MetaEditRequest {
                shard_info: self.table_data.shard_info,
                meta_edit: MetaEdit::Update(meta_update),
                table_catalog_info: self.table_data.table_catalog_info.clone(),
            }
        };
        // Update manifest and remove immutable memtables
        self.space_store
            .manifest
            .apply_edit(edit_req)
            .await
            .context(StoreVersionEdit)?;

        // Mark sequence <= flushed_sequence to be deleted.
        let table_location = self.table_data.table_location();
        let wal_location =
            instance::create_wal_location(table_location.id, table_location.shard_info);
        self.space_store
            .wal_manager
            .mark_delete_entries_up_to(wal_location, flushed_sequence)
            .await
            .context(PurgeWal {
                wal_location,
                sequence: flushed_sequence,
            })?;

        Ok(())
    }

    /// Flush rows in sampling memtable to multiple ssts according to segment
    /// duration.
    ///
    /// Returns flushed sequence.
    async fn dump_sampling_memtable(
        &self,
        request_id: RequestId,
        sampling_mem: &SamplingMemTable,
        files_to_level0: &mut Vec<AddFile>,
        need_reorder: bool,
    ) -> Result<Option<SequenceNumber>> {
        let (min_key, max_key) = match (sampling_mem.mem.min_key(), sampling_mem.mem.max_key()) {
            (Some(min_key), Some(max_key)) => (min_key, max_key),
            _ => {
                // the memtable is empty and nothing needs flushing.
                return Ok(None);
            }
        };

        let max_sequence = sampling_mem.mem.last_sequence();
        let time_ranges = sampling_mem.sampler.ranges();

        info!("Flush sampling memtable, table_id:{:?}, table_name:{:?}, request_id:{}, sampling memtable time_ranges:{:?}",
            self.table_data.id, self.table_data.name, request_id, time_ranges);

        let mut batch_record_senders = Vec::with_capacity(time_ranges.len());
        let mut sst_handlers = Vec::with_capacity(time_ranges.len());
        let mut file_ids = Vec::with_capacity(time_ranges.len());

        let sst_write_options = SstWriteOptions {
            storage_format_hint: self.table_data.table_options().storage_format_hint,
            num_rows_per_row_group: self.table_data.table_options().num_rows_per_row_group,
            compression: self.table_data.table_options().compression,
            max_buffer_size: self.write_sst_max_buffer_size,
            column_stats: Default::default(),
        };

        for time_range in &time_ranges {
            let (batch_record_sender, batch_record_receiver) =
                channel::<Result<FetchedRecordBatch>>(DEFAULT_CHANNEL_SIZE);
            let file_id = self
                .table_data
                .alloc_file_id(&self.space_store.manifest)
                .await
                .context(AllocFileId)?;

            let sst_file_path = self.table_data.sst_file_path(file_id);
            // TODO: `min_key` & `max_key` should be figured out when writing sst.
            let sst_meta = MetaData {
                min_key: min_key.clone(),
                max_key: max_key.clone(),
                time_range: *time_range,
                max_sequence,
                schema: self.table_data.schema(),
            };

            let store = self.space_store.clone();
            let storage_format_hint = self.table_data.table_options().storage_format_hint;
            let sst_write_options = sst_write_options.clone();
            let request_id = request_id.clone();

            // spawn build sst
            let handler = self.runtime.spawn(async move {
                let mut writer = store
                    .sst_factory
                    .create_writer(
                        &sst_write_options,
                        &sst_file_path,
                        store.store_picker(),
                        Level::MIN,
                    )
                    .await
                    .context(CreateSstWriter {
                        storage_format_hint,
                    })?;

                let sst_info = writer
                    .write(
                        request_id,
                        &sst_meta,
                        Box::new(batch_record_receiver.map_err(|e| Box::new(e) as _)),
                    )
                    .await
                    .map_err(|e| {
                        error!("Failed to write sst file, meta:{:?}, err:{}", sst_meta, e);
                        Box::new(e) as _
                    })
                    .with_context(|| WriteSst {
                        path: sst_file_path.to_string(),
                    })?;

                Ok((sst_info, sst_meta))
            });

            batch_record_senders.push(batch_record_sender);
            sst_handlers.push(handler);
            file_ids.push(file_id);
        }

        let iter = build_mem_table_iter(sampling_mem.mem.clone(), &self.table_data)?;
        let timestamp_idx = self.table_data.schema().timestamp_index();
        if need_reorder {
            let schema = self.table_data.schema();
            let primary_key_indexes = schema.primary_key_indexes();
            let reorder = Reorder {
                iter,
                schema: self.table_data.schema(),
                order_by_col_indexes: primary_key_indexes.to_vec(),
            };
            let mut stream = reorder.into_stream().await.context(ReorderMemIter)?;
            while let Some(data) = stream.next().await {
                for (idx, record_batch) in split_record_batch_with_time_ranges(
                    data.box_err().context(InvalidMemIter)?,
                    &time_ranges,
                    timestamp_idx,
                )?
                .into_iter()
                .enumerate()
                {
                    if !record_batch.is_empty() {
                        batch_record_senders[idx]
                            .send(Ok(record_batch))
                            .await
                            .context(ChannelSend)?;
                    }
                }
            }
        } else {
            for data in iter {
                for (idx, record_batch) in split_record_batch_with_time_ranges(
                    data.box_err().context(InvalidMemIter)?,
                    &time_ranges,
                    timestamp_idx,
                )?
                .into_iter()
                .enumerate()
                {
                    if !record_batch.is_empty() {
                        batch_record_senders[idx]
                            .send(Ok(record_batch))
                            .await
                            .context(ChannelSend)?;
                    }
                }
            }
        }

        batch_record_senders.clear();

        for (idx, sst_handler) in sst_handlers.into_iter().enumerate() {
            let info_and_metas = sst_handler.await.context(RuntimeJoin)?;
            let (sst_info, sst_meta) = info_and_metas?;
            files_to_level0.push(AddFile {
                level: Level::MIN,
                file: FileMeta {
                    id: file_ids[idx],
                    size: sst_info.file_size as u64,
                    row_num: sst_info.row_num as u64,
                    time_range: sst_info.time_range,
                    max_seq: sst_meta.max_sequence,
                    storage_format: sst_info.storage_format,
                    associated_files: vec![sst_info.meta_path],
                },
            })
        }

        Ok(Some(max_sequence))
    }

    /// Flush rows in normal (non-sampling) memtable to at most one sst file.
    async fn dump_normal_memtable(
        &self,
        request_id: RequestId,
        memtable_state: &MemTableState,
        need_reorder: bool,
    ) -> Result<Option<FileMeta>> {
        let (min_key, max_key) = match (memtable_state.mem.min_key(), memtable_state.mem.max_key())
        {
            (Some(min_key), Some(max_key)) => (min_key, max_key),
            _ => {
                // the memtable is empty and nothing needs flushing.
                return Ok(None);
            }
        };
        let max_sequence = memtable_state.last_sequence();
        let sst_meta = MetaData {
            min_key,
            max_key,
            time_range: memtable_state.aligned_time_range,
            max_sequence,
            schema: self.table_data.schema(),
        };

        // Alloc file id for next sst file
        let file_id = self
            .table_data
            .alloc_file_id(&self.space_store.manifest)
            .await
            .context(AllocFileId)?;

        let sst_file_path = self.table_data.sst_file_path(file_id);
        let storage_format_hint = self.table_data.table_options().storage_format_hint;
        let sst_write_options = SstWriteOptions {
            storage_format_hint,
            num_rows_per_row_group: self.table_data.table_options().num_rows_per_row_group,
            compression: self.table_data.table_options().compression,
            max_buffer_size: self.write_sst_max_buffer_size,
            column_stats: Default::default(),
        };
        let mut writer = self
            .space_store
            .sst_factory
            .create_writer(
                &sst_write_options,
                &sst_file_path,
                self.space_store.store_picker(),
                Level::MIN,
            )
            .await
            .context(CreateSstWriter {
                storage_format_hint,
            })?;

        let iter = build_mem_table_iter(memtable_state.mem.clone(), &self.table_data)?;

        let record_batch_stream = if need_reorder {
            let schema = self.table_data.schema();
            let primary_key_indexes = schema.primary_key_indexes().to_vec();
            let reorder = Reorder {
                iter,
                schema,
                order_by_col_indexes: primary_key_indexes,
            };
            Box::new(
                reorder
                    .into_stream()
                    .await
                    .context(ReorderMemIter)?
                    .map(|batch| batch.box_err()),
            ) as _
        } else {
            Box::new(stream::iter(iter).map(|batch| batch.box_err())) as _
        };

        let sst_info = writer
            .write(request_id, &sst_meta, record_batch_stream)
            .await
            .box_err()
            .with_context(|| WriteSst {
                path: sst_file_path.to_string(),
            })?;

        // update sst metadata by built info.

        Ok(Some(FileMeta {
            id: file_id,
            row_num: sst_info.row_num as u64,
            size: sst_info.file_size as u64,
            time_range: sst_info.time_range,
            max_seq: memtable_state.last_sequence(),
            storage_format: sst_info.storage_format,
            associated_files: vec![sst_info.meta_path],
        }))
    }
}

fn split_record_batch_with_time_ranges(
    record_batch: FetchedRecordBatch,
    time_ranges: &[TimeRange],
    timestamp_idx: usize,
) -> Result<Vec<FetchedRecordBatch>> {
    let fetched_schema = record_batch.schema();
    let primary_key_indexes = record_batch.primary_key_indexes();
    let mut builders: Vec<FetchedRecordBatchBuilder> = (0..time_ranges.len())
        .map(|_| {
            let primary_key_indexes = primary_key_indexes.map(|idxs| idxs.to_vec());
            FetchedRecordBatchBuilder::new(fetched_schema.clone(), primary_key_indexes)
        })
        .collect();

    for row_idx in 0..record_batch.num_rows() {
        let datum = record_batch.column(timestamp_idx).datum(row_idx);
        let timestamp = datum.as_timestamp().unwrap();
        let mut idx = None;
        for (i, time_range) in time_ranges.iter().enumerate() {
            if time_range.contains(timestamp) {
                idx = Some(i);
                break;
            }
        }

        if let Some(idx) = idx {
            let view = RowViewOnBatch {
                record_batch: &record_batch,
                row_idx,
            };
            builders[idx]
                .append_row_view(&view)
                .box_err()
                .context(SplitRecordBatch)?;
        } else {
            panic!(
                "Record timestamp is not in time_ranges, timestamp:{timestamp:?}, time_ranges:{time_ranges:?}"
            );
        }
    }
    let mut ret = Vec::with_capacity(builders.len());
    for mut builder in builders {
        ret.push(builder.build().box_err().context(SplitRecordBatch)?);
    }
    Ok(ret)
}

fn build_mem_table_iter(
    memtable: MemTableRef,
    table_data: &TableDataRef,
) -> Result<ColumnarIterPtr> {
    let scan_ctx = ScanContext::default();
    let projected_schema = ProjectedSchema::no_projection(table_data.schema());
    let fetched_schema = projected_schema.to_record_schema_with_key();
    let primary_key_indexes = fetched_schema.primary_key_idx().to_vec();
    let fetched_schema = fetched_schema.into_record_schema();
    let table_schema = projected_schema.table_schema().clone();
    let row_projector_builder =
        RowProjectorBuilder::new(fetched_schema, table_schema, Some(primary_key_indexes));
    let scan_req = ScanRequest {
        start_user_key: Bound::Unbounded,
        end_user_key: Bound::Unbounded,
        sequence: common_types::MAX_SEQUENCE_NUMBER,
        row_projector_builder,
        need_dedup: table_data.dedup(),
        reverse: false,
        metrics_collector: None,
        time_range: TimeRange::min_to_max(),
    };
    memtable
        .scan(scan_ctx, scan_req)
        .box_err()
        .context(InvalidMemIter)
}

#[cfg(test)]
mod tests {

    use common_types::{
        tests::{
            build_fetched_record_batch_by_rows, build_row, build_row_opt,
            check_record_batch_with_key_with_rows,
        },
        time::TimeRange,
    };

    use super::FrequentFlushChecker;
    use crate::instance::flush_compaction::split_record_batch_with_time_ranges;

    #[test]
    fn test_split_record_batch_with_time_ranges() {
        let rows0 = vec![build_row(
            b"binary key",
            20,
            10.0,
            "string value",
            1000,
            1_000_000,
        )];
        let rows1 = vec![build_row(
            b"binary key1",
            120,
            11.0,
            "string value 1",
            1000,
            1_000_000,
        )];
        let rows2 = vec![
            build_row_opt(
                b"binary key2",
                220,
                None,
                Some("string value 2"),
                Some(1000),
                None,
            ),
            build_row_opt(b"binary key3", 250, Some(13.0), None, None, Some(1_000_000)),
        ];

        let rows = vec![rows0.clone(), rows1.clone(), rows2.clone()]
            .into_iter()
            .flatten()
            .collect();
        let record_batch_with_key = build_fetched_record_batch_by_rows(rows);
        let column_num = record_batch_with_key.num_columns();
        let time_ranges = vec![
            TimeRange::new_unchecked_for_test(0, 100),
            TimeRange::new_unchecked_for_test(100, 200),
            TimeRange::new_unchecked_for_test(200, 300),
        ];

        let timestamp_idx = 1;
        let rets =
            split_record_batch_with_time_ranges(record_batch_with_key, &time_ranges, timestamp_idx)
                .unwrap();

        check_record_batch_with_key_with_rows(&rets[0], rows0.len(), column_num, rows0);
        check_record_batch_with_key_with_rows(&rets[1], rows1.len(), column_num, rows1);
        check_record_batch_with_key_with_rows(&rets[2], rows2.len(), column_num, rows2);
    }

    #[test]
    fn test_frequent_flush() {
        let now = time_ext::current_time_millis();
        let cases = vec![
            (now - 1000, 100, false),
            (now - 1000, 2000, true),
            (now - 10000, 200, false),
            (now - 2000, 2000, false),
            (now + 2000, 1000, true),
        ];
        for (last_flush_time_ms, min_flush_interval_ms, expect) in cases {
            let checker = FrequentFlushChecker {
                min_flush_interval_ms,
                last_flush_time_ms,
            };

            assert_eq!(expect, checker.is_frequent_flush());
        }
    }
}
