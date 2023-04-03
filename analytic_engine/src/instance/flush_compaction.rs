// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

// Flush and compaction logic of instance

use std::{cmp, collections::Bound, sync::Arc};

use common_types::{
    projected_schema::ProjectedSchema,
    record_batch::{RecordBatchWithKey, RecordBatchWithKeyBuilder},
    request_id::RequestId,
    row::RowViewOnBatch,
    time::TimeRange,
    SequenceNumber,
};
use common_util::{
    config::ReadableDuration,
    define_result,
    error::{BoxError, GenericError},
    runtime::Runtime,
    time,
};
use futures::{
    channel::{mpsc, mpsc::channel},
    future::try_join_all,
    stream, SinkExt, TryStreamExt,
};
use log::{debug, error, info};
use snafu::{Backtrace, ResultExt, Snafu};
use table_engine::{predicate::Predicate, table::Result as TableResult};
use tokio::sync::oneshot;
use wal::manager::WalLocation;

use crate::{
    compaction::{
        CompactionInputFiles, CompactionTask, ExpiredFiles, TableCompactionRequest, WaitError,
    },
    instance::{
        self,
        write_worker::{self, CompactTableCommand, FlushTableCommand, WorkerLocal},
        Instance, SpaceStore,
    },
    manifest::meta_update::{AlterOptionsMeta, MetaUpdate, MetaUpdateRequest, VersionEditMeta},
    memtable::{ColumnarIterPtr, MemTableRef, ScanContext, ScanRequest},
    row_iter::{
        self,
        dedup::DedupIterator,
        merge::{MergeBuilder, MergeConfig},
        IterOptions,
    },
    space::SpaceAndTable,
    sst::{
        factory::{self, ReadFrequency, ScanOptions, SstReadOptions, SstWriteOptions},
        file::FileMeta,
        meta_data::SstMetaReader,
        writer::{MetaData, RecordBatchStream},
    },
    table::{
        data::{TableData, TableDataRef},
        version::{FlushableMemTables, MemTableState, SamplingMemTable},
        version_edit::{AddFile, DeleteFile, VersionEdit},
    },
    table_options::StorageFormatHint,
};

const DEFAULT_CHANNEL_SIZE: usize = 5;

#[derive(Debug, Snafu)]
#[snafu(visibility = "pub")]
pub enum Error {
    #[snafu(display("Failed to store version edit, err:{}", source))]
    StoreVersionEdit { source: GenericError },

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

    #[snafu(display("Background flush failed, cannot schedule flush task, err:{}", source))]
    BackgroundFlushFailed {
        source: crate::instance::write_worker::Error,
    },

    #[snafu(display("Failed to send flush command, err:{}", source))]
    SendFlushCmd {
        source: crate::instance::write_worker::Error,
    },

    #[snafu(display("Failed to send compact command, err:{}", source))]
    SendCompactCmd {
        source: crate::instance::write_worker::Error,
    },

    #[snafu(display("Failed to build merge iterator, table:{}, err:{}", table, source))]
    BuildMergeIterator {
        table: String,
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
    RuntimeJoin { source: common_util::runtime::Error },

    #[snafu(display("Other failure, msg:{}.\nBacktrace:\n{:?}", msg, backtrace))]
    Other { msg: String, backtrace: Backtrace },

    #[snafu(display("Unknown flush policy.\nBacktrace:\n{:?}", backtrace))]
    UnknownPolicy { backtrace: Backtrace },
}

define_result!(Error);

/// Options to flush single table.
#[derive(Debug)]
pub struct TableFlushOptions {
    /// Flush result sender.
    ///
    /// Default is None.
    pub res_sender: Option<oneshot::Sender<TableResult<()>>>,
    /// Schedule a compaction request after flush.
    ///
    /// Default is true.
    pub compact_after_flush: bool,
    /// Whether to block on write thread.
    ///
    /// Default is false.
    pub block_on_write_thread: bool,
}

impl Default for TableFlushOptions {
    fn default() -> Self {
        Self {
            res_sender: None,
            compact_after_flush: true,
            block_on_write_thread: false,
        }
    }
}

/// Request to flush single table.
pub struct TableFlushRequest {
    /// Table to flush.
    pub table_data: TableDataRef,
    /// Max sequence number to flush (inclusive).
    pub max_sequence: SequenceNumber,
}

impl Instance {
    /// Flush this table.
    pub async fn flush_table(
        table_data: TableDataRef,
        flush_opts: TableFlushOptions,
    ) -> Result<()> {
        info!(
            "Instance flush table, table_data:{:?}, flush_opts:{:?}",
            table_data, flush_opts
        );

        // Create a oneshot channel to send/receive flush result.
        let (tx, rx) = oneshot::channel();
        let cmd = FlushTableCommand {
            table_data: table_data.clone(),
            flush_opts,
            tx,
        };

        // Actual work is done in flush_table_in_worker().
        write_worker::process_command_in_write_worker(cmd.into_command(), &table_data, rx)
            .await
            .context(SendFlushCmd)
    }

    /// Compact the table manually.
    pub async fn manual_compact_table(&self, space_table: &SpaceAndTable) -> Result<()> {
        info!("Instance compact table, space_table:{:?}", space_table);

        // Create a oneshot channel to send/receive result from write worker.
        let (tx, rx) = oneshot::channel();
        let (compact_tx, compact_rx) = oneshot::channel();
        // Create a oneshot channel to send/receive compaction result.
        let cmd = CompactTableCommand {
            table_data: space_table.table_data().clone(),
            waiter: Some(compact_tx),
            tx,
        };

        // The write worker will call schedule_table_compaction().
        write_worker::process_command_in_write_worker(
            cmd.into_command(),
            space_table.table_data(),
            rx,
        )
        .await
        .context(SendCompactCmd)?;

        // Now wait for compaction done, if the sender has been dropped, we convert it
        // into Error::Canceled.
        compact_rx
            .await
            .unwrap_or(Err(WaitError::Canceled))
            .context(ManualCompactFailed)
    }

    /// Flush given table in write worker thread.
    pub(crate) async fn flush_table_in_worker(
        self: &Arc<Self>,
        worker_local: &mut WorkerLocal,
        table_data: &TableDataRef,
        opts: TableFlushOptions,
    ) -> Result<()> {
        let flush_req = self.preprocess_flush(worker_local, table_data).await?;

        self.schedule_table_flush(worker_local, flush_req, opts)
            .await
    }

    async fn preprocess_flush(
        &self,
        worker_local: &mut WorkerLocal,
        table_data: &TableDataRef,
    ) -> Result<TableFlushRequest> {
        worker_local
            .ensure_permission(
                &table_data.name,
                table_data.id.as_u64() as usize,
                self.write_group_worker_num,
            )
            .context(BackgroundFlushFailed)?;

        let current_version = table_data.current_version();
        let last_sequence = table_data.last_sequence();
        // Switch (freeze) all mutable memtables. And update segment duration if
        // suggestion is returned.
        if let Some(suggest_segment_duration) =
            current_version.switch_memtables_or_suggest_duration(worker_local)
        {
            info!(
                "Update segment duration, table:{}, table_id:{}, segment_duration:{:?}",
                table_data.name, table_data.id, suggest_segment_duration
            );
            assert!(!suggest_segment_duration.is_zero());

            let mut new_table_opts = (*table_data.table_options()).clone();
            new_table_opts.segment_duration = Some(ReadableDuration(suggest_segment_duration));

            // Now persist the new options, the `worker_local` ensure there is no race
            // condition.
            let update_req = {
                let meta_update = MetaUpdate::AlterOptions(AlterOptionsMeta {
                    space_id: table_data.space_id,
                    table_id: table_data.id,
                    options: new_table_opts.clone(),
                });
                MetaUpdateRequest {
                    shard_info: table_data.shard_info,
                    meta_update,
                }
            };
            self.space_store
                .manifest
                .store_update(update_req)
                .await
                .context(StoreVersionEdit)?;

            table_data.set_table_options(worker_local, new_table_opts);

            // Now the segment duration is applied, we can stop sampling and freeze the
            // sampling memtable.
            current_version.freeze_sampling(worker_local);
        }

        info!("Try to trigger memtable flush of table, table:{}, table_id:{}, max_memtable_id:{}, last_sequence:{}",
            table_data.name, table_data.id, table_data.last_memtable_id(), last_sequence);

        // Try to flush all memtables of current table
        Ok(TableFlushRequest {
            table_data: table_data.clone(),
            max_sequence: last_sequence,
        })
    }

    /// Schedule table flush request to background workers
    async fn schedule_table_flush(
        self: &Arc<Self>,
        worker_local: &mut WorkerLocal,
        flush_req: TableFlushRequest,
        opts: TableFlushOptions,
    ) -> Result<()> {
        // TODO(yingwen): Store pending flush reqs and retry flush on recoverable error,
        // or try to recover from background error
        let table_data = flush_req.table_data.clone();
        let table = table_data.name.clone();

        let instance = self.clone();
        let flush_job = async move { instance.flush_memtables(&flush_req).await };

        let compact_req = TableCompactionRequest::no_waiter(
            table_data.clone(),
            Some(worker_local.compaction_notifier()),
        );
        let instance = self.clone();

        if opts.compact_after_flush {
            // Schedule compaction if flush completed successfully.
            let on_flush_success = async move {
                instance.schedule_table_compaction(compact_req).await;
            };

            worker_local
                .flush_sequentially(
                    table,
                    &table_data.metrics,
                    flush_job,
                    on_flush_success,
                    opts.block_on_write_thread,
                    opts.res_sender,
                )
                .await
                .context(BackgroundFlushFailed)
        } else {
            worker_local
                .flush_sequentially(
                    table,
                    &table_data.metrics,
                    flush_job,
                    async {},
                    opts.block_on_write_thread,
                    opts.res_sender,
                )
                .await
                .context(BackgroundFlushFailed)
        }
    }

    /// Each table can only have one running flush job.
    async fn flush_memtables(&self, flush_req: &TableFlushRequest) -> Result<()> {
        let TableFlushRequest {
            table_data,
            max_sequence,
        } = flush_req;

        let current_version = table_data.current_version();
        let mems_to_flush = current_version.pick_memtables_to_flush(*max_sequence);

        if mems_to_flush.is_empty() {
            return Ok(());
        }

        let request_id = RequestId::next_id();
        info!(
            "Instance try to flush memtables, table:{}, table_id:{}, request_id:{}, mems_to_flush:{:?}",
            table_data.name, table_data.id, request_id, mems_to_flush
        );

        // Start flush duration timer.
        let local_metrics = table_data.metrics.local_flush_metrics();
        let _timer = local_metrics.start_flush_timer();
        self.dump_memtables(table_data, request_id, &mems_to_flush)
            .await?;

        table_data.set_last_flush_time(time::current_time_millis());

        info!(
            "Instance flush memtables done, table:{}, table_id:{}, request_id:{}",
            table_data.name, table_data.id, request_id
        );

        Ok(())
    }

    /// This will write picked memtables [FlushableMemTables] to level 0 sst
    /// files. Sampling memtable may be dumped into multiple sst file according
    /// to the sampled segment duration.
    ///
    /// Memtables will be removed after all of them are dumped. The max sequence
    /// number in dumped memtables will be sent to the [WalManager].
    async fn dump_memtables(
        &self,
        table_data: &TableData,
        request_id: RequestId,
        mems_to_flush: &FlushableMemTables,
    ) -> Result<()> {
        let local_metrics = table_data.metrics.local_flush_metrics();
        let mut files_to_level0 = Vec::with_capacity(mems_to_flush.memtables.len());
        let mut flushed_sequence = 0;
        let mut sst_num = 0;

        // process sampling memtable and frozen memtable
        if let Some(sampling_mem) = &mems_to_flush.sampling_mem {
            if let Some(seq) = self
                .dump_sampling_memtable(table_data, request_id, sampling_mem, &mut files_to_level0)
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
                .dump_normal_memtable(table_data, request_id, mem)
                .await?;
            if let Some(file) = file {
                let sst_size = file.size;
                files_to_level0.push(AddFile { level: 0, file });

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
            table_data.name,
            table_data.id,
            request_id,
            mems_to_flush,
            files_to_level0,
            flushed_sequence
        );

        // Persist the flush result to manifest.
        let update_req = {
            let edit_meta = VersionEditMeta {
                space_id: table_data.space_id,
                table_id: table_data.id,
                flushed_sequence,
                files_to_add: files_to_level0.clone(),
                files_to_delete: vec![],
            };
            let meta_update = MetaUpdate::VersionEdit(edit_meta);
            MetaUpdateRequest {
                shard_info: table_data.shard_info,
                meta_update,
            }
        };
        self.space_store
            .manifest
            .store_update(update_req)
            .await
            .context(StoreVersionEdit)?;

        // Edit table version to remove dumped memtables.
        let mems_to_remove = mems_to_flush.ids();
        let edit = VersionEdit {
            flushed_sequence,
            mems_to_remove,
            files_to_add: files_to_level0,
            files_to_delete: vec![],
        };
        table_data.current_version().apply_edit(edit);

        // Mark sequence <= flushed_sequence to be deleted.
        let table_location = table_data.table_location();
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
        table_data: &TableData,
        request_id: RequestId,
        sampling_mem: &SamplingMemTable,
        files_to_level0: &mut Vec<AddFile>,
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
            table_data.id,table_data.name, request_id, time_ranges);

        let mut batch_record_senders = Vec::with_capacity(time_ranges.len());
        let mut sst_handlers = Vec::with_capacity(time_ranges.len());
        let mut file_ids = Vec::with_capacity(time_ranges.len());

        let sst_write_options = SstWriteOptions {
            storage_format_hint: table_data.table_options().storage_format_hint,
            num_rows_per_row_group: table_data.table_options().num_rows_per_row_group,
            compression: table_data.table_options().compression,
            max_buffer_size: self.write_sst_max_buffer_size,
        };

        for time_range in &time_ranges {
            let (batch_record_sender, batch_record_receiver) =
                channel::<Result<RecordBatchWithKey>>(DEFAULT_CHANNEL_SIZE);
            let file_id = table_data.alloc_file_id();
            let sst_file_path = table_data.set_sst_file_path(file_id);

            // TODO: `min_key` & `max_key` should be figured out when writing sst.
            let sst_meta = MetaData {
                min_key: min_key.clone(),
                max_key: max_key.clone(),
                time_range: *time_range,
                max_sequence,
                schema: table_data.schema(),
            };

            let store = self.space_store.clone();
            let storage_format_hint = table_data.table_options().storage_format_hint;
            let sst_write_options = sst_write_options.clone();

            // spawn build sst
            let handler = self.runtimes.bg_runtime.spawn(async move {
                let mut writer = store
                    .sst_factory
                    .create_writer(&sst_write_options, &sst_file_path, store.store_picker())
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

        let iter = build_mem_table_iter(sampling_mem.mem.clone(), table_data)?;

        let timestamp_idx = table_data.schema().timestamp_index();

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
        batch_record_senders.clear();

        let info_and_metas = try_join_all(sst_handlers).await.context(RuntimeJoin)?;
        for (idx, info_and_meta) in info_and_metas.into_iter().enumerate() {
            let (sst_info, sst_meta) = info_and_meta?;
            files_to_level0.push(AddFile {
                level: 0,
                file: FileMeta {
                    id: file_ids[idx],
                    size: sst_info.file_size as u64,
                    row_num: sst_info.row_num as u64,
                    time_range: sst_meta.time_range,
                    max_seq: sst_meta.max_sequence,
                    storage_format: sst_info.storage_format,
                },
            })
        }

        Ok(Some(max_sequence))
    }

    /// Flush rows in normal (non-sampling) memtable to at most one sst file.
    async fn dump_normal_memtable(
        &self,
        table_data: &TableData,
        request_id: RequestId,
        memtable_state: &MemTableState,
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
            time_range: memtable_state.time_range,
            max_sequence,
            schema: table_data.schema(),
        };

        // Alloc file id for next sst file
        let file_id = table_data.alloc_file_id();
        let sst_file_path = table_data.set_sst_file_path(file_id);

        let storage_format_hint = table_data.table_options().storage_format_hint;
        let sst_write_options = SstWriteOptions {
            storage_format_hint,
            num_rows_per_row_group: table_data.table_options().num_rows_per_row_group,
            compression: table_data.table_options().compression,
            max_buffer_size: self.write_sst_max_buffer_size,
        };
        let mut writer = self
            .space_store
            .sst_factory
            .create_writer(
                &sst_write_options,
                &sst_file_path,
                self.space_store.store_picker(),
            )
            .await
            .context(CreateSstWriter {
                storage_format_hint,
            })?;

        let iter = build_mem_table_iter(memtable_state.mem.clone(), table_data)?;

        let record_batch_stream: RecordBatchStream =
            Box::new(stream::iter(iter).map_err(|e| Box::new(e) as _));

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
            time_range: memtable_state.time_range,
            max_seq: memtable_state.last_sequence(),
            storage_format: sst_info.storage_format,
        }))
    }

    /// Schedule table compaction request to background workers and return
    /// immediately.
    pub async fn schedule_table_compaction(&self, compact_req: TableCompactionRequest) {
        self.compaction_scheduler
            .schedule_table_compaction(compact_req)
            .await;
    }
}

impl SpaceStore {
    pub(crate) async fn compact_table(
        &self,
        request_id: RequestId,
        table_data: &TableData,
        task: &CompactionTask,
        scan_options: ScanOptions,
        sst_write_options: &SstWriteOptions,
        runtime: Arc<Runtime>,
    ) -> Result<()> {
        debug!(
            "Begin compact table, table_name:{}, id:{}, task:{:?}",
            table_data.name, table_data.id, task
        );
        let mut edit_meta = VersionEditMeta {
            space_id: table_data.space_id,
            table_id: table_data.id,
            flushed_sequence: 0,
            // Use the number of compaction inputs as the estimated number of files to add.
            files_to_add: Vec::with_capacity(task.compaction_inputs.len()),
            files_to_delete: Vec::new(),
        };

        if task.expired.is_empty() && task.compaction_inputs.is_empty() {
            // Nothing to compact.
            return Ok(());
        }

        for files in &task.expired {
            self.delete_expired_files(table_data, request_id, files, &mut edit_meta);
        }

        info!(
            "try do compaction for table:{}#{}, estimated input files size:{}, input files number:{}",
            table_data.name,
            table_data.id,
            task.estimated_total_input_file_size(),
            task.num_input_files(),
        );

        for input in &task.compaction_inputs {
            self.compact_input_files(
                request_id,
                table_data,
                input,
                scan_options.clone(),
                sst_write_options,
                runtime.clone(),
                &mut edit_meta,
            )
            .await?;
        }

        let update_req = {
            let meta_update = MetaUpdate::VersionEdit(edit_meta.clone());
            MetaUpdateRequest {
                shard_info: table_data.shard_info,
                meta_update,
            }
        };
        self.manifest
            .store_update(update_req)
            .await
            .context(StoreVersionEdit)?;

        // Apply to the table version.
        let edit = edit_meta.into_version_edit();
        table_data.current_version().apply_edit(edit);

        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn compact_input_files(
        &self,
        request_id: RequestId,
        table_data: &TableData,
        input: &CompactionInputFiles,
        scan_options: ScanOptions,
        sst_write_options: &SstWriteOptions,
        runtime: Arc<Runtime>,
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

        info!(
            "Instance try to compact table, table:{}, table_id:{}, request_id:{}, input_files:{:?}",
            table_data.name, table_data.id, request_id, input.files,
        );

        // The schema may be modified during compaction, so we acquire it first and use
        // the acquired schema as the compacted sst meta.
        let schema = table_data.schema();
        let table_options = table_data.table_options();
        let projected_schema = ProjectedSchema::no_projection(schema.clone());
        let sst_read_options = SstReadOptions {
            reverse: false,
            num_rows_per_row_group: table_options.num_rows_per_row_group,
            frequency: ReadFrequency::Once,
            projected_schema: projected_schema.clone(),
            predicate: Arc::new(Predicate::empty()),
            meta_cache: self.meta_cache.clone(),
            scan_options,
            runtime: runtime.clone(),
        };
        let iter_options = IterOptions {
            batch_size: table_options.num_rows_per_row_group,
        };
        let merge_iter = {
            let space_id = table_data.space_id;
            let table_id = table_data.id;
            let sequence = table_data.last_sequence();
            let mut builder = MergeBuilder::new(MergeConfig {
                request_id,
                metrics_collector: None,
                // no need to set deadline for compaction
                deadline: None,
                space_id,
                table_id,
                sequence,
                projected_schema,
                predicate: Arc::new(Predicate::empty()),
                sst_factory: &self.sst_factory,
                sst_read_options: sst_read_options.clone(),
                store_picker: self.store_picker(),
                merge_iter_options: iter_options.clone(),
                need_dedup: table_options.need_dedup(),
                reverse: false,
            });
            // Add all ssts in compaction input to builder.
            builder
                .mut_ssts_of_level(input.level)
                .extend_from_slice(&input.files);
            builder.build().await.context(BuildMergeIterator {
                table: table_data.name.clone(),
            })?
        };

        let record_batch_stream = if table_options.need_dedup() {
            row_iter::record_batch_with_key_iter_to_stream(DedupIterator::new(
                request_id,
                merge_iter,
                iter_options,
            ))
        } else {
            row_iter::record_batch_with_key_iter_to_stream(merge_iter)
        };

        let sst_meta = {
            let meta_reader = SstMetaReader {
                space_id: table_data.space_id,
                table_id: table_data.id,
                factory: self.sst_factory.clone(),
                read_opts: sst_read_options,
                store_picker: self.store_picker.clone(),
            };
            let sst_metas = meta_reader
                .fetch_metas(&input.files)
                .await
                .context(ReadSstMeta)?;
            MetaData::merge(sst_metas.into_iter().map(MetaData::from), schema)
        };

        // Alloc file id for the merged sst.
        let file_id = table_data.alloc_file_id();
        let sst_file_path = table_data.set_sst_file_path(file_id);

        let mut sst_writer = self
            .sst_factory
            .create_writer(sst_write_options, &sst_file_path, self.store_picker())
            .await
            .context(CreateSstWriter {
                storage_format_hint: sst_write_options.storage_format_hint,
            })?;

        let sst_info = sst_writer
            .write(request_id, &sst_meta, record_batch_stream)
            .await
            .box_err()
            .with_context(|| WriteSst {
                path: sst_file_path.to_string(),
            })?;

        let sst_file_size = sst_info.file_size as u64;
        let sst_row_num = sst_info.row_num as u64;
        table_data
            .metrics
            .compaction_observe_output_sst_size(sst_file_size);
        table_data
            .metrics
            .compaction_observe_output_sst_row_num(sst_row_num);

        info!(
            "Instance files compacted, table:{}, table_id:{}, request_id:{}, output_path:{}, input_files:{:?}, sst_meta:{:?}, sst_info:{:?}",
            table_data.name,
            table_data.id,
            request_id,
            sst_file_path.to_string(),
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
            },
        });

        Ok(())
    }

    pub(crate) fn delete_expired_files(
        &self,
        table_data: &TableData,
        request_id: RequestId,
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

fn split_record_batch_with_time_ranges(
    record_batch: RecordBatchWithKey,
    time_ranges: &[TimeRange],
    timestamp_idx: usize,
) -> Result<Vec<RecordBatchWithKey>> {
    let mut builders: Vec<RecordBatchWithKeyBuilder> = (0..time_ranges.len())
        .map(|_| RecordBatchWithKeyBuilder::new(record_batch.schema_with_key().clone()))
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

fn build_mem_table_iter(memtable: MemTableRef, table_data: &TableData) -> Result<ColumnarIterPtr> {
    let scan_ctx = ScanContext::default();
    let scan_req = ScanRequest {
        start_user_key: Bound::Unbounded,
        end_user_key: Bound::Unbounded,
        sequence: common_types::MAX_SEQUENCE_NUMBER,
        projected_schema: ProjectedSchema::no_projection(table_data.schema()),
        need_dedup: table_data.dedup(),
        reverse: false,
        metrics_collector: None,
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
            build_record_batch_with_key_by_rows, build_row, build_row_opt,
            check_record_batch_with_key_with_rows,
        },
        time::TimeRange,
    };

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
        let record_batch_with_key = build_record_batch_with_key_by_rows(rows);
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
}
