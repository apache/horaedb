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
use common_util::{config::ReadableDuration, define_result, runtime::Runtime, time};
use futures::{
    channel::{mpsc, mpsc::channel},
    future::try_join_all,
    stream, SinkExt, TryStreamExt,
};
use log::{debug, error, info};
use snafu::{Backtrace, OptionExt, ResultExt, Snafu};
use table_engine::{predicate::Predicate, table::Result as TableResult};
use tokio::sync::oneshot;
use wal::manager::WalLocation;

use crate::{
    compaction::{
        CompactionInputFiles, CompactionTask, ExpiredFiles, TableCompactionRequest, WaitError,
    },
    instance::{
        write_worker::{self, CompactTableCommand, FlushTableCommand, WorkerLocal},
        Instance, SpaceStore,
    },
    memtable::{ColumnarIterPtr, MemTableRef, ScanContext, ScanRequest},
    meta::meta_update::{AlterOptionsMeta, MetaUpdate, MetaUpdateRequest, VersionEditMeta},
    row_iter::{
        self,
        dedup::DedupIterator,
        merge::{MergeBuilder, MergeConfig},
        IterOptions,
    },
    space::SpaceAndTable,
    sst::{
        builder::RecordBatchStream,
        factory::{SstBuilderOptions, SstReaderOptions, SstType},
        file::{self, FileMeta, SstMetaData},
    },
    table::{
        data::{TableData, TableDataRef},
        version::{FlushableMemTables, MemTableState, SamplingMemTable},
        version_edit::{AddFile, DeleteFile, VersionEdit},
    },
    table_options::StorageFormatOptions,
};

const DEFAULT_CHANNEL_SIZE: usize = 5;

#[derive(Debug, Snafu)]
#[snafu(visibility = "pub")]
pub enum Error {
    #[snafu(display("Failed to store version edit, err:{}", source))]
    StoreVersionEdit {
        source: Box<dyn std::error::Error + Send + Sync>,
    },

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
    InvalidMemIter {
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display(
        "Sst type is not found, sst_type:{:?}.\nBacktrace:\n{}",
        sst_type,
        backtrace
    ))]
    InvalidSstType {
        sst_type: SstType,
        backtrace: Backtrace,
    },

    #[snafu(display("Failed to build sst, file_path:{}, source:{}", path, source))]
    FailBuildSst {
        path: String,
        source: Box<dyn std::error::Error + Send + Sync>,
    },

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
    SplitRecordBatch {
        source: Box<dyn std::error::Error + Send + Sync>,
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
    /// Flush policy
    pub policy: TableFlushPolicy,
}

impl Default for TableFlushOptions {
    fn default() -> Self {
        Self {
            res_sender: None,
            compact_after_flush: true,
            block_on_write_thread: false,
            policy: TableFlushPolicy::Dump,
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

/// Policy of how to perform flush operation.
#[derive(Default, Debug, Clone, Copy)]
pub enum TableFlushPolicy {
    /// Unknown policy, this is the default value and operation will report
    /// error for it. Others except `RoleTable` should set policy to this
    /// variant.
    Unknown,
    /// Dump memtable to sst file.
    // todo: the default value should be [Unknown].
    #[default]
    Dump,
    // TODO: use this policy and remove "allow(dead_code)"
    /// Drop memtables.
    #[allow(dead_code)]
    Purge,
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
            let meta_update = MetaUpdate::AlterOptions(AlterOptionsMeta {
                space_id: table_data.space_id,
                table_id: table_data.id,
                options: new_table_opts.clone(),
            });
            self.space_store
                .manifest
                .store_update(MetaUpdateRequest::new(
                    table_data.wal_location(),
                    meta_update,
                ))
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
        let flush_job = async move { instance.flush_memtables(&flush_req, opts.policy).await };

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
    async fn flush_memtables(
        &self,
        flush_req: &TableFlushRequest,
        policy: TableFlushPolicy,
    ) -> Result<()> {
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
            "Instance try to flush memtables, table:{}, table_id:{}, request_id:{}, mems_to_flush:{:?}, policy:{:?}",
            table_data.name, table_data.id, request_id, mems_to_flush, policy,
        );

        // Start flush duration timer.
        let local_metrics = table_data.metrics.local_flush_metrics();
        local_metrics.observe_memtables_num(mems_to_flush.len());
        let _timer = local_metrics.flush_duration_histogram.start_timer();

        match policy {
            TableFlushPolicy::Unknown => {
                return UnknownPolicy {}.fail();
            }
            TableFlushPolicy::Dump => {
                self.dump_memtables(table_data, request_id, &mems_to_flush)
                    .await?
            }
            TableFlushPolicy::Purge => {
                self.purge_memtables(table_data, request_id, &mems_to_flush)
                    .await?
            }
        }

        table_data.set_last_flush_time(time::current_time_millis());

        info!(
            "Instance flush memtables done, table:{}, table_id:{}, request_id:{}",
            table_data.name, table_data.id, request_id
        );

        Ok(())
    }

    /// Flush action for [TableFlushPolicy::Purge].
    ///
    /// Purge is simply removing all selected memtables.
    async fn purge_memtables(
        &self,
        table_data: &TableData,
        request_id: RequestId,
        mems_to_flush: &FlushableMemTables,
    ) -> Result<()> {
        // calculate largest sequence number purged
        let mut last_sequence_purged = SequenceNumber::MIN;
        if let Some(sampling_mem) = &mems_to_flush.sampling_mem {
            last_sequence_purged = last_sequence_purged.max(sampling_mem.last_sequence());
        }
        for mem in &mems_to_flush.memtables {
            last_sequence_purged = last_sequence_purged.max(mem.last_sequence());
        }

        // remove these memtables
        let mems_to_remove = mems_to_flush.ids();
        let edit = VersionEdit {
            flushed_sequence: last_sequence_purged,
            mems_to_remove,
            files_to_add: vec![],
            files_to_delete: vec![],
        };
        table_data.current_version().apply_edit(edit);

        info!(
            "Instance purged memtables, table:{}, table_id:{}, request_id:{}, mems_to_flush:{:?}, last_sequence_purged:{}",
            table_data.name,
            table_data.id,
            request_id,
            mems_to_flush,
            last_sequence_purged
        );

        Ok(())
    }

    /// Flush action for [TableFlushPolicy::Dump].
    ///
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
                    local_metrics.observe_sst_size(add_file.file.meta.size);
                }
            }
        }
        for mem in &mems_to_flush.memtables {
            let file = self
                .dump_normal_memtable(table_data, request_id, mem)
                .await?;
            if let Some(file) = file {
                let sst_size = file.meta.size;
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
        let edit_meta = VersionEditMeta {
            space_id: table_data.space_id,
            table_id: table_data.id,
            flushed_sequence,
            files_to_add: files_to_level0.clone(),
            files_to_delete: vec![],
        };
        let meta_update = MetaUpdate::VersionEdit(edit_meta);
        self.space_store
            .manifest
            .store_update(MetaUpdateRequest::new(
                table_data.wal_location(),
                meta_update,
            ))
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
        self.space_store
            .wal_manager
            .mark_delete_entries_up_to(table_data.wal_location(), flushed_sequence)
            .await
            .context(PurgeWal {
                wal_location: table_data.wal_location(),
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

        let sst_builder_options = SstBuilderOptions {
            sst_type: table_data.sst_type,
            num_rows_per_row_group: table_data.table_options().num_rows_per_row_group,
            compression: table_data.table_options().compression,
        };

        for time_range in &time_ranges {
            let (batch_record_sender, batch_record_receiver) =
                channel::<Result<RecordBatchWithKey>>(DEFAULT_CHANNEL_SIZE);
            let file_id = table_data.alloc_file_id();
            let sst_file_path = table_data.set_sst_file_path(file_id);

            // TODO: min_key max_key set in sst_builder build
            let mut sst_meta = SstMetaData {
                min_key: min_key.clone(),
                max_key: max_key.clone(),
                time_range: *time_range,
                max_sequence,
                schema: table_data.schema(),
                size: 0,
                row_num: 0,
                storage_format_opts: StorageFormatOptions::new(
                    table_data.table_options().storage_format,
                ),
                bloom_filter: Default::default(),
            };

            let store = self.space_store.clone();
            let sst_builder_options_clone = sst_builder_options.clone();
            let sst_type = table_data.sst_type;

            // spawn build sst
            let handler = self.runtimes.bg_runtime.spawn(async move {
                let mut builder = store
                    .sst_factory
                    .new_sst_builder(
                        &sst_builder_options_clone,
                        &sst_file_path,
                        store.store_ref(),
                    )
                    .context(InvalidSstType { sst_type })?;

                let sst_info = builder
                    .build(
                        request_id,
                        &sst_meta,
                        Box::new(batch_record_receiver.map_err(|e| Box::new(e) as _)),
                    )
                    .await
                    .map_err(|e| {
                        error!("Failed to build sst file, meta:{:?}, err:{}", sst_meta, e);
                        Box::new(e) as _
                    })
                    .with_context(|| FailBuildSst {
                        path: sst_file_path.to_string(),
                    })?;

                // update sst metadata by built info.
                sst_meta.row_num = sst_info.row_num as u64;
                sst_meta.size = sst_info.file_size as u64;
                Ok(sst_meta)
            });

            batch_record_senders.push(batch_record_sender);
            sst_handlers.push(handler);
            file_ids.push(file_id);
        }

        let iter = build_mem_table_iter(sampling_mem.mem.clone(), table_data)?;

        let timestamp_idx = table_data.schema().timestamp_index();

        for data in iter {
            for (idx, record_batch) in split_record_batch_with_time_ranges(
                data.map_err(|e| Box::new(e) as _).context(InvalidMemIter)?,
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

        let ret = try_join_all(sst_handlers).await;
        for (idx, sst_meta) in ret.context(RuntimeJoin)?.into_iter().enumerate() {
            files_to_level0.push(AddFile {
                level: 0,
                file: FileMeta {
                    id: file_ids[idx],
                    meta: sst_meta?,
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
        let mut sst_meta = SstMetaData {
            min_key,
            max_key,
            time_range: memtable_state.time_range,
            max_sequence,
            schema: table_data.schema(),
            size: 0,
            row_num: 0,
            storage_format_opts: StorageFormatOptions::new(table_data.storage_format()),
            bloom_filter: Default::default(),
        };

        // Alloc file id for next sst file
        let file_id = table_data.alloc_file_id();
        let sst_file_path = table_data.set_sst_file_path(file_id);

        let sst_builder_options = SstBuilderOptions {
            sst_type: table_data.sst_type,
            num_rows_per_row_group: table_data.table_options().num_rows_per_row_group,
            compression: table_data.table_options().compression,
        };
        let mut builder = self
            .space_store
            .sst_factory
            .new_sst_builder(
                &sst_builder_options,
                &sst_file_path,
                self.space_store.store_ref(),
            )
            .context(InvalidSstType {
                sst_type: table_data.sst_type,
            })?;

        let iter = build_mem_table_iter(memtable_state.mem.clone(), table_data)?;

        let record_batch_stream: RecordBatchStream =
            Box::new(stream::iter(iter).map_err(|e| Box::new(e) as _));

        let sst_info = builder
            .build(request_id, &sst_meta, record_batch_stream)
            .await
            .map_err(|e| Box::new(e) as _)
            .with_context(|| FailBuildSst {
                path: sst_file_path.to_string(),
            })?;

        // update sst metadata by built info.
        sst_meta.row_num = sst_info.row_num as u64;
        sst_meta.size = sst_info.file_size as u64;

        Ok(Some(FileMeta {
            id: file_id,
            meta: sst_meta,
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
        runtime: Arc<Runtime>,
        table_data: &TableData,
        request_id: RequestId,
        task: &CompactionTask,
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
                runtime.clone(),
                table_data,
                request_id,
                input,
                &mut edit_meta,
            )
            .await?;
        }

        let meta_update = MetaUpdate::VersionEdit(edit_meta.clone());
        self.manifest
            .store_update(MetaUpdateRequest::new(
                table_data.wal_location(),
                meta_update,
            ))
            .await
            .context(StoreVersionEdit)?;

        // Apply to the table version.
        let edit = edit_meta.into_version_edit();
        table_data.current_version().apply_edit(edit);

        Ok(())
    }

    pub(crate) async fn compact_input_files(
        &self,
        runtime: Arc<Runtime>,
        table_data: &TableData,
        request_id: RequestId,
        input: &CompactionInputFiles,
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
        let _timer = table_data
            .metrics
            .compaction_duration_histogram
            .start_timer();
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

        let iter_options = IterOptions::default();
        let merge_iter = {
            let space_id = table_data.space_id;
            let table_id = table_data.id;
            let sequence = table_data.last_sequence();
            let projected_schema = ProjectedSchema::no_projection(schema.clone());
            let sst_reader_options = SstReaderOptions {
                sst_type: table_data.sst_type,
                read_batch_row_num: table_options.num_rows_per_row_group,
                reverse: false,
                projected_schema: projected_schema.clone(),
                predicate: Arc::new(Predicate::empty()),
                meta_cache: self.meta_cache.clone(),
                runtime: runtime.clone(),
                background_read_parallelism: 1,
                num_rows_per_row_group: table_options.num_rows_per_row_group,
            };
            let mut builder = MergeBuilder::new(MergeConfig {
                request_id,
                space_id,
                table_id,
                sequence,
                projected_schema,
                predicate: Arc::new(Predicate::empty()),
                sst_factory: &self.sst_factory,
                sst_reader_options,
                store: self.store_ref(),
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
            row_iter::record_batch_with_key_iter_to_stream(
                DedupIterator::new(request_id, merge_iter, iter_options),
                &runtime,
            )
        } else {
            row_iter::record_batch_with_key_iter_to_stream(merge_iter, &runtime)
        };

        let mut sst_meta = file::merge_sst_meta(&input.files, schema);

        // Alloc file id for the merged sst.
        let file_id = table_data.alloc_file_id();
        let sst_file_path = table_data.set_sst_file_path(file_id);

        let sst_builder_options = SstBuilderOptions {
            sst_type: table_data.sst_type,
            num_rows_per_row_group: table_options.num_rows_per_row_group,
            compression: table_options.compression,
        };
        let mut sst_builder = self
            .sst_factory
            .new_sst_builder(&sst_builder_options, &sst_file_path, self.store_ref())
            .context(InvalidSstType {
                sst_type: table_data.sst_type,
            })?;

        let sst_info = sst_builder
            .build(request_id, &sst_meta, record_batch_stream)
            .await
            .map_err(|e| Box::new(e) as _)
            .with_context(|| FailBuildSst {
                path: sst_file_path.to_string(),
            })?;

        // update sst metadata by built info.
        sst_meta.row_num = sst_info.row_num as u64;
        sst_meta.size = sst_info.file_size as u64;

        table_data
            .metrics
            .compaction_observe_output_sst_size(sst_meta.size);
        table_data
            .metrics
            .compaction_observe_output_sst_row_num(sst_meta.row_num);

        info!(
            "Instance files compacted, table:{}, table_id:{}, request_id:{}, output_path:{}, input_files:{:?}, sst_meta:{:?}",
            table_data.name,
            table_data.id,
            request_id,
            sst_file_path.to_string(),
            input.files,
            sst_meta
        );

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
                meta: sst_meta,
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
        .into_iter()
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
                .map_err(|e| Box::new(e) as _)
                .context(SplitRecordBatch)?;
        } else {
            panic!(
                "Record timestamp is not in time_ranges, timestamp:{:?}, time_ranges:{:?}",
                timestamp, time_ranges
            );
        }
    }
    let mut ret = Vec::with_capacity(builders.len());
    for mut builder in builders {
        ret.push(
            builder
                .build()
                .map_err(|e| Box::new(e) as _)
                .context(SplitRecordBatch)?,
        );
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
    };
    memtable
        .scan(scan_ctx, scan_req)
        .map_err(|e| Box::new(e) as _)
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
        let rows0 = vec![build_row(b"binary key", 20, 10.0, "string value")];
        let rows1 = vec![build_row(b"binary key1", 120, 11.0, "string value 1")];
        let rows2 = vec![
            build_row_opt(b"binary key2", 220, None, Some("string value 2")),
            build_row_opt(b"binary key3", 250, Some(13.0), None),
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
