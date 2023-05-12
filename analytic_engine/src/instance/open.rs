// Copyright 2022-2023 CeresDB Project Authors. Licensed under Apache-2.0.

//! Open logic of instance

use std::{
    collections::VecDeque,
    sync::{Arc, RwLock},
};

use common_types::schema::IndexInWriterSchema;
use log::{debug, error, info, trace};
use object_store::ObjectStoreRef;
use snafu::ResultExt;
use table_engine::engine::OpenTableRequest;
use wal::{
    log_batch::LogEntry,
    manager::{ReadBoundary, ReadContext, ReadRequest, WalManagerRef},
};

use crate::{
    compaction::scheduler::SchedulerImpl,
    context::OpenContext,
    engine,
    instance::{
        self,
        engine::{ApplyMemTable, FlushTable, OpenManifest, ReadMetaUpdate, ReadWal, Result},
        flush_compaction::TableFlushOptions,
        mem_collector::MemUsageCollector,
        serial_executor::TableOpSerialExecutor,
        write::MemTableWriter,
        Instance, SpaceStore,
    },
    manifest::{details::ManifestImpl, LoadRequest},
    payload::{ReadPayload, WalDecoder},
    row_iter::IterOptions,
    space::{SpaceRef, Spaces},
    sst::{
        factory::{FactoryRef as SstFactoryRef, ObjectStorePickerRef, ScanOptions},
        file::FilePurger,
    },
    table::data::TableDataRef,
    table_meta_set_impl::TableMetaSetImpl,
};

const MAX_RECORD_BATCHES_IN_FLIGHT_WHEN_COMPACTION_READ: usize = 64;

pub(crate) struct ManifestStorages {
    pub wal_manager: WalManagerRef,
    pub oss_storage: ObjectStoreRef,
}

impl Instance {
    /// Open a new instance
    pub(crate) async fn open(
        ctx: OpenContext,
        manifest_storages: ManifestStorages,
        wal_manager: WalManagerRef,
        store_picker: ObjectStorePickerRef,
        sst_factory: SstFactoryRef,
    ) -> Result<Arc<Self>> {
        let spaces: Arc<RwLock<Spaces>> = Arc::new(RwLock::new(Spaces::default()));
        let default_runtime = ctx.runtimes.default_runtime.clone();
        let file_purger = Arc::new(FilePurger::start(
            &default_runtime,
            store_picker.default_store().clone(),
        ));

        let table_meta_set_impl = Arc::new(TableMetaSetImpl {
            spaces: spaces.clone(),
            file_purger: file_purger.clone(),
            preflush_write_buffer_size_ratio: ctx.config.preflush_write_buffer_size_ratio,
        });
        let manifest = ManifestImpl::open(
            ctx.config.manifest.clone(),
            manifest_storages.wal_manager,
            manifest_storages.oss_storage,
            table_meta_set_impl,
        )
        .await
        .context(OpenManifest)?;

        let space_store = Arc::new(SpaceStore {
            spaces,
            manifest: Arc::new(manifest),
            wal_manager: wal_manager.clone(),
            store_picker: store_picker.clone(),
            sst_factory,
            meta_cache: ctx.meta_cache.clone(),
        });

        let scheduler_config = ctx.config.compaction.clone();
        let scan_options_for_compaction = ScanOptions {
            background_read_parallelism: 1,
            max_record_batches_in_flight: MAX_RECORD_BATCHES_IN_FLIGHT_WHEN_COMPACTION_READ,
        };
        let compaction_runtime = ctx.runtimes.compact_runtime.clone();
        let compaction_scheduler = Arc::new(SchedulerImpl::new(
            space_store.clone(),
            compaction_runtime,
            scheduler_config,
            ctx.config.write_sst_max_buffer_size.as_byte() as usize,
            scan_options_for_compaction,
        ));

        let scan_options = ScanOptions {
            background_read_parallelism: ctx.config.sst_background_read_parallelism,
            max_record_batches_in_flight: ctx.config.scan_max_record_batches_in_flight,
        };

        let iter_options = ctx
            .config
            .scan_batch_size
            .map(|batch_size| IterOptions { batch_size });
        let instance = Arc::new(Instance {
            space_store,
            runtimes: ctx.runtimes.clone(),
            table_opts: ctx.config.table_opts.clone(),

            compaction_scheduler,
            file_purger,
            meta_cache: ctx.meta_cache.clone(),
            mem_usage_collector: Arc::new(MemUsageCollector::default()),
            db_write_buffer_size: ctx.config.db_write_buffer_size,
            space_write_buffer_size: ctx.config.space_write_buffer_size,
            replay_batch_size: ctx.config.replay_batch_size,
            write_sst_max_buffer_size: ctx.config.write_sst_max_buffer_size.as_byte() as usize,
            retry_flush: ctx.config.retry_flush,
            max_bytes_per_write_batch: ctx
                .config
                .max_bytes_per_write_batch
                .map(|v| v.as_byte() as usize),
            iter_options,
            scan_options,
        });

        Ok(instance)
    }

    /// Open the table.
    pub async fn do_open_table(
        self: &Arc<Self>,
        space: SpaceRef,
        request: &OpenTableRequest,
    ) -> Result<Option<TableDataRef>> {
        if let Some(table_data) = space.find_table_by_id(request.table_id) {
            return Ok(Some(table_data));
        }

        // Recover table meta from manifest.
        self.recover_table_meta(request).await?;

        // Recover table data from wal.
        let table_data = match space.find_table_by_id(request.table_id) {
            Some(data) => data,
            None => return Ok(None),
        };

        let read_ctx = ReadContext {
            batch_size: self.replay_batch_size,
            ..Default::default()
        };
        self.recover_table_data(table_data.clone(), self.replay_batch_size, &read_ctx)
            .await
            .map_err(|e| {
                error!("Recovery table from wal failed, table_data:{table_data:?}, err:{e}");
                space.insert_open_failed_table(table_data.name.to_string());
                e
            })?;

        Ok(Some(table_data))
    }

    /// Recover meta data from manifest
    ///
    /// Return None if no meta data is found for the table.
    async fn recover_table_meta(self: &Arc<Self>, request: &OpenTableRequest) -> Result<()> {
        info!("Instance recover table:{} meta begin", request.table_id);

        // Load manifest, also create a new snapshot at startup.
        let table_id = request.table_id;
        let space_id = engine::build_space_id(request.schema_id);
        let load_req = LoadRequest {
            space_id,
            table_id,
            shard_id: request.shard_id,
        };
        self.space_store
            .manifest
            .recover(&load_req)
            .await
            .context(ReadMetaUpdate {
                table_id: request.table_id,
            })?;

        info!("Instance recover table:{} meta end", request.table_id);

        Ok(())
    }

    /// Recover table data from wal
    ///
    /// Called by write worker
    pub(crate) async fn recover_table_data(
        self: &Arc<Self>,
        table_data: TableDataRef,
        replay_batch_size: usize,
        read_ctx: &ReadContext,
    ) -> Result<()> {
        debug!(
            "Instance recover table from wal, replay batch size:{}, table id:{}, shard info:{:?}",
            replay_batch_size, table_data.id, table_data.shard_info
        );

        let table_location = table_data.table_location();
        let wal_location =
            instance::create_wal_location(table_location.id, table_location.shard_info);
        let read_req = ReadRequest {
            location: wal_location,
            start: ReadBoundary::Excluded(table_data.current_version().flushed_sequence()),
            end: ReadBoundary::Max,
        };

        // Read all wal of current table.
        let mut log_iter = self
            .space_store
            .wal_manager
            .read_batch(read_ctx, &read_req)
            .await
            .context(ReadWal)?;

        let mut serial_exec = table_data.serial_exec.lock().await;
        let mut log_entry_buf = VecDeque::with_capacity(replay_batch_size);
        loop {
            // fetch entries to log_entry_buf
            let decoder = WalDecoder::default();
            log_entry_buf = log_iter
                .next_log_entries(decoder, log_entry_buf)
                .await
                .context(ReadWal)?;

            // Replay all log entries of current table
            self.replay_table_log_entries(&mut serial_exec, &table_data, &log_entry_buf)
                .await?;

            // No more entries.
            if log_entry_buf.is_empty() {
                break;
            }
        }

        Ok(())
    }

    /// Replay all log entries into memtable and flush if necessary.
    async fn replay_table_log_entries(
        self: &Arc<Self>,
        serial_exec: &mut TableOpSerialExecutor,
        table_data: &TableDataRef,
        log_entries: &VecDeque<LogEntry<ReadPayload>>,
    ) -> Result<()> {
        if log_entries.is_empty() {
            info!(
                "Instance replay an empty table log entries, table:{}, table_id:{:?}",
                table_data.name, table_data.id
            );

            // No data in wal
            return Ok(());
        }

        let last_sequence = log_entries.back().unwrap().sequence;

        info!(
            "Instance replay table log entries begin, table:{}, table_id:{:?}, sequence:{}",
            table_data.name, table_data.id, last_sequence
        );

        for log_entry in log_entries {
            let (sequence, payload) = (log_entry.sequence, &log_entry.payload);

            // Apply to memtable
            match payload {
                ReadPayload::Write { row_group } => {
                    trace!(
                        "Instance replay row_group, table:{}, row_group:{:?}",
                        table_data.name,
                        row_group
                    );

                    let table_schema_version = table_data.schema_version();
                    if table_schema_version != row_group.schema().version() {
                        // Data with old schema should already been flushed, but we avoid panic
                        // here.
                        error!(
                            "Ignore data with mismatch schema version during replaying, \
                            table:{}, \
                            table_id:{:?}, \
                            expect:{}, \
                            actual:{}, \
                            last_sequence:{}, \
                            sequence:{}",
                            table_data.name,
                            table_data.id,
                            table_schema_version,
                            row_group.schema().version(),
                            last_sequence,
                            sequence,
                        );

                        continue;
                    }

                    let index_in_writer =
                        IndexInWriterSchema::for_same_schema(row_group.schema().num_columns());
                    let memtable_writer = MemTableWriter::new(table_data.clone(), serial_exec);
                    memtable_writer
                        .write(sequence, &row_group.into(), index_in_writer)
                        .context(ApplyMemTable {
                            space_id: table_data.space_id,
                            table: &table_data.name,
                            table_id: table_data.id,
                        })?;

                    // Flush the table if necessary.
                    if table_data.should_flush_table(serial_exec) {
                        let opts = TableFlushOptions {
                            res_sender: None,
                            compact_after_flush: None,
                            retry_flush: self.retry_flush,
                        };
                        let flusher = self.make_flusher();
                        let flush_scheduler = serial_exec.flush_scheduler();
                        flusher
                            .schedule_flush(flush_scheduler, table_data, opts)
                            .await
                            .context(FlushTable {
                                space_id: table_data.space_id,
                                table: &table_data.name,
                                table_id: table_data.id,
                            })?;
                    }
                }
                ReadPayload::AlterSchema { .. } | ReadPayload::AlterOptions { .. } => {
                    // Ignore records except Data.
                    //
                    // - DDL (AlterSchema and AlterOptions) should be recovered
                    //   from Manifest on start.
                }
            }
        }

        info!(
            "Instance replay table log entries end, table:{}, table_id:{:?}, last_sequence:{}",
            table_data.name, table_data.id, last_sequence
        );

        table_data.set_last_sequence(last_sequence);

        Ok(())
    }
}
