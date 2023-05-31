// Copyright 2022-2023 CeresDB Project Authors. Licensed under Apache-2.0.

//! Open logic of instance

use std::{
    collections::{HashMap, VecDeque},
    sync::{Arc, RwLock},
};

use common_types::{schema::IndexInWriterSchema, table::ShardId};
use log::{debug, error, info, trace};
use object_store::ObjectStoreRef;
use snafu::ResultExt;
use table_engine::{engine::TableDef, table::TableId};
use wal::{
    log_batch::LogEntry,
    manager::{ReadBoundary, ReadContext, ReadRequest, WalManager, WalManagerRef},
};

use super::{engine::OpenShard, flush_compaction::Flusher};
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
    manifest::{details::ManifestImpl, LoadRequest, Manifest, ManifestRef},
    payload::{ReadPayload, WalDecoder},
    row_iter::IterOptions,
    space::{SpaceAndTable, SpaceRef, Spaces},
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
            max_rows_in_write_queue: ctx.config.max_rows_in_write_queue,
            db_write_buffer_size: ctx.config.db_write_buffer_size,
            space_write_buffer_size: ctx.config.space_write_buffer_size,
            replay_batch_size: ctx.config.replay_batch_size,
            write_sst_max_buffer_size: ctx.config.write_sst_max_buffer_size.as_byte() as usize,
            max_retry_flush_limit: ctx.config.max_retry_flush_limit,
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
    pub async fn do_open_tables_of_shard(
        self: &Arc<Self>,
        context: TablesOfShardContext,
    ) -> Result<OpenTablesOfShardResult> {
        let mut shard_opener = ShardOpener::init(
            context,
            self.space_store.manifest.clone(),
            self.space_store.wal_manager.clone(),
            self.replay_batch_size,
            self.make_flusher(),
            self.max_retry_flush_limit,
        )?;

        shard_opener.open().await
    }
}

#[derive(Debug, Clone)]
pub struct TablesOfShardContext {
    /// Shard id
    pub shard_id: ShardId,
    /// Table infos
    pub table_ctxs: Vec<TableContext>,
}

#[derive(Clone, Debug)]
pub struct TableContext {
    pub table_def: TableDef,
    pub space: SpaceRef,
}

#[derive(Debug)]
enum TableOpenState {
    RecoverTableMeta(RecoverTableMetaContext),
    RecoverTableData(RecoverTableDataContext),
    Failed(crate::instance::engine::Error),
    Success(Option<SpaceAndTable>),
}

#[derive(Debug)]
struct RecoverTableMetaContext {
    table_def: TableDef,
    space: SpaceRef,
}

#[derive(Debug)]
struct RecoverTableDataContext {
    table_data: TableDataRef,
    space: SpaceRef,
}

pub type OpenTablesOfShardResult = HashMap<TableId, Result<Option<SpaceAndTable>>>;

/// Opener for tables of the same shard
struct ShardOpener {
    shard_id: ShardId,
    manifest: ManifestRef,
    wal_manager: WalManagerRef,
    states: HashMap<TableId, TableOpenState>,
    wal_replay_batch_size: usize,
    flusher: Flusher,
    max_retry_flush_limit: usize,
}

impl ShardOpener {
    fn init(
        shard_context: TablesOfShardContext,
        manifest: ManifestRef,
        wal_manager: WalManagerRef,
        wal_replay_batch_size: usize,
        flusher: Flusher,
        max_retry_flush_limit: usize,
    ) -> Result<Self> {
        let mut states = HashMap::with_capacity(shard_context.table_ctxs.len());
        for table_ctx in shard_context.table_ctxs {
            let space = &table_ctx.space;
            let table_id = table_ctx.table_def.id;
            let state = if let Some(table_data) = space.find_table_by_id(table_id) {
                TableOpenState::Success(Some(SpaceAndTable::new(space.clone(), table_data)))
            } else {
                TableOpenState::RecoverTableMeta(RecoverTableMetaContext {
                    table_def: table_ctx.table_def,
                    space: table_ctx.space,
                })
            };
            states.insert(table_id, state);
        }

        Ok(Self {
            shard_id: shard_context.shard_id,
            manifest,
            wal_manager,
            states,
            wal_replay_batch_size,
            flusher,
            max_retry_flush_limit,
        })
    }

    async fn open(&mut self) -> Result<OpenTablesOfShardResult> {
        // Recover tables' metadata.
        self.recover_table_metas().await?;

        // Recover table' data.
        self.recover_table_datas().await?;

        // Retrieve the table results and return.
        let states = std::mem::take(&mut self.states);
        let mut table_results = HashMap::with_capacity(states.len());
        for (table_id, state) in states {
            match state {
                TableOpenState::Failed(e) => {
                    table_results.insert(table_id, Err(e));
                }
                TableOpenState::Success(data) => {
                    table_results.insert(table_id, Ok(data));
                }
                TableOpenState::RecoverTableMeta(_) | TableOpenState::RecoverTableData(_) => {
                    return OpenShard {
                        msg: format!(
                            "unexpected table state, state:{state:?}, table_id:{table_id}",
                        ),
                    }
                    .fail()
                }
            }
        }

        Ok(table_results)
    }

    async fn recover_table_metas(&mut self) -> Result<()> {
        for (table_id, state) in self.states.iter_mut() {
            let result = if let TableOpenState::RecoverTableMeta(ctx) = state {
                match Self::recover_single_table_meta(
                    self.manifest.as_ref(),
                    self.shard_id,
                    &ctx.table_def,
                )
                .await
                {
                    Ok(()) => {
                        let table_data = ctx.space.find_table_by_id(*table_id);
                        Ok(table_data.map(|data| (data, ctx.space.clone())))
                    }
                    Err(e) => Err(e),
                }
            } else {
                return OpenShard {
                    msg: format!("unexpected table state:{state:?}"),
                }
                .fail();
            };

            match result {
                Ok(Some((table_data, space))) => {
                    *state = TableOpenState::RecoverTableData(RecoverTableDataContext {
                        table_data,
                        space,
                    })
                }
                Ok(None) => *state = TableOpenState::Success(None),
                Err(e) => *state = TableOpenState::Failed(e),
            }
        }

        Ok(())
    }

    async fn recover_table_datas(&mut self) -> Result<()> {
        for (_table_id, state) in self.states.iter_mut() {
            match state {
                TableOpenState::RecoverTableData(ctx) => {
                    let table_data = ctx.table_data.clone();
                    let read_ctx = ReadContext {
                        batch_size: self.wal_replay_batch_size,
                        ..Default::default()
                    };

                    let result = match Self::recover_single_table_data(
                        &self.flusher,
                        self.max_retry_flush_limit,
                        self.wal_manager.as_ref(),
                        table_data.clone(),
                        self.wal_replay_batch_size,
                        &read_ctx,
                    )
                    .await
                    {
                        Ok(()) => Ok((table_data, ctx.space.clone())),
                        Err(e) => Err(e),
                    };

                    match result {
                        Ok((table_data, space)) => {
                            *state = TableOpenState::Success(Some(SpaceAndTable::new(
                                space, table_data,
                            )));
                        }
                        Err(e) => *state = TableOpenState::Failed(e),
                    }
                }
                TableOpenState::Failed(_) => {}
                TableOpenState::RecoverTableMeta(_) | TableOpenState::Success(_) => {
                    return OpenShard {
                        msg: format!("unexpected table state:{state:?}"),
                    }
                    .fail()
                }
            }
        }

        Ok(())
    }

    /// Recover meta data from manifest
    ///
    /// Return None if no meta data is found for the table.
    async fn recover_single_table_meta(
        manifest: &dyn Manifest,
        shard_id: ShardId,
        table_def: &TableDef,
    ) -> Result<()> {
        info!("Instance recover table:{} meta begin", table_def.id);

        // Load manifest, also create a new snapshot at startup.
        let table_id = table_def.id;
        let space_id = engine::build_space_id(table_def.schema_id);
        let load_req = LoadRequest {
            space_id,
            table_id,
            shard_id,
        };
        manifest.recover(&load_req).await.context(ReadMetaUpdate {
            table_id: table_def.id,
        })?;

        info!("Instance recover table:{} meta end", table_def.id);

        Ok(())
    }

    /// Recover table data from wal
    ///
    /// Called by write worker
    pub(crate) async fn recover_single_table_data(
        flusher: &Flusher,
        max_retry_flush_limit: usize,
        wal_manager: &dyn WalManager,
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
        let mut log_iter = wal_manager
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
            Self::replay_table_log_entries(
                flusher,
                max_retry_flush_limit,
                &mut serial_exec,
                &table_data,
                &log_entry_buf,
            )
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
        flusher: &Flusher,
        max_retry_flush_limit: usize,
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

        debug!(
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
                            max_retry_flush_limit,
                        };
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

        debug!(
            "Instance replay table log entries end, table:{}, table_id:{:?}, last_sequence:{}",
            table_data.name, table_data.id, last_sequence
        );

        table_data.set_last_sequence(last_sequence);

        Ok(())
    }
}
