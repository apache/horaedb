// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

//! Open logic of instance

use std::sync::{Arc, RwLock};

use common_types::schema::IndexInWriterSchema;
use common_util::define_result;
use log::{debug, error, info, trace};
use object_store::ObjectStore;
use snafu::{ResultExt, Snafu};
use tokio::sync::{oneshot, Mutex};
use wal::{
    log_batch::LogEntry,
    manager::{LogIterator, ReadBoundary, ReadContext, ReadRequest, WalManager},
};

use crate::{
    compaction::scheduler::SchedulerImpl,
    context::OpenContext,
    instance::{
        mem_collector::MemUsageCollector,
        write_worker,
        write_worker::{RecoverTableCommand, WorkerLocal, WriteGroup},
        Instance, MetaState, SpaceStore, Spaces,
    },
    meta::{meta_data::ManifestData, Manifest},
    payload::{ReadPayload, WalDecoder},
    space::{Space, SpaceId},
    sst::{factory::Factory, file::FilePurger},
    table::data::{TableData, TableDataRef},
};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Failed to read meta update, err:{}", source))]
    ReadMetaUpdate {
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display(
        "Failed to recover table data, space_id:{}, table:{}, err:{}",
        space_id,
        table,
        source
    ))]
    RecoverTableData {
        space_id: SpaceId,
        table: String,
        source: crate::table::data::Error,
    },

    #[snafu(display("Failed to read wal, err:{}", source))]
    ReadWal { source: wal::manager::Error },

    #[snafu(display("Failed to apply log entry to memtable, err:{}", source))]
    ApplyMemTable {
        source: crate::instance::write::Error,
    },

    #[snafu(display("Failed to recover table, source:{}", source,))]
    RecoverTable { source: write_worker::Error },
}

define_result!(Error);

impl<
        Wal: WalManager + Send + Sync + 'static,
        Meta: Manifest + Send + Sync + 'static,
        Store: ObjectStore,
        Fa: Factory + Send + Sync + 'static,
    > Instance<Wal, Meta, Store, Fa>
{
    /// Open a new instance
    pub async fn open(
        ctx: OpenContext,
        manifest: Meta,
        wal_manager: Wal,
        store: Store,
        sst_factory: Fa,
    ) -> Result<Arc<Self>> {
        let store = Arc::new(store);
        let space_store = Arc::new(SpaceStore {
            spaces: RwLock::new(Spaces::default()),
            manifest,
            wal_manager,
            store: store.clone(),
            meta_state: Mutex::new(MetaState::default()),
            sst_factory,
            meta_cache: ctx.meta_cache.clone(),
            data_cache: ctx.data_cache.clone(),
        });

        let scheduler_config = ctx.config.compaction_config.clone();
        let bg_runtime = ctx.runtimes.bg_runtime.clone();
        let compaction_scheduler = Arc::new(SchedulerImpl::new(
            space_store.clone(),
            bg_runtime.clone(),
            scheduler_config,
        ));

        let file_purger = FilePurger::start(&*bg_runtime, store);

        let instance = Arc::new(Instance {
            space_store,
            runtimes: ctx.runtimes.clone(),
            table_opts: ctx.config.table_opts.clone(),
            write_group_worker_num: ctx.config.write_group_worker_num,
            write_group_command_channel_cap: ctx.config.write_group_command_channel_cap,
            compaction_scheduler,
            file_purger,
            meta_cache: ctx.meta_cache.clone(),
            data_cache: ctx.data_cache.clone(),
            mem_usage_collector: Arc::new(MemUsageCollector::default()),
            db_write_buffer_size: ctx.config.db_write_buffer_size,
            space_write_buffer_size: ctx.config.space_write_buffer_size,
        });

        instance.recover(ctx).await?;

        Ok(instance)
    }

    /// Recover the instance
    ///
    /// Should only called by open()
    async fn recover(self: &Arc<Self>, ctx: OpenContext) -> Result<()> {
        // Recover meta data, such as all spaces and tables
        self.recover_from_meta(&ctx).await?;

        // Recover from wal
        self.recover_from_wal(&ctx).await?;

        Ok(())
    }

    /// Recover meta data from manifest
    async fn recover_from_meta(self: &Arc<Self>, ctx: &OpenContext) -> Result<()> {
        info!("Instance recover from meta begin");

        // Load manifest, also create a new snapshot at startup.
        let manifest_data = self
            .space_store
            .manifest
            .load_data(true)
            .await
            .map_err(|e| Box::new(e) as _)
            .context(ReadMetaUpdate)?;

        self.apply_manifest_data(manifest_data, ctx).await?;

        info!("Instance recover from meta end");

        Ok(())
    }

    /// Apply manifest data to instance
    async fn apply_manifest_data(
        self: &Arc<Self>,
        manifest_data: ManifestData,
        ctx: &OpenContext,
    ) -> Result<()> {
        // Apply all spaces.
        for (space_id, space_meta_data) in manifest_data.spaces {
            // Create write group for space.
            let space_meta = space_meta_data.space_meta;
            let write_group_opts = self.write_group_options(space_id);
            let write_group = WriteGroup::new(write_group_opts, self.clone());

            // Add this space to instance.
            let space = Arc::new(Space::new(
                space_id,
                space_meta.space_name.clone(),
                ctx.config.space_write_buffer_size,
                write_group,
                self.mem_usage_collector.clone(),
            ));
            {
                let mut spaces = self.space_store.spaces.write().unwrap();
                spaces.insert(space_meta.space_name, space.clone());
            }

            // Add all tables to the space.
            for (table_id, table_meta_data) in space_meta_data.tables {
                let table_meta = table_meta_data.table_meta;
                let table_name = table_meta.table_name.clone();
                // Choose write worker for this table
                let write_handle = space.write_group.choose_worker(table_id);

                debug!("Instance apply add table, meta :{:?}", table_meta);

                let table_data = Arc::new(
                    TableData::recover_from_add(
                        table_meta,
                        write_handle,
                        &self.file_purger,
                        space.mem_usage_collector.clone(),
                    )
                    .context(RecoverTableData {
                        space_id,
                        table: &table_name,
                    })?,
                );
                // Apply version meta to the table.
                let version_meta = table_meta_data.version_meta;
                let max_file_id = version_meta.max_file_id_to_add();
                table_data.current_version().apply_meta(version_meta);
                // In recovery case, we need to maintain last file id of the table manually.
                if table_data.last_file_id() < max_file_id {
                    table_data.set_last_file_id(max_file_id);
                }
                // Add table to space.
                space.insert_table(table_data);
            }
        }

        // Update meta state.
        let mut meta_state = self.space_store.meta_state.lock().await;
        meta_state.last_space_id = manifest_data.last_space_id;

        Ok(())
    }

    /// Recover all table data from wal
    async fn recover_from_wal(&self, ctx: &OpenContext) -> Result<()> {
        // replay_batch_size == 0 causes infinite loop.
        assert!(ctx.config.replay_batch_size > 0);

        info!("Instance recover from wal begin, ctx:{:?}", ctx);

        // For each table, recover data of that table
        let tables = {
            let mut tables = Vec::new();
            self.space_store.list_all_tables(&mut tables);
            tables
        };

        let replay_batch_size = ctx.config.max_replay_tables_per_batch;
        let mut replaying_rxs = Vec::with_capacity(replay_batch_size);
        let mut replaying_tables = Vec::with_capacity(replay_batch_size);

        for table_data in tables {
            // Create a oneshot channel to send/recieve recover result
            let (tx, rx) = oneshot::channel();
            let cmd = RecoverTableCommand {
                table_data: table_data.clone(),
                tx,
                replay_batch_size: ctx.config.replay_batch_size,
            };

            // Send recover request to write worker, actual works done in
            // Self::recover_table_from_wal()
            write_worker::send_command_to_write_worker(cmd.into_command(), &table_data).await;

            replaying_rxs.push(rx);
            replaying_tables.push(table_data.clone());

            if replaying_rxs.len() >= replay_batch_size {
                // Wait batch done
                write_worker::join_all(&replaying_tables, replaying_rxs)
                    .await
                    .context(RecoverTable)?;

                replaying_rxs = Vec::with_capacity(replay_batch_size);
                replaying_tables.clear();
            }
        }

        // Don't forget to wait the last batch done.
        if !replaying_rxs.is_empty() {
            write_worker::join_all(&replaying_tables, replaying_rxs)
                .await
                .context(RecoverTable)?;
        }

        info!("Instance recover from wal end");

        Ok(())
    }

    /// Recover table data from wal
    ///
    /// Called by write worker
    pub(crate) async fn recover_table_from_wal(
        &self,
        worker_local: &WorkerLocal,
        table: TableDataRef,
        replay_batch_size: usize,
        read_ctx: &ReadContext,
        log_entry_buf: &mut Vec<LogEntry<ReadPayload>>,
    ) -> Result<()> {
        let decoder = WalDecoder::default();

        let read_req = ReadRequest {
            region_id: table.wal_region_id(),
            start: ReadBoundary::Min,
            end: ReadBoundary::Max,
        };

        // Read all wal of current table
        let mut log_iter = self
            .space_store
            .wal_manager
            .read(read_ctx, &read_req)
            .context(ReadWal)?;

        loop {
            // fetch entries to log_entry_buf
            let no_more_data = {
                log_entry_buf.clear();

                for _ in 0..replay_batch_size {
                    if let Some(log_entry) = log_iter.next_log_entry(&decoder).context(ReadWal)? {
                        log_entry_buf.push(log_entry);
                    } else {
                        break;
                    }
                }

                log_entry_buf.len() < replay_batch_size
            };

            // Replay all log entries of current table
            self.replay_table_log_entries(worker_local, &*table, log_entry_buf)
                .await?;

            // No more entries.
            if no_more_data {
                break;
            }
        }

        Ok(())
    }

    /// Replay all log entries into memtable
    async fn replay_table_log_entries(
        &self,
        worker_local: &WorkerLocal,
        table_data: &TableData,
        log_entries: &mut [LogEntry<ReadPayload>],
    ) -> Result<()> {
        if log_entries.is_empty() {
            // No data in wal
            return Ok(());
        }

        let last_sequence = log_entries.last().unwrap().sequence;

        info!(
            "Instance replay table log entries begin, table:{}, table_id:{:?}, sequence:{}",
            table_data.name, table_data.id, last_sequence
        );

        // TODO(yingwen): Maybe we need to trigger flush if memtable is full during
        // recovery Replay entries
        for log_entry in log_entries {
            let (sequence, payload) = (log_entry.sequence, &mut log_entry.payload);

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
                    Self::write_to_memtable(
                        worker_local,
                        table_data,
                        sequence,
                        row_group,
                        index_in_writer,
                    )
                    .context(ApplyMemTable)?;
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
