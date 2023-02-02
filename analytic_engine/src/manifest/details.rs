// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

//! Implementation of Manifest

use std::{
    collections::VecDeque,
    mem,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};

use async_trait::async_trait;
use common_types::table::TableId;
use common_util::{config::ReadableDuration, define_result};
use log::{debug, info};
use object_store::ObjectStoreRef;
use serde_derive::Deserialize;
use snafu::{Backtrace, ResultExt, Snafu};
use tokio::sync::Mutex;
use wal::{
    log_batch::LogEntry,
    manager::{
        BatchLogIteratorAdapter, ReadBoundary, ReadContext, ReadRequest, SequenceNumber,
        WalLocation, WalManagerRef, WriteContext,
    },
};

use crate::{
    manifest::{
        meta_data::{TableManifestData, TableManifestDataBuilder},
        meta_update::{MetaUpdate, MetaUpdateDecoder, MetaUpdatePayload, MetaUpdateRequest},
        Manifest,
    },
    space::SpaceId,
};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display(
        "Failed to get to get log batch encoder, wal_location:{:?}, err:{}",
        wal_location,
        source
    ))]
    GetLogBatchEncoder {
        wal_location: WalLocation,
        source: wal::manager::Error,
    },

    #[snafu(display(
        "Failed to encode payloads, wal_location:{:?}, err:{}",
        wal_location,
        source
    ))]
    EncodePayloads {
        wal_location: WalLocation,
        source: wal::manager::Error,
    },
    #[snafu(display("Failed to write update to wal, err:{}", source))]
    WriteWal { source: wal::manager::Error },

    #[snafu(display("Failed to read wal, err:{}", source))]
    ReadWal { source: wal::manager::Error },

    #[snafu(display("Failed to read log entry, err:{}", source))]
    ReadEntry { source: wal::manager::Error },

    #[snafu(display("Failed to apply table meta update, err:{}", source))]
    ApplyUpdate {
        source: crate::manifest::meta_data::Error,
    },

    #[snafu(display("Failed to clean wal, err:{}", source))]
    CleanWal { source: wal::manager::Error },

    #[snafu(display(
        "Failed to clean snapshot, wal_location:{:?}, err:{}",
        wal_location,
        source
    ))]
    CleanSnapshot {
        wal_location: WalLocation,
        source: wal::manager::Error,
    },

    #[snafu(display("Failed to load sequence of manifest, err:{}", source))]
    LoadSequence { source: wal::manager::Error },

    #[snafu(display("Failed to load sequence of snapshot state, err:{}", source))]
    LoadSnapshotMetaSequence { source: wal::manager::Error },

    #[snafu(display("Failed to clean snapshot state, err:{}", source))]
    CleanSnapshotState { source: wal::manager::Error },

    #[snafu(display(
        "Snapshot flag log is corrupted, end flag's sequence:{}.\nBacktrace:\n{}",
        seq,
        backtrace
    ))]
    CorruptedSnapshotFlag {
        seq: SequenceNumber,
        backtrace: Backtrace,
    },
}

define_result!(Error);

#[async_trait]
trait MetaUpdateLogEntryIterator {
    async fn next_update(&mut self) -> Result<Option<(SequenceNumber, MetaUpdate)>>;
}

/// Implementation of [MetaUpdateReader]
#[derive(Debug)]
pub struct MetaUpdateReaderImpl {
    iter: BatchLogIteratorAdapter,
    has_next: bool,
    buffer: VecDeque<LogEntry<MetaUpdate>>,
}

#[async_trait]
impl MetaUpdateLogEntryIterator for MetaUpdateReaderImpl {
    async fn next_update(&mut self) -> Result<Option<(SequenceNumber, MetaUpdate)>> {
        if !self.has_next {
            return Ok(None);
        }

        if self.buffer.is_empty() {
            let decoder = MetaUpdateDecoder;
            let buffer = mem::take(&mut self.buffer);
            self.buffer = self
                .iter
                .next_log_entries(decoder, buffer)
                .await
                .context(ReadEntry)?;
        }

        match self.buffer.pop_front() {
            Some(entry) => Ok(Some((entry.sequence, entry.payload))),
            None => {
                self.has_next = false;
                Ok(None)
            }
        }
    }
}

/// Options for manifest
#[derive(Debug, Clone, Deserialize)]
pub struct Options {
    /// Steps to do snapshot
    pub snapshot_every_n_updates: usize,

    /// Timeout to read manifest entries
    pub scan_timeout: ReadableDuration,

    /// Batch size to read manifest entries
    pub scan_batch_size: usize,

    /// Timeout to store manifest entries
    pub store_timeout: ReadableDuration,
}

impl Default for Options {
    fn default() -> Self {
        Self {
            snapshot_every_n_updates: 10_000,
            scan_timeout: ReadableDuration::secs(5),
            scan_batch_size: 100,
            store_timeout: ReadableDuration::secs(5),
        }
    }
}

#[derive(Debug, Clone)]
struct SnapshotContext {
    space_id: SpaceId,
    table_id: TableId,
}

/// The implementation based on wal of Manifest which features:
///  - The manifest of table is separate from each other.
///  - The snapshot mechanism is based on logs(check the details on comments on
///    [`Snapshotter`]).
#[derive(Debug)]
pub struct ManifestImpl {
    opts: Options,
    wal_manager: WalManagerRef,
    store: ObjectStoreRef,

    /// Number of updates wrote to wal since last snapshot.
    num_updates_since_snapshot: Arc<AtomicUsize>,

    /// Ensure the snapshot procedure is non-concurrent.
    ///
    /// Use tokio mutex because this guard protects the snapshot procedure which
    /// contains io operations.
    snapshot_write_guard: Arc<Mutex<()>>,
}

impl ManifestImpl {
    pub async fn open(
        opts: Options,
        wal_manager: WalManagerRef,
        store: ObjectStoreRef,
    ) -> Result<Self> {
        let manifest = Self {
            opts,
            wal_manager,
            store,
            num_updates_since_snapshot: Arc::new(AtomicUsize::new(0)),
            snapshot_write_guard: Arc::new(Mutex::new(())),
        };

        Ok(manifest)
    }

    async fn store_update_to_wal(&self, request: MetaUpdateRequest) -> Result<SequenceNumber> {
        info!("Manifest store update, request:{:?}", request);
        let log_store = RegionWal {
            opts: self.opts.clone(),
            location: request.location,
            wal_manager: self.wal_manager.clone(),
        };
        log_store.append(request.meta_update).await
    }

    /// Do snapshot if no other snapshot is triggered.
    ///
    /// Returns the latest snapshot if snapshot is done.
    async fn maybe_do_snapshot(
        &self,
        space_id: SpaceId,
        location: WalLocation,
        force: bool,
    ) -> Result<Option<Snapshot>> {
        if !force {
            let num_updates = self
                .num_updates_since_snapshot
                .fetch_add(1, Ordering::Relaxed);
            if num_updates < self.opts.snapshot_every_n_updates {
                return Ok(None);
            }
        }

        if let Ok(_guard) = self.snapshot_write_guard.try_lock() {
            let snapshot_ctx = SnapshotContext {
                space_id,
                table_id: location.table_id,
            };
            let log_store = RegionWal {
                opts: self.opts.clone(),
                location,
                wal_manager: self.wal_manager.clone(),
            };
            let snapshotter = Snapshotter {
                snapshot_ctx,
                location,
                log_store,
                snapshot_store: ObjectStoreBasedSnapshotStore {},
            };
            let snapshot = snapshotter.snapshot().await?.map(|v| {
                self.decrease_num_updates(v.original_logs_num);
                v
            });
            Ok(snapshot)
        } else {
            debug!("Avoid concurrent snapshot");
            Ok(None)
        }
    }

    // with snapshot guard held
    fn decrease_num_updates(&self, num: usize) {
        if num > self.num_updates_since_snapshot.load(Ordering::Relaxed) {
            self.num_updates_since_snapshot.store(0, Ordering::Relaxed);
        } else {
            self.num_updates_since_snapshot
                .fetch_sub(self.opts.snapshot_every_n_updates, Ordering::Relaxed);
        }
    }
}

#[async_trait]
impl Manifest for ManifestImpl {
    async fn store_update(
        &self,
        request: MetaUpdateRequest,
    ) -> std::result::Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let location = request.location;
        let space_id = request.meta_update.space_id();
        self.store_update_to_wal(request).await?;

        self.maybe_do_snapshot(space_id, location, false).await?;

        Ok(())
    }

    async fn load_data(
        &self,
        space_id: SpaceId,
        location: WalLocation,
        do_snapshot: bool,
    ) -> std::result::Result<Option<TableManifestData>, Box<dyn std::error::Error + Send + Sync>>
    {
        if do_snapshot {
            if let Some(snapshot) = self.maybe_do_snapshot(space_id, location, true).await? {
                return Ok(snapshot.data);
            }
        }

        let snapshot_ctx = SnapshotContext {
            space_id: space_id,
            table_id: location.table_id,
        };
        let log_store = RegionWal {
            opts: self.opts.clone(),
            location,
            wal_manager: self.wal_manager.clone(),
        };
        let snapshotter = Snapshotter {
            snapshot_ctx,
            location,
            log_store,
            snapshot_store: ObjectStoreBasedSnapshotStore {},
        };
        let snapshot = snapshotter.create_latest_snapshot().await?;
        Ok(snapshot.and_then(|v| v.data))
    }
}

#[async_trait]
trait MetaUpdateLogStore: std::fmt::Debug {
    type Iter: MetaUpdateLogEntryIterator;
    async fn scan(&self, start: ReadBoundary, end: ReadBoundary) -> Result<Self::Iter>;

    async fn append(&self, meta_update: MetaUpdate) -> Result<SequenceNumber>;

    async fn delete_up_to(&self, inclusive_end: SequenceNumber) -> Result<()>;
}

#[async_trait]
trait MetaUpdateSnapshotStore: std::fmt::Debug {
    async fn store(&self, ctx: &SnapshotContext, snapshot: &Snapshot) -> Result<()>;

    async fn load(&self, ctx: &SnapshotContext) -> Result<Option<Snapshot>>;
}

#[derive(Debug)]
struct ObjectStoreBasedSnapshotStore {}

#[async_trait]
impl MetaUpdateSnapshotStore for ObjectStoreBasedSnapshotStore {
    async fn store(&self, ctx: &SnapshotContext, snapshot: &Snapshot) -> Result<()> {
        todo!()
    }

    async fn load(&self, ctx: &SnapshotContext) -> Result<Option<Snapshot>> {
        todo!()
    }
}

#[derive(Debug, Clone)]
struct RegionWal {
    opts: Options,
    location: WalLocation,
    wal_manager: WalManagerRef,
}

#[async_trait]
impl MetaUpdateLogStore for RegionWal {
    type Iter = MetaUpdateReaderImpl;

    async fn scan(&self, start: ReadBoundary, end: ReadBoundary) -> Result<Self::Iter> {
        let ctx = ReadContext {
            timeout: self.opts.scan_timeout.0,
            batch_size: self.opts.scan_batch_size,
        };

        let read_req = ReadRequest {
            location: self.location,
            start,
            end,
        };

        let iter = self
            .wal_manager
            .read_batch(&ctx, &read_req)
            .await
            .context(ReadWal)?;

        Ok(MetaUpdateReaderImpl {
            iter,
            has_next: true,
            buffer: VecDeque::with_capacity(ctx.batch_size),
        })
    }

    async fn append(&self, meta_update: MetaUpdate) -> Result<SequenceNumber> {
        let payload = MetaUpdatePayload::from(meta_update);
        let log_batch_encoder =
            self.wal_manager
                .encoder(self.location)
                .context(GetLogBatchEncoder {
                    wal_location: self.location,
                })?;
        let log_batch = log_batch_encoder.encode(&payload).context(EncodePayloads {
            wal_location: self.location,
        })?;

        let write_ctx = WriteContext {
            timeout: self.opts.store_timeout.0,
        };

        self.wal_manager
            .write(&write_ctx, &log_batch)
            .await
            .context(WriteWal)
    }

    async fn delete_up_to(&self, inclusive_end: SequenceNumber) -> Result<()> {
        self.wal_manager
            .mark_delete_entries_up_to(self.location, inclusive_end)
            .await
            .context(CleanWal)
    }
}

#[derive(Debug, Clone)]
struct Snapshotter<LogStore, SnapshotStore> {
    snapshot_ctx: SnapshotContext,
    location: WalLocation,
    log_store: LogStore,
    snapshot_store: SnapshotStore,
}

#[derive(Debug, Clone)]
struct CurrentSnapshot {
    sequence: SequenceNumber,
}

/// The snapshot for the current logs.
#[derive(Debug, Clone)]
struct Snapshot {
    /// The end sequence of the logs that this snapshot covers.
    /// Basically it is the latest sequence number of the logs when creating a
    /// new snapshot.
    end_seq: SequenceNumber,
    /// The number of the original logs(excluding previous snapshot log) that
    /// this snapshot covers.
    original_logs_num: usize,
    /// The data of the snapshot.
    /// None means the table not exists(maybe dropped or not created yet).
    data: Option<TableManifestData>,
}

impl<LogStore, SnapshotStore> Snapshotter<LogStore, SnapshotStore>
where
    LogStore: MetaUpdateLogStore + Send + Sync,
    SnapshotStore: MetaUpdateSnapshotStore + Send + Sync,
{
    /// Do snapshot for the current logs including:
    ///  - saving the snapshot.
    ///  - deleting the expired logs.
    async fn snapshot(&self) -> Result<Option<Snapshot>> {
        let snapshot = if let Some(v) = self.create_latest_snapshot().await? {
            v
        } else {
            info!("No content to do snapshot, location:{:?}", self.location);
            return Ok(None);
        };

        info!(
            "Store snapshot to region, location:{:?}, snapshot_end_seq:{}",
            self.location, snapshot.end_seq,
        );

        if snapshot.original_logs_num == 0 {
            info!("No new logs after previous snapshot is found, no need to update snapshot, location:{:?}", self.location);
            return Ok(Some(snapshot));
        }

        // Update the current snapshot to the new one.
        self.snapshot_store
            .store(&self.snapshot_ctx, &snapshot)
            .await?;
        // Delete the expired logs after saving the snapshot.
        self.log_store.delete_up_to(snapshot.end_seq).await?;

        Ok(Some(snapshot))
    }

    /// Create a latest snapshot of the current logs.
    async fn create_latest_snapshot(&self) -> Result<Option<Snapshot>> {
        // Load the current snapshot first.
        let curr_snapshot = self.snapshot_store.load(&self.snapshot_ctx).await?;
        let log_start_boundary = if let Some(v) = &curr_snapshot {
            ReadBoundary::Excluded(v.end_seq)
        } else {
            ReadBoundary::Included(SequenceNumber::MIN)
        };
        let mut reader = self
            .log_store
            .scan(log_start_boundary, ReadBoundary::Max)
            .await?;

        let mut num_logs = 0usize;
        let mut latest_seq = SequenceNumber::MIN;
        let curr_snapshot_exists = curr_snapshot.is_some();
        let mut manifest_data_builder = if let Some(Some(v)) = curr_snapshot.map(|v| v.data) {
            TableManifestDataBuilder::new(Some(v.table_meta), v.version_meta)
        } else {
            TableManifestDataBuilder::default()
        };
        while let Some((seq, update)) = reader.next_update().await? {
            latest_seq = seq;
            num_logs += 1;
            manifest_data_builder
                .apply_update(update)
                .context(ApplyUpdate)?;
        }

        if curr_snapshot_exists || num_logs > 0 {
            let snapshot = Snapshot {
                end_seq: latest_seq,
                original_logs_num: num_logs,
                data: manifest_data_builder.build(),
            };
            Ok(Some(snapshot))
        } else {
            Ok(None)
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, iter::FromIterator, path::PathBuf, sync::Arc, vec};

    use bytes::Bytes;
    use common_types::{
        column_schema,
        datum::DatumKind,
        schema,
        schema::Schema,
        table::{DEFAULT_CLUSTER_VERSION, DEFAULT_SHARD_ID},
    };
    use common_util::{runtime, runtime::Runtime, tests::init_log_for_test};
    use futures::future::BoxFuture;
    use table_engine::{
        partition::{HashPartitionInfo, PartitionDefinition, PartitionInfo},
        table::{SchemaId, TableId, TableSeqGenerator},
    };
    use wal::rocks_impl::manager::Builder as WalBuilder;

    use super::*;
    use crate::{
        manifest::{
            details::{MetaUpdateLogEntryIterator, MetaUpdateLogStore},
            meta_update::{
                AddTableMeta, AlterOptionsMeta, AlterSchemaMeta, DropTableMeta, MetaUpdate,
                VersionEditMeta,
            },
            Manifest,
        },
        table::data::{TableLocation, TableShardInfo},
        TableOptions,
    };

    fn build_altered_schema(schema: &Schema) -> Schema {
        let mut builder = schema::Builder::new().auto_increment_column_id(true);
        for column_schema in schema.key_columns() {
            builder = builder
                .add_key_column(column_schema.clone())
                .expect("should succeed to add key column");
        }
        for column_schema in schema.normal_columns() {
            builder = builder
                .add_normal_column(column_schema.clone())
                .expect("should succeed to add normal column");
        }
        builder
            .add_normal_column(
                column_schema::Builder::new("field3".to_string(), DatumKind::String)
                    .build()
                    .expect("should succeed build column schema"),
            )
            .unwrap()
            .build()
            .unwrap()
    }

    fn build_runtime(thread_num: usize) -> Arc<Runtime> {
        Arc::new(
            runtime::Builder::default()
                .worker_threads(thread_num)
                .enable_all()
                .build()
                .unwrap(),
        )
    }

    struct TestContext {
        dir: PathBuf,
        runtime: Arc<Runtime>,
        options: Options,
        schema_id: SchemaId,
        table_seq_gen: TableSeqGenerator,
    }

    impl TestContext {
        fn new(prefix: &str, schema_id: SchemaId) -> Self {
            init_log_for_test();
            let dir = tempfile::Builder::new().prefix(prefix).tempdir().unwrap();
            let runtime = build_runtime(2);

            let options = Options {
                snapshot_every_n_updates: 100,
                ..Default::default()
            };
            Self {
                dir: dir.into_path(),
                runtime,
                options,
                schema_id,
                table_seq_gen: TableSeqGenerator::default(),
            }
        }

        fn alloc_table_id(&self) -> TableId {
            TableId::with_seq(
                self.schema_id,
                self.table_seq_gen.alloc_table_seq().unwrap(),
            )
            .unwrap()
        }

        fn table_name_from_id(table_id: TableId) -> String {
            format!("table_{:?}", table_id)
        }

        async fn open_manifest(&self) -> ManifestImpl {
            let manifest_wal =
                WalBuilder::with_default_rocksdb_config(self.dir.clone(), self.runtime.clone())
                    .build()
                    .unwrap();

            ManifestImpl::open(Arc::new(manifest_wal), self.options.clone())
                .await
                .unwrap()
        }

        async fn check_table_manifest_data_with_manifest(
            &self,
            location: WalLocation,
            expected: &Option<TableManifestData>,
            manifest: &ManifestImpl,
        ) {
            let data = manifest.load_data(location, false).await.unwrap();
            assert_eq!(&data, expected);
        }

        async fn check_table_manifest_data(
            &self,
            location: WalLocation,
            expected: &Option<TableManifestData>,
        ) {
            let manifest = self.open_manifest().await;
            self.check_table_manifest_data_with_manifest(location, expected, &manifest)
                .await;
        }

        fn meta_update_add_table(&self, table_id: TableId) -> MetaUpdate {
            let table_name = Self::table_name_from_id(table_id);
            MetaUpdate::AddTable(AddTableMeta {
                space_id: self.schema_id.as_u32(),
                table_id,
                table_name,
                schema: common_types::tests::build_schema(),
                opts: TableOptions::default(),
                partition_info: None,
            })
        }

        fn meta_update_add_table_with_partition_info(
            &self,
            table_id: TableId,
            partition_info: Option<PartitionInfo>,
        ) -> MetaUpdate {
            let table_name = Self::table_name_from_id(table_id);
            MetaUpdate::AddTable(AddTableMeta {
                space_id: self.schema_id.as_u32(),
                table_id,
                table_name,
                schema: common_types::tests::build_schema(),
                opts: TableOptions::default(),
                partition_info,
            })
        }

        fn meta_update_drop_table(&self, table_id: TableId) -> MetaUpdate {
            let table_name = Self::table_name_from_id(table_id);
            MetaUpdate::DropTable(DropTableMeta {
                space_id: self.schema_id.as_u32(),
                table_id,
                table_name,
            })
        }

        fn meta_update_version_edit(
            &self,
            table_id: TableId,
            flushed_seq: Option<SequenceNumber>,
        ) -> MetaUpdate {
            MetaUpdate::VersionEdit(VersionEditMeta {
                space_id: self.schema_id.as_u32(),
                table_id,
                flushed_sequence: flushed_seq.unwrap_or(100),
                files_to_add: Vec::new(),
                files_to_delete: Vec::new(),
            })
        }

        fn meta_update_alter_table_options(&self, table_id: TableId) -> MetaUpdate {
            MetaUpdate::AlterOptions(AlterOptionsMeta {
                space_id: self.schema_id.as_u32(),
                table_id,
                options: TableOptions {
                    enable_ttl: false,
                    ..Default::default()
                },
            })
        }

        fn meta_update_alter_table_schema(&self, table_id: TableId) -> MetaUpdate {
            MetaUpdate::AlterSchema(AlterSchemaMeta {
                space_id: self.schema_id.as_u32(),
                table_id,
                schema: build_altered_schema(&common_types::tests::build_schema()),
                pre_schema_version: 1,
            })
        }

        async fn add_table_with_manifest(
            &self,
            table_id: TableId,
            partition_info: Option<PartitionInfo>,
            manifest_data_builder: &mut TableManifestDataBuilder,
            manifest: &ManifestImpl,
        ) {
            let shard_info = TableShardInfo {
                shard_id: DEFAULT_SHARD_ID,
                cluster_version: DEFAULT_CLUSTER_VERSION,
            };

            let table_location = TableLocation {
                id: table_id.as_u64(),
                shard_info,
            };

            let add_table =
                self.meta_update_add_table_with_partition_info(table_id, partition_info);
            manifest
                .store_update(MetaUpdateRequest::new(table_location, add_table.clone()))
                .await
                .unwrap();
            manifest_data_builder.apply_update(add_table).unwrap();
        }

        async fn drop_table_with_manifest(
            &self,
            table_id: TableId,
            manifest_data_builder: &mut TableManifestDataBuilder,
            manifest: &ManifestImpl,
        ) {
            let shard_info = TableShardInfo {
                shard_id: DEFAULT_SHARD_ID,
                cluster_version: DEFAULT_CLUSTER_VERSION,
            };

            let table_location = TableLocation {
                id: table_id.as_u64(),
                shard_info,
            };

            let drop_table = self.meta_update_drop_table(table_id);
            manifest
                .store_update(MetaUpdateRequest::new(table_location, drop_table.clone()))
                .await
                .unwrap();
            manifest_data_builder.apply_update(drop_table).unwrap();
        }

        async fn version_edit_table_with_manifest(
            &self,
            table_id: TableId,
            flushed_seq: Option<SequenceNumber>,
            manifest_data_builder: &mut TableManifestDataBuilder,
            manifest: &ManifestImpl,
        ) {
            let shard_info = TableShardInfo {
                shard_id: DEFAULT_SHARD_ID,
                cluster_version: DEFAULT_CLUSTER_VERSION,
            };

            let table_location = TableLocation {
                id: table_id.as_u64(),
                shard_info,
            };

            let version_edit = self.meta_update_version_edit(table_id, flushed_seq);
            manifest
                .store_update(MetaUpdateRequest::new(table_location, version_edit.clone()))
                .await
                .unwrap();
            manifest_data_builder.apply_update(version_edit).unwrap();
        }

        async fn add_table(
            &self,
            table_id: TableId,
            manifest_data_builder: &mut TableManifestDataBuilder,
        ) {
            let manifest = self.open_manifest().await;
            self.add_table_with_manifest(table_id, None, manifest_data_builder, &manifest)
                .await;
        }

        async fn drop_table(
            &self,
            table_id: TableId,
            manifest_data_builder: &mut TableManifestDataBuilder,
        ) {
            let manifest = self.open_manifest().await;

            self.drop_table_with_manifest(table_id, manifest_data_builder, &manifest)
                .await;
        }

        async fn version_edit_table(
            &self,
            table_id: TableId,
            manifest_data_builder: &mut TableManifestDataBuilder,
        ) {
            let manifest = self.open_manifest().await;
            self.version_edit_table_with_manifest(table_id, None, manifest_data_builder, &manifest)
                .await;
        }

        async fn alter_table_options(
            &self,
            table_id: TableId,
            manifest_data_builder: &mut TableManifestDataBuilder,
        ) {
            let manifest = self.open_manifest().await;

            let shard_info = TableShardInfo {
                shard_id: DEFAULT_SHARD_ID,
                cluster_version: DEFAULT_CLUSTER_VERSION,
            };

            let table_location = TableLocation {
                id: table_id.as_u64(),
                shard_info,
            };

            let alter_options = self.meta_update_alter_table_options(table_id);
            manifest
                .store_update(MetaUpdateRequest::new(
                    table_location,
                    alter_options.clone(),
                ))
                .await
                .unwrap();
            manifest_data_builder.apply_update(alter_options).unwrap();
        }

        async fn alter_table_schema(
            &self,
            table_id: TableId,
            manifest_data_builder: &mut TableManifestDataBuilder,
        ) {
            let manifest = self.open_manifest().await;

            let shard_info = TableShardInfo {
                shard_id: DEFAULT_SHARD_ID,
                cluster_version: DEFAULT_CLUSTER_VERSION,
            };

            let table_location = TableLocation {
                id: table_id.as_u64(),
                shard_info,
            };

            let alter_schema = self.meta_update_alter_table_schema(table_id);
            manifest
                .store_update(MetaUpdateRequest::new(table_location, alter_schema.clone()))
                .await
                .unwrap();
            manifest_data_builder.apply_update(alter_schema).unwrap();
        }
    }

    fn run_basic_manifest_test<F>(ctx: TestContext, update_table_meta: F)
    where
        F: for<'a> FnOnce(
            &'a TestContext,
            TableId,
            &'a mut TableManifestDataBuilder,
        ) -> BoxFuture<'a, ()>,
    {
        let runtime = ctx.runtime.clone();
        runtime.block_on(async move {
            let table_id = ctx.alloc_table_id();
            let mut manifest_data_builder = TableManifestDataBuilder::default();

            update_table_meta(&ctx, table_id, &mut manifest_data_builder).await;

            let location = WalLocation::new(
                table_id.as_u64(),
                DEFAULT_CLUSTER_VERSION,
                table_id.as_u64(),
            );
            let expected_table_manifest_data = manifest_data_builder.build();
            ctx.check_table_manifest_data(location, &expected_table_manifest_data)
                .await;
        })
    }

    #[test]
    fn test_manifest_add_table() {
        let ctx = TestContext::new("add_table", SchemaId::from_u32(0));
        run_basic_manifest_test(ctx, |ctx, table_id, manifest_data_builder| {
            Box::pin(async move {
                ctx.add_table(table_id, manifest_data_builder).await;
            })
        });
    }

    #[test]
    fn test_manifest_drop_table() {
        let ctx = TestContext::new("drop_table", SchemaId::from_u32(0));
        run_basic_manifest_test(ctx, |ctx, table_id, manifest_data_builder| {
            Box::pin(async move {
                ctx.add_table(table_id, manifest_data_builder).await;
                ctx.drop_table(table_id, manifest_data_builder).await;
            })
        });
    }

    #[test]
    fn test_manifest_version_edit() {
        let ctx = TestContext::new("version_edit", SchemaId::from_u32(0));
        run_basic_manifest_test(ctx, |ctx, table_id, manifest_data_builder| {
            Box::pin(async move {
                ctx.add_table(table_id, manifest_data_builder).await;
                ctx.version_edit_table(table_id, manifest_data_builder)
                    .await;
            })
        });
    }

    #[test]
    fn test_manifest_alter_options() {
        let ctx = TestContext::new("version_edit", SchemaId::from_u32(0));
        run_basic_manifest_test(ctx, |ctx, table_id, manifest_data_builder| {
            Box::pin(async move {
                ctx.add_table(table_id, manifest_data_builder).await;
                ctx.alter_table_options(table_id, manifest_data_builder)
                    .await;
            })
        });
    }

    #[test]
    fn test_manifest_alter_schema() {
        let ctx = TestContext::new("version_edit", SchemaId::from_u32(0));
        run_basic_manifest_test(ctx, |ctx, table_id, manifest_data_builder| {
            Box::pin(async move {
                ctx.add_table(table_id, manifest_data_builder).await;
                ctx.alter_table_schema(table_id, manifest_data_builder)
                    .await;
            })
        });
    }

    #[test]
    fn test_manifest_snapshot_one_table() {
        let ctx = TestContext::new("snapshot_one_table", SchemaId::from_u32(0));
        let runtime = ctx.runtime.clone();

        runtime.block_on(async move {
            let table_id = ctx.alloc_table_id();
            let default_version = 0;
            let partition_info = Some(PartitionInfo::Hash(HashPartitionInfo {
                version: default_version,
                definitions: vec![PartitionDefinition {
                    name: "p0".to_string(),
                    origin_name: Some("region0".to_string()),
                }],
                expr: Bytes::from("test"),
                linear: false,
            }));
            let location = WalLocation::new(
                table_id.as_u64(),
                DEFAULT_CLUSTER_VERSION,
                table_id.as_u64(),
            );
            let mut manifest_data_builder = TableManifestDataBuilder::default();
            let manifest = ctx.open_manifest().await;
            ctx.add_table_with_manifest(
                table_id,
                partition_info,
                &mut manifest_data_builder,
                &manifest,
            )
            .await;

            manifest.maybe_do_snapshot(location).await.unwrap();

            ctx.version_edit_table_with_manifest(
                table_id,
                None,
                &mut manifest_data_builder,
                &manifest,
            )
            .await;
            ctx.check_table_manifest_data_with_manifest(
                location,
                &manifest_data_builder.build(),
                &manifest,
            )
            .await;
        });
    }

    #[test]
    fn test_manifest_snapshot_one_table_massive_logs() {
        let ctx = TestContext::new("snapshot_one_table_massive_logs", SchemaId::from_u32(0));
        let runtime = ctx.runtime.clone();
        runtime.block_on(async move {
            let table_id = ctx.alloc_table_id();
            let location = WalLocation::new(
                table_id.as_u64(),
                DEFAULT_CLUSTER_VERSION,
                table_id.as_u64(),
            );
            let mut manifest_data_builder = TableManifestDataBuilder::default();
            let manifest = ctx.open_manifest().await;
            ctx.add_table_with_manifest(table_id, None, &mut manifest_data_builder, &manifest)
                .await;

            for i in 0..500 {
                ctx.version_edit_table_with_manifest(
                    table_id,
                    Some(i),
                    &mut manifest_data_builder,
                    &manifest,
                )
                .await;
            }
            ctx.check_table_manifest_data_with_manifest(
                location,
                &manifest_data_builder.clone().build(),
                &manifest,
            )
            .await;

            manifest.maybe_do_snapshot(location).await.unwrap();
            for i in 500..550 {
                ctx.version_edit_table_with_manifest(
                    table_id,
                    Some(i),
                    &mut manifest_data_builder,
                    &manifest,
                )
                .await;
            }
            ctx.check_table_manifest_data_with_manifest(
                location,
                &manifest_data_builder.build(),
                &manifest,
            )
            .await;
        });
    }

    #[derive(Debug)]
    struct MemLogStore {
        logs: Mutex<Vec<Option<MetaUpdateLogEntry>>>,
    }

    impl MemLogStore {
        fn from_logs(logs: &[MetaUpdateLogEntry]) -> Self {
            let mut buf = Vec::with_capacity(logs.len());
            buf.extend(logs.iter().map(|v| Some(v.clone())));
            Self {
                logs: Mutex::new(buf),
            }
        }

        async fn to_log_entries(&self) -> Vec<MetaUpdateLogEntry> {
            let logs = self.logs.lock().await;
            logs.iter().filter_map(|v| v.clone()).collect()
        }
    }

    #[async_trait]
    impl MetaUpdateLogStore for MemLogStore {
        type Iter = vec::IntoIter<(SequenceNumber, MetaUpdateLogEntry)>;

        async fn scan(
            &self,
            _ctx: &ScanContext,
            start: ReadBoundary,
            end: ReadBoundary,
        ) -> Result<Self::Iter> {
            let logs = self.logs.lock().await;
            let start = start.as_start_sequence_number().unwrap() as usize;
            let end = {
                let inclusive_end = end.as_end_sequence_number().unwrap() as usize;
                if logs.len() == 0 {
                    0
                } else if inclusive_end < logs.len() {
                    inclusive_end + 1
                } else {
                    logs.len()
                }
            };

            let mut exist_logs = Vec::with_capacity(end - start);
            for (idx, log_entry) in logs[..end].iter().enumerate() {
                if idx < start {
                    continue;
                }
                if let Some(log_entry) = &log_entry {
                    exist_logs.push((idx as u64, log_entry.clone()));
                }
            }

            Ok(exist_logs.into_iter())
        }

        async fn store(
            &self,
            _ctx: &StoreContext,
            log_entries: &[MetaUpdateLogEntry],
        ) -> Result<()> {
            let mut logs = self.logs.lock().await;
            logs.extend(log_entries.iter().map(|v| Some(v.clone())));

            Ok(())
        }

        async fn delete_up_to(&self, inclusive_end: SequenceNumber) -> Result<()> {
            let mut logs = self.logs.lock().await;
            for i in 0..=inclusive_end {
                logs[i as usize] = None;
            }

            Ok(())
        }
    }

    #[async_trait]
    impl<T> MetaUpdateLogEntryIterator for T
    where
        T: Iterator<Item = (SequenceNumber, MetaUpdateLogEntry)> + Send + Sync,
    {
        async fn next_update(&mut self) -> Result<Option<(SequenceNumber, MetaUpdateLogEntry)>> {
            Ok(self.next())
        }
    }

    fn run_snapshot_test(
        ctx: Arc<TestContext>,
        table_id: TableId,
        logs: Vec<(&str, MetaUpdateLogEntry)>,
        expect_log_name_order: &[&str],
        expect_snapshot_log_num: usize,
        expect_meta_updates: &[MetaUpdateLogEntry],
    ) {
        let table_manifest_data = {
            let mapping: HashMap<_, _> =
                HashMap::from_iter(logs.iter().enumerate().map(|(idx, (name, _))| (*name, idx)));
            let mut manifest_builder = TableManifestDataBuilder::default();
            // apply the real order:
            // S0,S1,N2,N3,N4
            for log_name in expect_log_name_order {
                let (_, log_entry) = &logs[*mapping.get(log_name).unwrap()];
                let meta_update = match log_entry {
                    MetaUpdateLogEntry::Normal(meta_update) => meta_update,
                    MetaUpdateLogEntry::Snapshot { meta_update, .. } => meta_update,
                    _ => unreachable!(),
                };
                manifest_builder.apply_update(meta_update.clone()).unwrap();
            }
            manifest_builder.build()
        };

        let location = WalLocation::new(
            table_id.as_u64(),
            DEFAULT_CLUSTER_VERSION,
            table_id.as_u64(),
        );
        let log_store = {
            let log_entries: Vec<_> = logs
                .iter()
                .map(|(_, log_entry)| log_entry.clone())
                .collect();
            MemLogStore::from_logs(&log_entries)
        };

        let options = ctx.options.clone();
        ctx.runtime.block_on(async move {
            let snapshotter = Snapshotter {
                location,
                log_store,
                options,
            };

            let latest_snapshot = snapshotter.create_latest_snapshot().await.unwrap();
            assert_eq!(latest_snapshot.data, table_manifest_data);

            let snapshot_log_num = snapshotter.snapshot().await.unwrap().original_logs_num;
            assert_eq!(expect_snapshot_log_num, snapshot_log_num);

            let latest_snapshot = snapshotter.create_latest_snapshot().await.unwrap();
            assert_eq!(latest_snapshot.data, table_manifest_data);

            let entries = snapshotter.log_store.to_log_entries().await;
            assert_eq!(expect_meta_updates, &entries);
        });
    }

    // Actual logs:
    // N0,N1,SS(1),N2
    // =>
    // logs after snapshot:
    // SS(3),Snapshot(...),SE(3)
    // logs applied after snapshot:
    // N0,N1,N2
    #[test]
    fn test_no_snapshot_logs_merge() {
        let ctx = Arc::new(TestContext::new(
            "snapshot_merge_no_snapshot",
            SchemaId::from_u32(0),
        ));
        let table_id = ctx.alloc_table_id();
        let logs: Vec<(&str, MetaUpdateLogEntry)> = vec![
            (
                "N0",
                MetaUpdateLogEntry::Normal(ctx.meta_update_add_table(table_id)),
            ),
            (
                "N1",
                MetaUpdateLogEntry::Normal(ctx.meta_update_version_edit(table_id, Some(1))),
            ),
            ("SS(1)", MetaUpdateLogEntry::SnapshotStart(1)),
            (
                "N2",
                MetaUpdateLogEntry::Normal(ctx.meta_update_version_edit(table_id, Some(3))),
            ),
        ];

        let snapshot_start_seq = (logs.len() - 1) as u64;
        let expect_log_name_order = &["N0", "N1", "N2"];
        let expect_snapshot_updates = &[
            MetaUpdateLogEntry::SnapshotStart(snapshot_start_seq),
            MetaUpdateLogEntry::Snapshot {
                sequence: snapshot_start_seq,
                meta_update: ctx.meta_update_add_table(table_id),
            },
            MetaUpdateLogEntry::Snapshot {
                sequence: snapshot_start_seq,
                meta_update: ctx.meta_update_version_edit(table_id, Some(3)),
            },
            MetaUpdateLogEntry::SnapshotEnd(3),
        ];

        run_snapshot_test(
            ctx,
            table_id,
            logs,
            expect_log_name_order,
            3,
            expect_snapshot_updates,
        );
    }

    // Actual logs:
    // N0,N1,SS(1),N2,S0(1),S1(1),N3,SE(1),N4
    // =>
    // logs after snapshot:
    // SS(8),Snapshot(...),SE(8)
    // logs applied after snapshot:
    // S0(1),S1(1),N2,N3,N4
    #[test]
    fn test_multiple_snapshot_merge_normal() {
        let ctx = Arc::new(TestContext::new(
            "snapshot_merge_normal",
            SchemaId::from_u32(0),
        ));
        let table_id = ctx.alloc_table_id();
        let logs: Vec<(&str, MetaUpdateLogEntry)> = vec![
            (
                "N0",
                MetaUpdateLogEntry::Normal(ctx.meta_update_add_table(table_id)),
            ),
            (
                "N1",
                MetaUpdateLogEntry::Normal(ctx.meta_update_version_edit(table_id, Some(1))),
            ),
            ("SS(1)", MetaUpdateLogEntry::SnapshotStart(1)),
            (
                "N2",
                MetaUpdateLogEntry::Normal(ctx.meta_update_version_edit(table_id, Some(3))),
            ),
            (
                "S0(1)",
                MetaUpdateLogEntry::Snapshot {
                    sequence: 1,
                    meta_update: ctx.meta_update_add_table(table_id),
                },
            ),
            (
                "S1(1)",
                MetaUpdateLogEntry::Snapshot {
                    sequence: 1,
                    meta_update: ctx.meta_update_version_edit(table_id, Some(1)),
                },
            ),
            (
                "N3",
                MetaUpdateLogEntry::Normal(ctx.meta_update_version_edit(table_id, Some(6))),
            ),
            ("SE(1)", MetaUpdateLogEntry::SnapshotEnd(1)),
            (
                "N4",
                MetaUpdateLogEntry::Normal(ctx.meta_update_version_edit(table_id, Some(7))),
            ),
        ];

        let expect_log_name_order = &["S0(1)", "S1(1)", "N2", "N3", "N4"];
        let snapshot_start_seq = (logs.len() - 1) as u64;
        let expect_snapshot_updates = &[
            MetaUpdateLogEntry::SnapshotStart(snapshot_start_seq),
            MetaUpdateLogEntry::Snapshot {
                sequence: snapshot_start_seq,
                meta_update: ctx.meta_update_add_table(table_id),
            },
            MetaUpdateLogEntry::Snapshot {
                sequence: snapshot_start_seq,
                meta_update: ctx.meta_update_version_edit(table_id, Some(7)),
            },
            MetaUpdateLogEntry::SnapshotEnd(snapshot_start_seq),
        ];
        run_snapshot_test(
            ctx,
            table_id,
            logs,
            expect_log_name_order,
            3,
            expect_snapshot_updates,
        );
    }

    // Actual logs:
    // 0 - N0
    // 1 - SS(0)
    // 2 - N1
    // 3 - S0(0)
    // 4 - N2
    // 5 - SS(4)
    // 6 - N3
    // 7 - S1(4)
    // 8 - S2(4)
    // 9 - SE(4)
    // 10- N4
    // =>
    // logs after snapshot:
    // SS(10),Snapshot(...),SE(10)
    // logs applied after snapshot:
    // S1(4),S2(4),N3,N4
    #[test]
    fn test_multiple_snapshot_merge_interleaved_snapshot() {
        let ctx = Arc::new(TestContext::new(
            "snapshot_merge_interleaved",
            SchemaId::from_u32(0),
        ));
        let table_id = ctx.alloc_table_id();
        let logs: Vec<(&str, MetaUpdateLogEntry)> = vec![
            (
                "N0",
                MetaUpdateLogEntry::Normal(ctx.meta_update_add_table(table_id)),
            ),
            ("SS(0)", MetaUpdateLogEntry::SnapshotStart(0)),
            (
                "N1",
                MetaUpdateLogEntry::Normal(ctx.meta_update_version_edit(table_id, Some(1))),
            ),
            (
                "S0(0)",
                MetaUpdateLogEntry::Snapshot {
                    sequence: 0,
                    meta_update: ctx.meta_update_add_table(table_id),
                },
            ),
            (
                "N2",
                MetaUpdateLogEntry::Normal(ctx.meta_update_version_edit(table_id, Some(2))),
            ),
            ("SS(4)", MetaUpdateLogEntry::SnapshotStart(4)),
            (
                "N3",
                MetaUpdateLogEntry::Normal(ctx.meta_update_version_edit(table_id, Some(3))),
            ),
            (
                "S1(4)",
                MetaUpdateLogEntry::Snapshot {
                    sequence: 4,
                    meta_update: ctx.meta_update_add_table(table_id),
                },
            ),
            (
                "S2(4)",
                MetaUpdateLogEntry::Snapshot {
                    sequence: 4,
                    meta_update: ctx.meta_update_version_edit(table_id, Some(3)),
                },
            ),
            ("SE(4)", MetaUpdateLogEntry::SnapshotEnd(4)),
            (
                "N4",
                MetaUpdateLogEntry::Normal(ctx.meta_update_version_edit(table_id, Some(4))),
            ),
        ];

        let expect_log_name_order = &["S1(4)", "S2(4)", "N3", "N4"];
        let snapshot_start_seq = (logs.len() - 1) as u64;
        let expect_snapshot_updates = &[
            MetaUpdateLogEntry::SnapshotStart(snapshot_start_seq),
            MetaUpdateLogEntry::Snapshot {
                sequence: snapshot_start_seq,
                meta_update: ctx.meta_update_add_table(table_id),
            },
            MetaUpdateLogEntry::Snapshot {
                sequence: snapshot_start_seq,
                meta_update: ctx.meta_update_version_edit(table_id, Some(4)),
            },
            MetaUpdateLogEntry::SnapshotEnd(snapshot_start_seq),
        ];
        run_snapshot_test(
            ctx,
            table_id,
            logs,
            expect_log_name_order,
            2,
            expect_snapshot_updates,
        );
    }

    // Actual logs:
    // 0 - N0
    // 1 - N1
    // 2 - SS(0)
    // 3 - N2
    // 4 - S0(0)
    // 5 - SE(0)
    // =>
    // logs after snapshot:
    // SS(5),Snapshot(...),SE(5)
    // logs applied after snapshot:
    // S0(0),N1,N2
    #[test]
    fn test_multiple_snapshot_merge_sneaked_update() {
        let ctx = Arc::new(TestContext::new(
            "snapshot_merge_sneaked_update",
            SchemaId::from_u32(0),
        ));
        let table_id = ctx.alloc_table_id();
        let logs: Vec<(&str, MetaUpdateLogEntry)> = vec![
            (
                "N0",
                MetaUpdateLogEntry::Normal(ctx.meta_update_add_table(table_id)),
            ),
            (
                "N1",
                MetaUpdateLogEntry::Normal(ctx.meta_update_version_edit(table_id, Some(1))),
            ),
            ("SS(0)", MetaUpdateLogEntry::SnapshotStart(0)),
            (
                "N2",
                MetaUpdateLogEntry::Normal(ctx.meta_update_version_edit(table_id, Some(2))),
            ),
            (
                "S0(0)",
                MetaUpdateLogEntry::Snapshot {
                    sequence: 0,
                    meta_update: ctx.meta_update_add_table(table_id),
                },
            ),
            ("SE(0)", MetaUpdateLogEntry::SnapshotEnd(0)),
        ];

        let expect_log_name_order = &["S0(0)", "N1", "N2"];
        let snapshot_start_seq = (logs.len() - 1) as u64;
        let expect_snapshot_updates = &[
            MetaUpdateLogEntry::SnapshotStart(snapshot_start_seq),
            MetaUpdateLogEntry::Snapshot {
                sequence: snapshot_start_seq,
                meta_update: ctx.meta_update_add_table(table_id),
            },
            MetaUpdateLogEntry::Snapshot {
                sequence: snapshot_start_seq,
                meta_update: ctx.meta_update_version_edit(table_id, Some(2)),
            },
            MetaUpdateLogEntry::SnapshotEnd(snapshot_start_seq),
        ];
        run_snapshot_test(
            ctx,
            table_id,
            logs,
            expect_log_name_order,
            2,
            expect_snapshot_updates,
        );
    }

    // Actual logs:
    // 0 - N0(add table)
    // 1 - N1(drop table)
    // =>
    // logs after snapshot:
    // SS(1),SE(1)
    // logs applied after snapshot:
    // N0,N1
    #[test]
    fn test_multiple_snapshot_drop_table() {
        let ctx = Arc::new(TestContext::new(
            "snapshot_drop_table",
            SchemaId::from_u32(0),
        ));
        let table_id = ctx.alloc_table_id();
        let logs: Vec<(&str, MetaUpdateLogEntry)> = vec![
            (
                "N0",
                MetaUpdateLogEntry::Normal(ctx.meta_update_add_table(table_id)),
            ),
            (
                "N1",
                MetaUpdateLogEntry::Normal(ctx.meta_update_drop_table(table_id)),
            ),
        ];

        let expect_log_name_order = &["N0", "N1"];
        let snapshot_start_seq = (logs.len() - 1) as u64;
        let expect_snapshot_updates = &[
            MetaUpdateLogEntry::SnapshotStart(snapshot_start_seq),
            MetaUpdateLogEntry::SnapshotEnd(snapshot_start_seq),
        ];
        run_snapshot_test(
            ctx,
            table_id,
            logs,
            expect_log_name_order,
            2,
            expect_snapshot_updates,
        );
    }
}
