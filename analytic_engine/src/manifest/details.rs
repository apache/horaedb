// Copyright 2022-2023 CeresDB Project Authors. Licensed under Apache-2.0.

//! Implementation of Manifest

use std::{
    collections::VecDeque,
    mem,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
};

use async_trait::async_trait;
use ceresdbproto::manifest as manifest_pb;
use common_util::{
    config::ReadableDuration,
    define_result,
    error::{BoxError, GenericResult},
};
use log::{debug, info, warn};
use object_store::{ObjectStoreRef, Path};
use parquet::data_type::AsBytes;
use prost::Message;
use serde::{Deserialize, Serialize};
use snafu::{Backtrace, ResultExt, Snafu};
use table_engine::table::TableId;
use tokio::sync::Mutex;
use wal::{
    kv_encoder::LogBatchEncoder,
    log_batch::LogEntry,
    manager::{
        BatchLogIteratorAdapter, ReadBoundary, ReadContext, ReadRequest, SequenceNumber,
        WalLocation, WalManagerRef, WriteContext,
    },
};

use crate::{
    manifest::{
        meta_data::{TableManifestData, TableManifestDataBuilder},
        meta_update::{
            AddTableMeta, MetaUpdate, MetaUpdateDecoder, MetaUpdatePayload, MetaUpdateRequest,
            VersionEditMeta,
        },
        LoadRequest, Manifest, SnapshotRequest,
    },
    space::SpaceId,
    table::version::TableVersionMeta,
};

#[derive(Debug, Snafu)]
pub enum Error {
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

    #[snafu(display("Failed to parse snapshot, err:{}", source))]
    ParseSnapshot {
        source: crate::manifest::meta_update::Error,
    },

    #[snafu(display(
        "Failed to store snapshot, err:{}.\nBacktrace:\n{:?}",
        source,
        backtrace
    ))]
    StoreSnapshot {
        source: object_store::ObjectStoreError,
        backtrace: Backtrace,
    },

    #[snafu(display(
        "Failed to fetch snapshot, err:{}.\nBacktrace:\n{:?}",
        source,
        backtrace
    ))]
    FetchSnapshot {
        source: object_store::ObjectStoreError,
        backtrace: Backtrace,
    },

    #[snafu(display(
        "Failed to decode snapshot, err:{}.\nBacktrace:\n{:?}",
        source,
        backtrace
    ))]
    DecodeSnapshot {
        source: prost::DecodeError,
        backtrace: Backtrace,
    },
}

define_result!(Error);

#[async_trait]
trait MetaUpdateLogEntryIterator {
    async fn next_update(&mut self) -> Result<Option<(SequenceNumber, MetaUpdate)>>;
}

/// Implementation of [`MetaUpdateLogEntryIterator`].
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
#[derive(Debug, Clone, Deserialize, Serialize)]
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

/// The implementation based on wal and object store of [`Manifest`].
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

    async fn store_update_to_wal(
        &self,
        meta_update: MetaUpdate,
        location: WalLocation,
    ) -> Result<SequenceNumber> {
        let log_store = WalBasedLogStore {
            opts: self.opts.clone(),
            location,
            wal_manager: self.wal_manager.clone(),
        };
        log_store.append(meta_update).await
    }

    /// Do snapshot if no other snapshot is triggered.
    ///
    /// Returns the latest snapshot if snapshot is done.
    async fn maybe_do_snapshot(
        &self,
        space_id: SpaceId,
        table_id: TableId,
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
            let log_store = WalBasedLogStore {
                opts: self.opts.clone(),
                location,
                wal_manager: self.wal_manager.clone(),
            };
            let snapshot_store =
                ObjectStoreBasedSnapshotStore::new(space_id, table_id, self.store.clone());
            let snapshotter = Snapshotter {
                location,
                log_store,
                snapshot_store,
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
    async fn store_update(&self, request: MetaUpdateRequest) -> GenericResult<()> {
        info!("Manifest store update, request:{:?}", request);

        let table_id = request.meta_update.table_id();
        let shard_id = request.shard_info.shard_id;
        let location = WalLocation::new(shard_id as u64, table_id.as_u64());
        let space_id = request.meta_update.space_id();
        let table_id = request.meta_update.table_id();
        self.store_update_to_wal(request.meta_update, location)
            .await?;

        self.maybe_do_snapshot(space_id, table_id, location, false)
            .await?;

        Ok(())
    }

    async fn load_data(&self, load_req: &LoadRequest) -> GenericResult<Option<TableManifestData>> {
        info!("Manifest load data, request:{:?}", load_req);

        let location = WalLocation::new(load_req.shard_id as u64, load_req.table_id.as_u64());

        let log_store = WalBasedLogStore {
            opts: self.opts.clone(),
            location,
            wal_manager: self.wal_manager.clone(),
        };
        let snapshot_store = ObjectStoreBasedSnapshotStore::new(
            load_req.space_id,
            load_req.table_id,
            self.store.clone(),
        );
        let snapshotter = Snapshotter {
            location,
            log_store,
            snapshot_store,
        };
        let snapshot = snapshotter.create_latest_snapshot().await?;

        Ok(snapshot.and_then(|v| v.data))
    }

    async fn do_snapshot(&self, request: SnapshotRequest) -> GenericResult<()> {
        info!("Manifest do snapshot, request:{:?}", request);

        let table_id = request.table_id;
        let location = WalLocation::new(request.shard_id as u64, table_id.as_u64());
        let space_id = request.space_id;
        let table_id = request.table_id;

        self.maybe_do_snapshot(space_id, table_id, location, true)
            .await
            .box_err()?;

        Ok(())
    }
}

#[async_trait]
trait MetaUpdateLogStore: std::fmt::Debug {
    type Iter: MetaUpdateLogEntryIterator;
    async fn scan(&self, start: ReadBoundary) -> Result<Self::Iter>;

    async fn append(&self, meta_update: MetaUpdate) -> Result<SequenceNumber>;

    async fn delete_up_to(&self, inclusive_end: SequenceNumber) -> Result<()>;
}

#[async_trait]
trait MetaUpdateSnapshotStore: std::fmt::Debug {
    async fn store(&self, snapshot: &Snapshot) -> Result<()>;

    async fn load(&self) -> Result<Option<Snapshot>>;
}

#[derive(Debug)]
struct ObjectStoreBasedSnapshotStore {
    store: ObjectStoreRef,
    snapshot_path: Path,
}

impl ObjectStoreBasedSnapshotStore {
    const CURRENT_SNAPSHOT_NAME: &str = "current";
    const SNAPSHOT_PATH_PREFIX: &str = "manifest/snapshot";

    pub fn new(space_id: SpaceId, table_id: TableId, store: ObjectStoreRef) -> Self {
        let snapshot_path = Self::snapshot_path(space_id, table_id);
        Self {
            store,
            snapshot_path,
        }
    }

    fn snapshot_path(space_id: SpaceId, table_id: TableId) -> Path {
        format!(
            "{}/{}/{}/{}",
            Self::SNAPSHOT_PATH_PREFIX,
            space_id,
            table_id,
            Self::CURRENT_SNAPSHOT_NAME,
        )
        .into()
    }
}

#[async_trait]
impl MetaUpdateSnapshotStore for ObjectStoreBasedSnapshotStore {
    /// Store the latest snapshot to the underlying store by overwriting the old
    /// snapshot.
    async fn store(&self, snapshot: &Snapshot) -> Result<()> {
        let snapshot_pb = manifest_pb::Snapshot::from(snapshot.clone());
        let payload = snapshot_pb.encode_to_vec();
        // The atomic write is ensured by the [`ObjectStore`] implementation.
        self.store
            .put(&self.snapshot_path, payload.into())
            .await
            .context(StoreSnapshot)?;

        Ok(())
    }

    /// Load the `current_snapshot` file from the underlying store, and with the
    /// mapping info in it load the latest snapshot file then.
    async fn load(&self) -> Result<Option<Snapshot>> {
        let get_res = self.store.get(&self.snapshot_path).await;
        if let Err(object_store::ObjectStoreError::NotFound { path, source }) = &get_res {
            warn!(
                "Current snapshot file doesn't exist, path:{}, err:{}",
                path, source
            );
            return Ok(None);
        };

        // TODO: currently, this is just a workaround to handle the case where the error
        // is not thrown as [object_store::ObjectStoreError::NotFound].
        if let Err(err) = &get_res {
            let err_msg = err.to_string().to_lowercase();
            if err_msg.contains("404") || err_msg.contains("not found") {
                warn!("Current snapshot file doesn't exist, err:{}", err);
                return Ok(None);
            }
        }

        let payload = get_res
            .context(FetchSnapshot)?
            .bytes()
            .await
            .context(FetchSnapshot)?;
        let snapshot = manifest_pb::Snapshot::decode(payload.as_bytes()).context(DecodeSnapshot)?;
        Ok(Some(Snapshot::try_from(snapshot)?))
    }
}

#[derive(Debug, Clone)]
struct WalBasedLogStore {
    opts: Options,
    location: WalLocation,
    wal_manager: WalManagerRef,
}

#[async_trait]
impl MetaUpdateLogStore for WalBasedLogStore {
    type Iter = MetaUpdateReaderImpl;

    async fn scan(&self, start: ReadBoundary) -> Result<Self::Iter> {
        let ctx = ReadContext {
            timeout: self.opts.scan_timeout.0,
            batch_size: self.opts.scan_batch_size,
        };

        let read_req = ReadRequest {
            location: self.location,
            start,
            end: ReadBoundary::Max,
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
        let log_batch_encoder = LogBatchEncoder::create(self.location);
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
    location: WalLocation,
    log_store: LogStore,
    snapshot_store: SnapshotStore,
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

impl TryFrom<manifest_pb::Snapshot> for Snapshot {
    type Error = Error;

    fn try_from(src: manifest_pb::Snapshot) -> Result<Self> {
        let meta = src
            .meta
            .map(AddTableMeta::try_from)
            .transpose()
            .context(ParseSnapshot)?;

        let version_edit = src
            .version_edit
            .map(VersionEditMeta::try_from)
            .transpose()
            .context(ParseSnapshot)?;

        let version_meta = version_edit.map(|v| {
            let mut version_meta = TableVersionMeta::default();
            version_meta.apply_edit(v.into_version_edit());
            version_meta
        });

        let table_manifest_data = meta.map(|v| TableManifestData {
            table_meta: v,
            version_meta,
        });
        Ok(Self {
            end_seq: src.end_seq,
            original_logs_num: 0,
            data: table_manifest_data,
        })
    }
}

impl From<Snapshot> for manifest_pb::Snapshot {
    fn from(src: Snapshot) -> Self {
        if let Some((meta, version_edit)) = src.data.map(|v| {
            let space_id = v.table_meta.space_id;
            let table_id = v.table_meta.table_id;
            let table_meta = manifest_pb::AddTableMeta::from(v.table_meta);
            let version_edit = v.version_meta.map(|version_meta| VersionEditMeta {
                space_id,
                table_id,
                flushed_sequence: version_meta.flushed_sequence,
                files_to_add: version_meta.ordered_files(),
                files_to_delete: vec![],
            });
            (
                table_meta,
                version_edit.map(manifest_pb::VersionEditMeta::from),
            )
        }) {
            Self {
                end_seq: src.end_seq,
                meta: Some(meta),
                version_edit,
            }
        } else {
            Self {
                end_seq: src.end_seq,
                meta: None,
                version_edit: None,
            }
        }
    }
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
        self.snapshot_store.store(&snapshot).await?;
        // Delete the expired logs after saving the snapshot.
        // TODO: Actually this operation can be performed background, and the failure of
        // it can be ignored.
        self.log_store.delete_up_to(snapshot.end_seq).await?;

        Ok(Some(snapshot))
    }

    /// Create a latest snapshot of the current logs.
    async fn create_latest_snapshot(&self) -> Result<Option<Snapshot>> {
        // Load the current snapshot first.
        match self.snapshot_store.load().await? {
            Some(v) => Ok(Some(self.create_latest_snapshot_with_prev(v).await?)),
            None => self.create_latest_snapshot_without_prev().await,
        }
    }

    async fn create_latest_snapshot_with_prev(&self, prev_snapshot: Snapshot) -> Result<Snapshot> {
        let log_start_boundary = ReadBoundary::Excluded(prev_snapshot.end_seq);
        let mut reader = self.log_store.scan(log_start_boundary).await?;

        let mut num_logs = 0usize;
        let mut latest_seq = prev_snapshot.end_seq;
        let mut manifest_data_builder = if let Some(v) = prev_snapshot.data {
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
        Ok(Snapshot {
            end_seq: latest_seq,
            original_logs_num: num_logs,
            data: manifest_data_builder.build(),
        })
    }

    async fn create_latest_snapshot_without_prev(&self) -> Result<Option<Snapshot>> {
        let mut reader = self.log_store.scan(ReadBoundary::Min).await?;

        let mut num_logs = 0usize;
        let mut latest_seq = SequenceNumber::MIN;
        let mut manifest_data_builder = TableManifestDataBuilder::default();
        while let Some((seq, update)) = reader.next_update().await? {
            latest_seq = seq;
            num_logs += 1;
            manifest_data_builder
                .apply_update(update)
                .context(ApplyUpdate)?;
        }

        if num_logs > 0 {
            Ok(Some(Snapshot {
                end_seq: latest_seq,
                original_logs_num: num_logs,
                data: manifest_data_builder.build(),
            }))
        } else {
            Ok(None)
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{path::PathBuf, sync::Arc, vec};

    use common_types::{
        column_schema, datum::DatumKind, schema, schema::Schema, table::DEFAULT_SHARD_ID,
    };
    use common_util::{runtime, runtime::Runtime, tests::init_log_for_test};
    use futures::future::BoxFuture;
    use object_store::LocalFileSystem;
    use table_engine::table::{SchemaId, TableId, TableSeqGenerator};
    use wal::rocks_impl::manager::Builder as WalBuilder;

    use super::*;
    use crate::{
        manifest::{
            details::{MetaUpdateLogEntryIterator, MetaUpdateLogStore},
            meta_update::{
                AddTableMeta, AlterOptionsMeta, AlterSchemaMeta, DropTableMeta, MetaUpdate,
                VersionEditMeta,
            },
            LoadRequest, Manifest,
        },
        table::data::TableShardInfo,
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
                column_schema::Builder::new("field5".to_string(), DatumKind::String)
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
            format!("table_{table_id:?}")
        }

        async fn open_manifest(&self) -> ManifestImpl {
            let manifest_wal = WalBuilder::new(self.dir.clone(), self.runtime.clone())
                .build()
                .unwrap();

            let object_store = LocalFileSystem::new_with_prefix(&self.dir).unwrap();
            ManifestImpl::open(
                self.options.clone(),
                Arc::new(manifest_wal),
                Arc::new(object_store),
            )
            .await
            .unwrap()
        }

        async fn check_table_manifest_data_with_manifest(
            &self,
            load_req: &LoadRequest,
            expected: &Option<TableManifestData>,
            manifest: &ManifestImpl,
        ) {
            let data = manifest.load_data(load_req).await.unwrap();
            assert_eq!(&data, expected);
        }

        async fn check_table_manifest_data(
            &self,
            load_req: &LoadRequest,
            expected: &Option<TableManifestData>,
        ) {
            let manifest = self.open_manifest().await;
            self.check_table_manifest_data_with_manifest(load_req, expected, &manifest)
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
            manifest_data_builder: &mut TableManifestDataBuilder,
            manifest: &ManifestImpl,
        ) {
            let shard_info = TableShardInfo {
                shard_id: DEFAULT_SHARD_ID,
            };

            let add_table = self.meta_update_add_table(table_id);
            let update_req = {
                MetaUpdateRequest {
                    shard_info,
                    meta_update: add_table.clone(),
                }
            };

            manifest.store_update(update_req).await.unwrap();
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
            };

            let drop_table = self.meta_update_drop_table(table_id);
            let update_req = {
                MetaUpdateRequest {
                    shard_info,
                    meta_update: drop_table.clone(),
                }
            };
            manifest.store_update(update_req).await.unwrap();
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
            };

            let version_edit = self.meta_update_version_edit(table_id, flushed_seq);
            let update_req = {
                MetaUpdateRequest {
                    shard_info,
                    meta_update: version_edit.clone(),
                }
            };
            manifest.store_update(update_req).await.unwrap();
            manifest_data_builder.apply_update(version_edit).unwrap();
        }

        async fn add_table(
            &self,
            table_id: TableId,
            manifest_data_builder: &mut TableManifestDataBuilder,
        ) {
            let manifest = self.open_manifest().await;
            self.add_table_with_manifest(table_id, manifest_data_builder, &manifest)
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
            };

            let alter_options = self.meta_update_alter_table_options(table_id);
            let update_req = {
                MetaUpdateRequest {
                    shard_info,
                    meta_update: alter_options.clone(),
                }
            };
            manifest.store_update(update_req).await.unwrap();
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
            };

            let alter_schema = self.meta_update_alter_table_schema(table_id);
            let update_req = {
                MetaUpdateRequest {
                    shard_info,
                    meta_update: alter_schema.clone(),
                }
            };

            manifest.store_update(update_req).await.unwrap();
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

            let load_req = LoadRequest {
                table_id,
                shard_id: DEFAULT_SHARD_ID,
                space_id: ctx.schema_id.as_u32(),
            };
            let expected_table_manifest_data = manifest_data_builder.build();
            ctx.check_table_manifest_data(&load_req, &expected_table_manifest_data)
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
            let location = WalLocation::new(DEFAULT_SHARD_ID as u64, table_id.as_u64());
            let mut manifest_data_builder = TableManifestDataBuilder::default();
            let manifest = ctx.open_manifest().await;
            ctx.add_table_with_manifest(table_id, &mut manifest_data_builder, &manifest)
                .await;

            manifest
                .maybe_do_snapshot(ctx.schema_id.as_u32(), table_id, location, true)
                .await
                .unwrap();

            ctx.version_edit_table_with_manifest(
                table_id,
                None,
                &mut manifest_data_builder,
                &manifest,
            )
            .await;
            let load_req = LoadRequest {
                space_id: ctx.schema_id.as_u32(),
                table_id,
                shard_id: DEFAULT_SHARD_ID,
            };
            ctx.check_table_manifest_data_with_manifest(
                &load_req,
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
            let load_req = LoadRequest {
                space_id: ctx.schema_id.as_u32(),
                table_id,
                shard_id: table_id.as_u64() as u32,
            };
            let mut manifest_data_builder = TableManifestDataBuilder::default();
            let manifest = ctx.open_manifest().await;
            ctx.add_table_with_manifest(table_id, &mut manifest_data_builder, &manifest)
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
                &load_req,
                &manifest_data_builder.clone().build(),
                &manifest,
            )
            .await;

            let location = WalLocation::new(DEFAULT_SHARD_ID as u64, table_id.as_u64());
            manifest
                .maybe_do_snapshot(ctx.schema_id.as_u32(), table_id, location, true)
                .await
                .unwrap();
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
                &load_req,
                &manifest_data_builder.build(),
                &manifest,
            )
            .await;
        });
    }

    #[derive(Debug)]
    struct MemLogStore {
        logs: std::sync::Mutex<Vec<Option<MetaUpdate>>>,
    }

    impl MemLogStore {
        fn from_updates(updates: &[MetaUpdate]) -> Self {
            let mut buf = Vec::with_capacity(updates.len());
            buf.extend(updates.iter().map(|v| Some(v.clone())));
            Self {
                logs: std::sync::Mutex::new(buf),
            }
        }

        async fn to_meta_updates(&self) -> Vec<MetaUpdate> {
            let logs = self.logs.lock().unwrap();
            logs.iter().filter_map(|v| v.clone()).collect()
        }

        fn next_seq(&self) -> u64 {
            let logs = self.logs.lock().unwrap();
            logs.len() as u64
        }
    }

    #[async_trait]
    impl MetaUpdateLogStore for MemLogStore {
        type Iter = vec::IntoIter<(SequenceNumber, MetaUpdate)>;

        async fn scan(&self, start: ReadBoundary) -> Result<Self::Iter> {
            let logs = self.logs.lock().unwrap();
            let start = start.as_start_sequence_number().unwrap() as usize;

            let mut exist_logs = Vec::new();
            let logs_with_idx = logs.iter().enumerate();
            for (idx, update) in logs_with_idx {
                if idx < start || update.is_none() {
                    continue;
                }
                exist_logs.push((idx as u64, update.clone().unwrap()));
            }

            Ok(exist_logs.into_iter())
        }

        async fn append(&self, meta_update: MetaUpdate) -> Result<SequenceNumber> {
            let mut logs = self.logs.lock().unwrap();
            let seq = logs.len() as u64;
            logs.push(Some(meta_update));

            Ok(seq)
        }

        async fn delete_up_to(&self, inclusive_end: SequenceNumber) -> Result<()> {
            let mut logs = self.logs.lock().unwrap();
            for i in 0..=inclusive_end {
                logs[i as usize] = None;
            }

            Ok(())
        }
    }

    #[async_trait]
    impl<T> MetaUpdateLogEntryIterator for T
    where
        T: Iterator<Item = (SequenceNumber, MetaUpdate)> + Send + Sync,
    {
        async fn next_update(&mut self) -> Result<Option<(SequenceNumber, MetaUpdate)>> {
            Ok(self.next())
        }
    }

    #[derive(Debug)]
    struct MemSnapshotStore {
        curr_snapshot: Mutex<Option<Snapshot>>,
    }

    impl MemSnapshotStore {
        fn new() -> Self {
            Self {
                curr_snapshot: Mutex::new(None),
            }
        }
    }

    #[async_trait]
    impl MetaUpdateSnapshotStore for MemSnapshotStore {
        async fn store(&self, snapshot: &Snapshot) -> Result<()> {
            let mut curr_snapshot = self.curr_snapshot.lock().await;
            *curr_snapshot = Some(snapshot.clone());
            Ok(())
        }

        async fn load(&self) -> Result<Option<Snapshot>> {
            let curr_snapshot = self.curr_snapshot.lock().await;
            Ok(curr_snapshot.clone())
        }
    }

    fn run_snapshot_test(
        ctx: Arc<TestContext>,
        table_id: TableId,
        input_updates: Vec<MetaUpdate>,
        updates_after_snapshot: Vec<MetaUpdate>,
    ) {
        let location = WalLocation::new(DEFAULT_SHARD_ID as u64, table_id.as_u64());
        let log_store = MemLogStore::from_updates(&input_updates);
        let snapshot_store = MemSnapshotStore::new();

        ctx.runtime.block_on(async move {
            let snapshotter = Snapshotter {
                location,
                log_store,
                snapshot_store,
            };

            // Create and check the latest snapshot first.
            let mut manifest_builder = TableManifestDataBuilder::default();
            for update in &input_updates {
                manifest_builder.apply_update(update.clone()).unwrap();
            }
            let expect_table_manifest_data = manifest_builder.clone().build();

            // Do snapshot and check the snapshot result
            let snapshot = snapshotter.snapshot().await.unwrap();
            if input_updates.is_empty() {
                assert!(snapshot.is_none());
            } else {
                assert!(snapshot.is_some());
                let snapshot = snapshot.unwrap();
                assert_eq!(input_updates.len(), snapshot.original_logs_num);
                assert_eq!(snapshot.data, expect_table_manifest_data);
                assert_eq!(snapshot.end_seq, snapshotter.log_store.next_seq() - 1);
            }

            // The logs in the log store should be cleared after snapshot.
            let updates_in_log_store = snapshotter.log_store.to_meta_updates().await;
            assert!(updates_in_log_store.is_empty());

            // Write the updates after snapshot.
            for update in &updates_after_snapshot {
                manifest_builder.apply_update(update.clone()).unwrap();
                snapshotter.log_store.append(update.clone()).await.unwrap();
            }
            let expect_table_manifest_data = manifest_builder.build();
            // Do snapshot and check the snapshot result again.
            let snapshot = snapshotter.snapshot().await.unwrap();

            if input_updates.is_empty() && updates_after_snapshot.is_empty() {
                assert!(snapshot.is_none());
            } else {
                assert!(snapshot.is_some());
                let snapshot = snapshot.unwrap();
                assert_eq!(updates_after_snapshot.len(), snapshot.original_logs_num);
                assert_eq!(snapshot.data, expect_table_manifest_data);
                assert_eq!(snapshot.end_seq, snapshotter.log_store.next_seq() - 1);
            }
            // The logs in the log store should be cleared after snapshot.
            let updates_in_log_store = snapshotter.log_store.to_meta_updates().await;
            assert!(updates_in_log_store.is_empty());
        });
    }

    #[test]
    fn test_simple_snapshot() {
        let ctx = Arc::new(TestContext::new(
            "snapshot_merge_no_snapshot",
            SchemaId::from_u32(0),
        ));
        let table_id = ctx.alloc_table_id();
        let input_updates = vec![
            ctx.meta_update_add_table(table_id),
            ctx.meta_update_version_edit(table_id, Some(1)),
            ctx.meta_update_version_edit(table_id, Some(3)),
        ];

        run_snapshot_test(ctx, table_id, input_updates, vec![]);
    }

    #[test]
    fn test_snapshot_drop_table() {
        let ctx = Arc::new(TestContext::new(
            "snapshot_drop_table",
            SchemaId::from_u32(0),
        ));
        let table_id = ctx.alloc_table_id();
        let input_updates = vec![
            ctx.meta_update_add_table(table_id),
            ctx.meta_update_drop_table(table_id),
        ];

        run_snapshot_test(ctx, table_id, input_updates, vec![]);
    }

    #[test]
    fn test_snapshot_twice() {
        let ctx = Arc::new(TestContext::new(
            "snapshot_merge_no_snapshot",
            SchemaId::from_u32(0),
        ));
        let table_id = ctx.alloc_table_id();
        let input_updates = vec![
            ctx.meta_update_add_table(table_id),
            ctx.meta_update_version_edit(table_id, Some(1)),
            ctx.meta_update_version_edit(table_id, Some(3)),
        ];
        let updates_after_snapshot = vec![
            ctx.meta_update_version_edit(table_id, Some(4)),
            ctx.meta_update_version_edit(table_id, Some(8)),
        ];

        run_snapshot_test(ctx, table_id, input_updates, updates_after_snapshot);
    }
}
