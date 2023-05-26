// Copyright 2022-2023 CeresDB Project Authors. Licensed under Apache-2.0.

//! Implementation of Manifest

use std::{
    collections::{HashMap, VecDeque},
    fmt, mem,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
};

use arc_swap::ArcSwap;
use async_trait::async_trait;
use ceresdbproto::{manifest as manifest_pb, meta_storage::procedure_info::State};
use common_types::table::ShardId;
use common_util::{
    config::ReadableDuration,
    define_result,
    error::{BoxError, GenericError, GenericResult},
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
        BatchLogIteratorAdapter, ReadBoundary, ReadContext, ReadRequest, RegionId, ScanContext,
        ScanRequest, SequenceNumber, WalLocation, WalManagerRef, WriteContext,
    },
};

use super::{RecoverRequest, RecoverResult, TableResult};
use crate::{
    manifest::{
        error::*,
        meta_edit::{
            MetaEdit, MetaEditRequest, MetaUpdate, MetaUpdateDecoder, MetaUpdatePayload, Snapshot,
        },
        meta_snapshot::{self, MetaSnapshot, MetaSnapshotBuilder},
        storage::{MetaUpdateLogEntryIterator, *},
        DoSnapshotRequest, Manifest,
    },
    space::SpaceId,
    table::data::TableShardInfo,
};

/// Table meta set
///
/// Get snapshot of or modify table's metadata through it.
pub(crate) trait TableMetaSet: fmt::Debug + Send + Sync {
    fn get_table_snapshot(
        &self,
        space_id: SpaceId,
        table_id: TableId,
    ) -> Result<Option<MetaSnapshot>>;

    fn apply_edit_to_table(&self, update: MetaEditRequest) -> Result<()>;
}

/// Snapshot recoverer
///
/// Usually, it will recover the snapshot from storage(like disk, oss, etc).
// TODO: remove `LogStore` and related operations, it should be called directly but not in the
// `SnapshotReoverer`.
#[derive(Debug)]
struct SnapshotRecoverer<LogStore, SnapshotStore> {
    log_store: LogStore,
    snapshot_store: SnapshotStore,
    request: RecoverRequest,
    states: HashMap<TableId, RecoverState>,
}

#[derive(Debug)]
enum RecoverState {
    FromSnapshotStore,
    FromLogStore {
        latest_seq: SequenceNumber,
        meta_snapshot_builder: MetaSnapshotBuilder,
        is_empty: bool,
    },
    Failed {
        err: GenericError,
    },
    Success(Option<Snapshot>),
}

pub type SnapshotRecoverResult = TableResult<Option<Snapshot>>;

impl<LogStore, SnapshotStore> SnapshotRecoverer<LogStore, SnapshotStore>
where
    LogStore: MetaUpdateLogStore + Send + Sync,
    SnapshotStore: MetaUpdateSnapshotStore + Send + Sync,
{
    async fn recover_from_snapshot(&mut self) -> Result<()> {
        info!(
            "Manifest recover from snapshot store, request:{:?}",
            self.request
        );
        // Pull all snapshots concurrently.
        let load_snapshots = self
            .request
            .tables
            .iter()
            .map(|table| {
                let snapshot_path =
                    ObjectStoreBasedSnapshotStore::snapshot_path(table.space_id, table.table_id);
                let request = LoadSnapshotRequest { snapshot_path };
                let store = self.snapshot_store.clone();
                async move { (table.table_id, store.load(request).await) }
            })
            .collect::<Vec<_>>();
        let results = futures::future::join_all(load_snapshots).await;

        // Update state according to the pull results.
        for (table_id, result) in results {
            let state = self.states.get_mut(&table_id).expect("table id must exist");
            assert!(matches!(state, RecoverState::FromSnapshotStore));

            match result {
                Ok(snapshot_opt) => {
                    let mut is_empty = true;
                    let (latest_seq, meta_snapshot_builder) = match snapshot_opt {
                        Some(snapshot) => {
                            is_empty = false;
                            match snapshot.data {
                                Some(data) => (
                                    snapshot.end_seq,
                                    MetaSnapshotBuilder::new(
                                        Some(data.table_meta),
                                        data.version_meta,
                                    ),
                                ),
                                None => (snapshot.end_seq, MetaSnapshotBuilder::default()),
                            }
                        }
                        None => (SequenceNumber::MIN, MetaSnapshotBuilder::default()),
                    };
                    *state = RecoverState::FromLogStore {
                        latest_seq,
                        meta_snapshot_builder,
                        is_empty,
                    };
                }
                Err(e) => {
                    *state = RecoverState::Failed { err: Box::new(e) };
                }
            }
        }

        Ok(())
    }

    async fn recover_from_log(&mut self) -> Result<()> {
        self.region_based_recover_from_log().await
    }

    // TODO: now we just recover table by table as origin.
    // Maybe we should make it more concurrently.
    async fn table_based_recover_from_log(&mut self) -> Result<()> {
        todo!()
    }

    async fn region_based_recover_from_log(&mut self) -> Result<()> {
        info!(
            "Manifest recover from log store, request:{:?}",
            self.request
        );

        let request = ScanLogRequest::Region({
            ScanRegionRequest {
                opts: Options::default(),
                region_id: self.request.shard_id as RegionId,
            }
        });
        let mut reader = self.log_store.scan(request).await.unwrap();

        while let Some(MetaUpdateLogEntry {
            table_id,
            sequence: seq,
            meta_update: update,
        }) = reader.next_update().await?
        {
            if let Some(state) = self.states.get_mut(&table_id) {
                match state {
                    RecoverState::FromLogStore {
                        latest_seq,
                        meta_snapshot_builder,
                        is_empty,
                    } => {
                        *latest_seq = seq;
                        *is_empty = false;
                        if let Err(e) = meta_snapshot_builder.apply_update(update) {
                            *state = RecoverState::Failed { err: Box::new(e) }
                        }
                    }
                    RecoverState::Failed { .. } => {}
                    RecoverState::Success { .. } | RecoverState::FromSnapshotStore => {
                        unreachable!();
                    }
                }
            }
        }

        for (table_id, state) in self.states.iter_mut() {
            if let RecoverState::FromLogStore {
                latest_seq,
                meta_snapshot_builder,
                is_empty,
            } = state
            {
                let snapshot = if *is_empty {
                    None
                } else {
                    let meta_snapshot_builder = std::mem::take(meta_snapshot_builder);
                    let meta_snapshot = meta_snapshot_builder.build();
                    Some(Snapshot {
                        end_seq: *latest_seq,
                        data: meta_snapshot,
                    })
                };
                *state = RecoverState::Success(snapshot);
            }
        }

        Ok(())
    }

    async fn recover(&mut self) -> Result<Vec<SnapshotRecoverResult>> {
        info!(
            "Manifest start to recover from storage, request:{:?}",
            self.request
        );

        self.recover_from_snapshot().await?;
        self.recover_from_log().await?;
        let states = std::mem::take(&mut self.states);

        let table_results = states
            .into_iter()
            .map(|(table_id, state)| {
                let result = match state {
                    RecoverState::Failed { err } => Err(err),
                    RecoverState::Success(snapshot_opt) => Ok(snapshot_opt),
                    RecoverState::FromSnapshotStore | RecoverState::FromLogStore { .. } => {
                        unreachable!()
                    }
                };
                SnapshotRecoverResult { table_id, result }
            })
            .collect();

        info!(
            "Manifest finish to recover from storage, request:{:?}",
            self.request
        );
        Ok(table_results)
    }
}

/// Snapshot creator
///
/// Usually, it will get snapshot from memory, and store them to storage(like
/// disk, oss, etc).
// TODO: remove `LogStore` and related operations, it should be called directly but not in the
// `Snapshotter`.
#[derive(Debug, Clone)]
struct Snapshotter<LogStore, SnapshotStore> {
    log_store: LogStore,
    snapshot_store: SnapshotStore,
    snapshot_data_provider: Arc<dyn TableMetaSet>,
}

struct SnapshotRequest {
    space_id: SpaceId,
    table_id: TableId,
    location: WalLocation,
    opts: Options,
    end_seq: SequenceNumber,
}

impl<LogStore, SnapshotStore> Snapshotter<LogStore, SnapshotStore>
where
    LogStore: MetaUpdateLogStore + Send + Sync,
    SnapshotStore: MetaUpdateSnapshotStore + Send + Sync,
{
    /// Create a latest snapshot of the current logs.
    async fn snapshot(&self, request: SnapshotRequest) -> Result<()> {
        // Get snapshot data from memory.
        let table_snapshot_opt = self
            .snapshot_data_provider
            .get_table_snapshot(request.space_id, request.table_id)?;
        let snapshot = Snapshot {
            end_seq: request.end_seq,
            data: table_snapshot_opt,
        };

        // Update the current snapshot to the new one.
        let snapshot_path =
            ObjectStoreBasedSnapshotStore::snapshot_path(request.space_id, request.table_id);
        let store_snapshot_request = StoreSnapshotRequest {
            snapshot,
            snapshot_path,
        };
        self.snapshot_store.store(store_snapshot_request).await?;

        // Delete the expired logs after saving the snapshot.
        // TODO: Actually this operation can be performed background, and the failure of
        // it can be ignored.
        let delete_log_request = DeleteRequest {
            opts: request.opts,
            location: request.location,
            inclusive_end: request.end_seq,
        };
        self.log_store.delete_up_to(delete_log_request).await?;

        Ok(())
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

    table_meta_set: Arc<dyn TableMetaSet>,
}

impl ManifestImpl {
    pub(crate) async fn open(
        opts: Options,
        wal_manager: WalManagerRef,
        store: ObjectStoreRef,
        table_meta_set: Arc<dyn TableMetaSet>,
    ) -> Result<Self> {
        let manifest = Self {
            opts,
            wal_manager,
            store,
            num_updates_since_snapshot: Arc::new(AtomicUsize::new(0)),
            snapshot_write_guard: Arc::new(Mutex::new(())),
            table_meta_set,
        };

        Ok(manifest)
    }

    async fn store_update_to_wal(
        &self,
        meta_update: MetaUpdate,
        location: WalLocation,
    ) -> Result<SequenceNumber> {
        let log_store = WalBasedLogStore {
            wal_manager: self.wal_manager.clone(),
        };

        let request = AppendRequest {
            opts: self.opts.clone(),
            location,
            meta_update,
        };

        let latest_sequence = log_store.append(request).await?;
        self.num_updates_since_snapshot
            .fetch_add(1, Ordering::Relaxed);

        Ok(latest_sequence)
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
    ) -> Result<()> {
        if !force {
            let num_updates = self.num_updates_since_snapshot.load(Ordering::Relaxed);
            if num_updates < self.opts.snapshot_every_n_updates {
                return Ok(());
            }
        }

        if let Ok(_guard) = self.snapshot_write_guard.try_lock() {
            let log_store = WalBasedLogStore {
                wal_manager: self.wal_manager.clone(),
            };
            let snapshot_store = ObjectStoreBasedSnapshotStore {
                store: self.store.clone(),
            };
            let end_seq = self.wal_manager.sequence_num(location).await.unwrap();
            let snapshotter = Snapshotter {
                log_store,
                snapshot_store,
                snapshot_data_provider: self.table_meta_set.clone(),
            };

            let request = SnapshotRequest {
                space_id,
                table_id,
                opts: self.opts.clone(),
                end_seq,
                location,
            };

            snapshotter.snapshot(request).await
        } else {
            debug!("Avoid concurrent snapshot");
            Ok(())
        }
    }

    // with snapshot guard held
    fn decrease_num_updates(&self) {
        if self.opts.snapshot_every_n_updates
            > self.num_updates_since_snapshot.load(Ordering::Relaxed)
        {
            self.num_updates_since_snapshot.store(0, Ordering::Relaxed);
        } else {
            self.num_updates_since_snapshot
                .fetch_sub(self.opts.snapshot_every_n_updates, Ordering::Relaxed);
        }
    }
}

#[async_trait]
impl Manifest for ManifestImpl {
    async fn apply_edit(&self, request: MetaEditRequest) -> GenericResult<()> {
        info!("Manifest store update, request:{:?}", request);

        // Update storage.
        let MetaEditRequest {
            shard_info,
            meta_edit,
        } = request.clone();

        let meta_update = MetaUpdate::try_from(meta_edit).box_err()?;
        let table_id = meta_update.table_id();
        let shard_id = shard_info.shard_id;
        let location = WalLocation::new(shard_id as u64, table_id.as_u64());
        let space_id = meta_update.space_id();

        self.maybe_do_snapshot(space_id, table_id, location, false)
            .await?;

        self.store_update_to_wal(meta_update, location).await?;

        // Update memory.
        self.table_meta_set.apply_edit_to_table(request).box_err()
    }

    async fn recover(&self, recover_req: &RecoverRequest) -> GenericResult<Vec<RecoverResult>> {
        info!("Manifest recover, request:{:?}", recover_req);

        let states = recover_req
            .tables
            .iter()
            .map(|table| (table.table_id, RecoverState::FromSnapshotStore))
            .collect::<HashMap<_, _>>();

        let log_store = WalBasedLogStore {
            wal_manager: self.wal_manager.clone(),
        };

        let snapshot_store = ObjectStoreBasedSnapshotStore {
            store: self.store.clone(),
        };

        let mut recoverer = SnapshotRecoverer {
            log_store,
            snapshot_store,
            request: recover_req.clone(),
            states,
        };
        let recover_results = recoverer.recover().await.box_err()?;

        // Apply it to table.
        let mut final_results = Vec::with_capacity(recover_results.len());
        for recover_result in recover_results {
            match recover_result.result {
                Ok(Some(snapshot)) => {
                    if let Some(snapshot) = snapshot.data {
                        let meta_edit = MetaEdit::Snapshot(snapshot);
                        let request = MetaEditRequest {
                            shard_info: TableShardInfo::new(recover_req.shard_id),
                            meta_edit,
                        };
                        if let Err(e) = self.table_meta_set.apply_edit_to_table(request).box_err() {
                            final_results.push(RecoverResult {
                                table_id: recover_result.table_id,
                                result: Err(e),
                            });
                        }
                    } else {
                        warn!(
                            "Manifest try recover a table has not exist, table_id:{}",
                            recover_result.table_id
                        );
                    }
                    final_results.push(RecoverResult {
                        table_id: recover_result.table_id,
                        result: Ok(()),
                    })
                }

                Ok(None) => {}

                Err(e) => final_results.push(RecoverResult {
                    table_id: recover_result.table_id,
                    result: Err(e),
                }),
            }
        }

        Ok(final_results)
    }

    async fn do_snapshot(&self, request: DoSnapshotRequest) -> GenericResult<()> {
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
            meta_edit::{
                AddTableMeta, AlterOptionsMeta, AlterSchemaMeta, DropTableMeta, MetaEdit,
                MetaUpdate, VersionEditMeta,
            },
            DoSnapshotRequest, Manifest, TableInSpace,
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

    #[derive(Debug, Default)]
    struct MockProviderImpl {
        builder: std::sync::Mutex<MetaSnapshotBuilder>,
    }

    impl TableMetaSet for MockProviderImpl {
        fn get_table_snapshot(
            &self,
            _space_id: SpaceId,
            _table_id: TableId,
        ) -> Result<Option<MetaSnapshot>> {
            let builder = self.builder.lock().unwrap();
            Ok(builder.clone().build())
        }

        fn apply_edit_to_table(&self, request: MetaEditRequest) -> Result<()> {
            let mut builder = self.builder.lock().unwrap();
            let MetaEditRequest {
                shard_info: _,
                meta_edit,
            } = request;

            match meta_edit {
                MetaEdit::Update(update) => {
                    builder.apply_update(update).unwrap();
                }
                MetaEdit::Snapshot(meta_snapshot) => {
                    let MetaSnapshot {
                        table_meta,
                        version_meta,
                    } = meta_snapshot;
                    *builder = MetaSnapshotBuilder::new(Some(table_meta), version_meta);
                }
            }

            Ok(())
        }
    }

    struct TestContext {
        dir: PathBuf,
        runtime: Arc<Runtime>,
        options: Options,
        schema_id: SchemaId,
        table_seq_gen: TableSeqGenerator,
        mock_provider: Arc<MockProviderImpl>,
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
                mock_provider: Arc::new(MockProviderImpl::default()),
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
                self.mock_provider.clone(),
            )
            .await
            .unwrap()
        }

        async fn check_table_manifest_data_with_manifest(
            &self,
            recover_req: &RecoverRequest,
            expected: &Option<MetaSnapshot>,
            manifest: &ManifestImpl,
        ) {
            manifest.recover(recover_req).await.unwrap();
            let data = self.mock_provider.builder.lock().unwrap().clone().build();
            assert_eq!(&data, expected);
        }

        async fn check_table_manifest_data(
            &self,
            recover_req: &RecoverRequest,
            expected: &Option<MetaSnapshot>,
        ) {
            let manifest = self.open_manifest().await;
            self.check_table_manifest_data_with_manifest(recover_req, expected, &manifest)
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
                files_to_add: vec![],
                files_to_delete: vec![],
                mems_to_remove: vec![],
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
            manifest_data_builder: &mut MetaSnapshotBuilder,
            manifest: &ManifestImpl,
        ) {
            let shard_info = TableShardInfo {
                shard_id: DEFAULT_SHARD_ID,
            };

            let add_table = self.meta_update_add_table(table_id);
            let edit_req = {
                MetaEditRequest {
                    shard_info,
                    meta_edit: MetaEdit::Update(add_table.clone()),
                }
            };

            manifest.apply_edit(edit_req).await.unwrap();
            manifest_data_builder
                .apply_update(add_table.clone())
                .unwrap();
        }

        async fn drop_table_with_manifest(
            &self,
            table_id: TableId,
            manifest_data_builder: &mut MetaSnapshotBuilder,
            manifest: &ManifestImpl,
        ) {
            let shard_info = TableShardInfo {
                shard_id: DEFAULT_SHARD_ID,
            };

            let drop_table = self.meta_update_drop_table(table_id);
            let edit_req = {
                MetaEditRequest {
                    shard_info,
                    meta_edit: MetaEdit::Update(drop_table.clone()),
                }
            };
            manifest.apply_edit(edit_req).await.unwrap();
            manifest_data_builder
                .apply_update(drop_table.clone())
                .unwrap();
        }

        async fn version_edit_table_with_manifest(
            &self,
            table_id: TableId,
            flushed_seq: Option<SequenceNumber>,
            manifest_data_builder: &mut MetaSnapshotBuilder,
            manifest: &ManifestImpl,
        ) {
            let shard_info = TableShardInfo {
                shard_id: DEFAULT_SHARD_ID,
            };

            let version_edit = self.meta_update_version_edit(table_id, flushed_seq);
            let edit_req = {
                MetaEditRequest {
                    shard_info,
                    meta_edit: MetaEdit::Update(version_edit.clone()),
                }
            };
            manifest.apply_edit(edit_req).await.unwrap();
            manifest_data_builder
                .apply_update(version_edit.clone())
                .unwrap();
        }

        async fn add_table(
            &self,
            table_id: TableId,
            manifest_data_builder: &mut MetaSnapshotBuilder,
        ) {
            let manifest = self.open_manifest().await;
            self.add_table_with_manifest(table_id, manifest_data_builder, &manifest)
                .await;
        }

        async fn drop_table(
            &self,
            table_id: TableId,
            manifest_data_builder: &mut MetaSnapshotBuilder,
        ) {
            let manifest = self.open_manifest().await;

            self.drop_table_with_manifest(table_id, manifest_data_builder, &manifest)
                .await;
        }

        async fn version_edit_table(
            &self,
            table_id: TableId,
            manifest_data_builder: &mut MetaSnapshotBuilder,
        ) {
            let manifest = self.open_manifest().await;
            self.version_edit_table_with_manifest(table_id, None, manifest_data_builder, &manifest)
                .await;
        }

        async fn alter_table_options(
            &self,
            table_id: TableId,
            manifest_data_builder: &mut MetaSnapshotBuilder,
        ) {
            let manifest = self.open_manifest().await;

            let shard_info = TableShardInfo {
                shard_id: DEFAULT_SHARD_ID,
            };

            let alter_options = self.meta_update_alter_table_options(table_id);
            let edit_req = {
                MetaEditRequest {
                    shard_info,
                    meta_edit: MetaEdit::Update(alter_options.clone()),
                }
            };
            manifest.apply_edit(edit_req).await.unwrap();
            manifest_data_builder.apply_update(alter_options).unwrap();
        }

        async fn alter_table_schema(
            &self,
            table_id: TableId,
            manifest_data_builder: &mut MetaSnapshotBuilder,
        ) {
            let manifest = self.open_manifest().await;

            let shard_info = TableShardInfo {
                shard_id: DEFAULT_SHARD_ID,
            };

            let alter_schema = self.meta_update_alter_table_schema(table_id);
            let edit_req = {
                MetaEditRequest {
                    shard_info,
                    meta_edit: MetaEdit::Update(alter_schema.clone()),
                }
            };

            manifest.apply_edit(edit_req).await.unwrap();
            manifest_data_builder.apply_update(alter_schema).unwrap();
        }
    }

    fn run_basic_manifest_test<F>(ctx: TestContext, update_table_meta: F)
    where
        F: for<'a> FnOnce(
            &'a TestContext,
            TableId,
            &'a mut MetaSnapshotBuilder,
        ) -> BoxFuture<'a, ()>,
    {
        let runtime = ctx.runtime.clone();
        runtime.block_on(async move {
            let table_id = ctx.alloc_table_id();
            let mut manifest_data_builder = MetaSnapshotBuilder::default();

            update_table_meta(&ctx, table_id, &mut manifest_data_builder).await;
            let recover_req = RecoverRequest {
                tables: vec![TableInSpace {
                    table_id,
                    space_id: DEFAULT_SHARD_ID,
                }],
                shard_id: ctx.schema_id.as_u32(),
            };
            let expected_table_manifest_data = manifest_data_builder.build();
            ctx.check_table_manifest_data(&recover_req, &expected_table_manifest_data)
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
            let mut manifest_data_builder = MetaSnapshotBuilder::default();
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
            let recover_req = RecoverRequest {
                tables: vec![TableInSpace {
                    table_id,
                    space_id: DEFAULT_SHARD_ID,
                }],
                shard_id: ctx.schema_id.as_u32(),
            };
            ctx.check_table_manifest_data_with_manifest(
                &recover_req,
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
            let recover_req = RecoverRequest {
                tables: vec![TableInSpace {
                    table_id,
                    space_id: ctx.schema_id.as_u32(),
                }],
                shard_id: DEFAULT_SHARD_ID,
            };
            let mut manifest_data_builder = MetaSnapshotBuilder::default();
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
                &recover_req,
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
                &recover_req,
                &manifest_data_builder.build(),
                &manifest,
            )
            .await;
        });
    }

    #[derive(Debug, Clone)]
    struct MemLogStore {
        table_id: TableId,
        logs: Arc<std::sync::Mutex<Vec<Option<MetaUpdate>>>>,
    }

    impl MemLogStore {
        fn from_updates(table_id: TableId, updates: &[MetaUpdate]) -> Self {
            let mut buf = Vec::with_capacity(updates.len());
            buf.extend(updates.iter().map(|v| Some(v.clone())));
            Self {
                table_id,
                logs: Arc::new(std::sync::Mutex::new(buf)),
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
        type Iter = vec::IntoIter<MetaUpdateLogEntry>;

        async fn scan(&self, request: ScanLogRequest) -> Result<Self::Iter> {
            let request = if let ScanLogRequest::Table(req) = request{
                req
            } else {
                panic!("")
            };

            let logs = self.logs.lock().unwrap();
            let start = request.start.as_start_sequence_number().unwrap() as usize;

            let mut exist_logs = Vec::new();
            let logs_with_idx = logs.iter().enumerate();
            for (idx, update) in logs_with_idx {
                if idx < start || update.is_none() {
                    continue;
                }

                let log_entry = MetaUpdateLogEntry {
                    sequence: idx as u64,
                    table_id: self.table_id,
                    meta_update: update.clone().unwrap(),
                };
                exist_logs.push(log_entry);
            }

            Ok(exist_logs.into_iter())
        }
    
        async fn append(&self, request: AppendRequest) -> Result<SequenceNumber> {
            let mut logs = self.logs.lock().unwrap();
            let seq = logs.len() as u64;
            logs.push(Some(request.meta_update));

            Ok(seq)
        }
    
        async fn delete_up_to(&self, request: DeleteRequest) -> Result<()> {
            let mut logs = self.logs.lock().unwrap();
            for i in 0..=request.inclusive_end {
                logs[i as usize] = None;
            }

            Ok(())
        }
    }

    #[async_trait]
    impl<T> MetaUpdateLogEntryIterator for T
    where
        T: Iterator<Item = MetaUpdateLogEntry> + Send + Sync,
    {
        async fn next_update(&mut self) -> Result<Option<MetaUpdateLogEntry>> {
            Ok(self.next())
        }
    }

    #[derive(Debug, Clone)]
    struct MemSnapshotStore {
        curr_snapshot: Arc<Mutex<Option<Snapshot>>>,
    }

    impl MemSnapshotStore {
        fn new() -> Self {
            Self {
                curr_snapshot: Arc::new(Mutex::new(None)),
            }
        }
    }

    #[async_trait]
    impl MetaUpdateSnapshotStore for MemSnapshotStore {
        async fn store(&self, request: StoreSnapshotRequest) -> Result<()> {
            let mut curr_snapshot = self.curr_snapshot.lock().await;
            *curr_snapshot = Some(request.snapshot.clone());
            Ok(())
        }

        async fn load(&self, request: LoadSnapshotRequest) -> Result<Option<Snapshot>> {
            let curr_snapshot = self.curr_snapshot.lock().await;
            Ok(curr_snapshot.clone())
        }
    }

    // The test idea is:
    //  + 1. Use `MetaSnapshotBuilder` to build expected result.
    //  + 2. Make snapshot and store.
    //  + 3. Load snapshot.
    //  + 4. Compare loaded snapshot and expected.
    fn run_snapshot_test(
        ctx: Arc<TestContext>,
        table_id: TableId,
        input_updates: Vec<MetaUpdate>,
        updates_after_snapshot: Vec<MetaUpdate>,
    ) {
        let log_store = MemLogStore::from_updates(table_id, &[]);
        let snapshot_store = MemSnapshotStore::new();
        let snapshot_data_provider = ctx.mock_provider.clone();

        ctx.runtime.block_on(async move {
            let log_store = log_store;
            let snapshot_store = snapshot_store;
            let snapshot_provider = snapshot_data_provider;
            let mut manifest_builder = MetaSnapshotBuilder::default();

            check_snapshot_workflow(&mut manifest_builder, &log_store, &snapshot_store, snapshot_provider.clone(), table_id, &input_updates, input_updates.is_empty());
            check_snapshot_workflow(&mut manifest_builder, &log_store, &snapshot_store, snapshot_provider, table_id, &updates_after_snapshot, input_updates.is_empty() && updates_after_snapshot.is_empty());
            // 1. Test write and do snapshot
            // Create and check the latest snapshot first.
            
            // 2. Test write after snapshot, and do snapshot again
            // Write the updates after snapshot.
            // for update in &updates_after_snapshot {
            //     manifest_builder.apply_update(update.clone()).unwrap();

            //     let request = AppendRequest {
            //         opts: Options::default(),
            //         location: WalLocation::new(DEFAULT_SHARD_ID as RegionId, table_id.as_u64()),
            //         meta_update: update.clone(),
            //     };
            //     log_store.append(request).await.unwrap();
            //     let request = MetaEditRequest {
            //         shard_info: TableShardInfo::new(DEFAULT_SHARD_ID),
            //         meta_edit: MetaEdit::Update(update.clone()),
            //     };
            //     snapshot_provider.apply_edit_to_table(request).unwrap();
            // }
            // let expect_table_manifest_data = manifest_builder.build();
            // // Do snapshot and check the snapshot result again.
            // let snapshot =
            //     build_and_store_snapshot(&log_store, &snapshot_store, snapshot_provider, table_id)
            //         .await;

            // if input_updates.is_empty() && updates_after_snapshot.is_empty() {
            //     assert!(snapshot.is_none());
            // } else {
            //     assert!(snapshot.is_some());
            //     let snapshot = snapshot.unwrap();
            //     assert_eq!(snapshot.data, expect_table_manifest_data);
            //     assert_eq!(snapshot.end_seq, log_store.next_seq() - 1);

            //     let recovered_snapshot = recover_snapshot(&log_store, &snapshot_store).await;
            //     assert_eq!(snapshot, recovered_snapshot.unwrap());
            // }
            // // The logs in the log store should be cleared after snapshot.
            // let updates_in_log_store = log_store.to_meta_updates().await;
            // assert!(updates_in_log_store.is_empty());
        });
    }

    async fn check_snapshot_workflow(manifest_builder: &mut MetaSnapshotBuilder,
        log_store: &MemLogStore, snapshot_store: &MemSnapshotStore, snapshot_provider: Arc<MockProviderImpl>,
        table_id: TableId, input_updates: &[MetaUpdate],
        is_empty: bool)
    {
        for update in input_updates {
            manifest_builder.apply_update(update.clone()).unwrap();
            
            let request = AppendRequest {
                opts: Options::default(),
                location: WalLocation::new(DEFAULT_SHARD_ID as RegionId, table_id.as_u64()),
                meta_update: update.clone(),
            };
            log_store.append(request).await.unwrap();
            
            let request = MetaEditRequest {
                shard_info: TableShardInfo::new(DEFAULT_SHARD_ID),
                meta_edit: MetaEdit::Update(update.clone()),
            };
            snapshot_provider.apply_edit_to_table(request).unwrap();
        }
        let expect_table_manifest_data = manifest_builder.clone().build();

        // Do snapshot from memory and check the snapshot result.
        // Build and store.
        build_and_store_snapshot(
            &log_store,
            &snapshot_store,
            snapshot_provider.clone(),
            table_id,
        )
        .await;

        // Fetch 
        let tables = vec![TableInSpace { table_id, space_id: 0 }];
        let recovered_snapshot = recover_snapshot(&log_store, &snapshot_store, &tables, DEFAULT_SHARD_ID).await;
        if is_empty {
            assert!(recovered_snapshot.is_none());
        } else {
            assert!(recovered_snapshot.is_some());
            let snapshot = recovered_snapshot.unwrap();
            assert_eq!(snapshot.data, expect_table_manifest_data);
            assert_eq!(snapshot.end_seq, log_store.next_seq() - 1);
        }

        // The logs in the log store should be cleared after snapshot.
        let updates_in_log_store = log_store.to_meta_updates().await;
        assert!(updates_in_log_store.is_empty());
    }

    async fn build_and_store_snapshot(
        log_store: &MemLogStore,
        snapshot_store: &MemSnapshotStore,
        snapshot_provider: Arc<MockProviderImpl>,
        table_id: TableId,
    ) {
        let end_seq = log_store.next_seq() - 1;
        let space_id = 0;
        let snapshotter = Snapshotter {
            log_store: log_store.clone(),
            snapshot_store: snapshot_store.clone(),
            snapshot_data_provider: snapshot_provider,
        };
        let request = SnapshotRequest {
            space_id,
            table_id,
            location: WalLocation::new(DEFAULT_SHARD_ID as RegionId, table_id.as_u64()),
            opts: Options::default(),
            end_seq,
        };
        snapshotter.snapshot(request).await.unwrap();
    }

    async fn recover_snapshot(
        log_store: &MemLogStore,
        snapshot_store: &MemSnapshotStore,
        tables: &[TableInSpace],
        shard_id: ShardId,
    ) -> Option<Snapshot> {
        let request = RecoverRequest {
            tables: tables.to_owned(),
            shard_id,
        };
        let states = tables.iter().map(|table| {
            (table.table_id, RecoverState::FromSnapshotStore)
        }).collect::<HashMap<_,_>>();

        let mut recoverer = SnapshotRecoverer {
            log_store: log_store.clone(),
            snapshot_store: snapshot_store.clone(),
            request,
            states,
            // log_store: log_store.clone(),
            // snapshot_store: snapshot_store.clone(),
        };
        recoverer.recover().await.unwrap().pop().unwrap().result.unwrap()
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
