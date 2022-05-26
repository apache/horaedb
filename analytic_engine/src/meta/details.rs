// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

//! Implementation of Manifest

use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};

use async_trait::async_trait;
use common_types;
use common_util::define_result;
use log::{error, info, warn};
use serde_derive::Deserialize;
use snafu::{ResultExt, Snafu};
use tokio::sync::Mutex;
use wal::{
    log_batch::{LogWriteBatch, LogWriteEntry},
    manager::{
        LogIterator, ReadBoundary, ReadContext, ReadRequest, RegionId, SequenceNumber, WalManager,
        WriteContext,
    },
};

use crate::meta::{
    meta_data::ManifestData,
    meta_update::{
        MetaUpdate, MetaUpdateDecoder, MetaUpdatePayload, SnapshotManifestMeta, VersionEditMeta,
    },
    Manifest,
};

/// The region id manifest used.
const MANIFEST_REGION_ID: RegionId = 1;
/// The region id to store snapshot state.
const SNAPSHOT_STATE_REGION_ID: RegionId = 2;
// The first region id of snapshot region.
const FIRST_SNAPSHOT_REGION_ID: RegionId = 3;
// The second region id of snapshot region.
const SECOND_SNAPSHOT_REGION_ID: RegionId = 4;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Failed to write update to wal, err:{}", source))]
    WriteWal { source: wal::manager::Error },

    #[snafu(display("Failed to read wal, err:{}", source))]
    ReadWal { source: wal::manager::Error },

    #[snafu(display("Failed to read log entry, err:{}", source))]
    ReadEntry { source: wal::manager::Error },

    #[snafu(display("Failed to apply meta update, err:{}", source))]
    ApplyUpdate {
        source: crate::meta::meta_data::Error,
    },

    #[snafu(display("Failed to clean wal, err:{}", source))]
    CleanWal { source: wal::manager::Error },

    #[snafu(display("Failed to clean snapshot, region_id:{}, err:{}", region_id, source))]
    CleanSnapshot {
        region_id: RegionId,
        source: wal::manager::Error,
    },

    #[snafu(display("Failed to load sequence of manifest, err:{}", source))]
    LoadSequence { source: wal::manager::Error },

    #[snafu(display("Failed to load sequence of snapshot state, err:{}", source))]
    LoadSnapshotMetaSequence { source: wal::manager::Error },

    #[snafu(display("Failed to clean snapshot state, err:{}", source))]
    CleanSnapshotState { source: wal::manager::Error },
}

define_result!(Error);

const STORE_UPDATE_BATCH: usize = 500;

/// Implementation of [MetaUpdateReader]
#[derive(Debug)]
pub struct MetaUpdateReaderImpl<W: WalManager> {
    iter: W::Iterator,
}

impl<W: WalManager + Send + Sync> MetaUpdateReaderImpl<W> {
    async fn next_update(&mut self) -> Result<Option<MetaUpdate>> {
        let decoder = MetaUpdateDecoder;

        match self.iter.next_log_entry(&decoder).context(ReadEntry)? {
            Some(entry) => Ok(Some(entry.payload)),
            None => Ok(None),
        }
    }
}

/// State to track manifest snapshot.
#[derive(Debug, Default)]
struct SnapshotState {
    /// Meta data of the snapshot of the manifest, `None` if there is no
    /// snapshot.
    snapshot_meta: Option<SnapshotManifestMeta>,
}

impl SnapshotState {
    fn install_snapshot_meta(&mut self, snapshot_meta: SnapshotManifestMeta) {
        self.snapshot_meta = Some(snapshot_meta);
    }

    fn next_snapshot_region_id(&self) -> RegionId {
        match self.snapshot_meta {
            Some(snapshot_meta) => {
                if snapshot_meta.snapshot_region_id == FIRST_SNAPSHOT_REGION_ID {
                    SECOND_SNAPSHOT_REGION_ID
                } else {
                    FIRST_SNAPSHOT_REGION_ID
                }
            }
            None => FIRST_SNAPSHOT_REGION_ID,
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct Options {
    pub snapshot_every_n_updates: usize,
    pub paranoid_checks: bool,
}

impl Default for Options {
    fn default() -> Self {
        Self {
            snapshot_every_n_updates: 10_000,
            paranoid_checks: true,
        }
    }
}

// TODO(yingwen): Wrap into an inner struct if there are too many Arc fields.
/// Implementation of [Manifest].
#[derive(Debug, Clone)]
pub struct ManifestImpl<W> {
    /// Region id for this manifest.
    manifest_region_id: RegionId,
    /// Wal manager, the manifest use its own wal manager instance.
    wal_manager: Arc<W>,
    opts: Options,

    // Snapshot related:
    /// Region id to store snapshot state.
    snapshot_state_region_id: RegionId,
    snapshot_state: Arc<Mutex<SnapshotState>>,
    /// Number of updates wrote to wal since last snapshot.
    num_updates_since_snapshot: Arc<AtomicUsize>,
}

impl<W: WalManager + Send + Sync> ManifestImpl<W> {
    pub async fn open(wal_manager: W, opts: Options) -> Result<Self> {
        let mut manifest = Self {
            manifest_region_id: MANIFEST_REGION_ID,
            wal_manager: Arc::new(wal_manager),
            opts,
            snapshot_state_region_id: SNAPSHOT_STATE_REGION_ID,
            snapshot_state: Arc::new(Mutex::new(SnapshotState::default())),
            num_updates_since_snapshot: Arc::new(AtomicUsize::new(0)),
        };

        manifest.load_snapshot_state().await?;

        Ok(manifest)
    }

    async fn load_snapshot_state(&mut self) -> Result<()> {
        // Load snapshot state.
        let mut reader = self.read_updates_from_region(
            self.snapshot_state_region_id,
            ReadBoundary::Min,
            ReadBoundary::Max,
        )?;

        let mut last_snapshot_meta = None;
        while let Some(update) = reader.next_update().await? {
            // If the entry is a snapshot entry.
            if let Some(snapshot_meta) = update.snapshot_manifest_meta() {
                last_snapshot_meta = Some(snapshot_meta);
            } else {
                error!(
                    "Manifest found non snapshot state entry, entry:{:?}",
                    update
                );
            }
        }

        let mut snapshot_state = self.snapshot_state.lock().await;
        if let Some(snapshot_meta) = last_snapshot_meta {
            // Previous snapshot exists.
            snapshot_state.install_snapshot_meta(snapshot_meta);

            info!(
                "Manifest found snapshot_meta, snapshot_state:{:?}, last_snapshot_meta:{:?}",
                snapshot_state, last_snapshot_meta
            );
        }

        Ok(())
    }

    fn read_updates_from_region(
        &self,
        region_id: RegionId,
        start: ReadBoundary,
        end: ReadBoundary,
    ) -> Result<MetaUpdateReaderImpl<W>> {
        let request = ReadRequest {
            region_id,
            start,
            end,
        };
        let ctx = ReadContext::default();

        let iter = self.wal_manager.read(&ctx, &request).context(ReadWal)?;

        Ok(MetaUpdateReaderImpl { iter })
    }

    /// Load meta update from region of given `region_id` and apply into
    /// `manifest_data`.
    async fn load_data_from_region(
        &self,
        region_id: RegionId,
        manifest_data: &mut ManifestData,
    ) -> Result<()> {
        self.load_data_from_region_in_range(
            region_id,
            ReadBoundary::Min,
            ReadBoundary::Max,
            manifest_data,
        )
        .await?;

        Ok(())
    }

    /// Load meta update in given range from region of given `region_id`
    /// boundary, and apply into `manifest_data`. Returns number of MetaUpdates
    /// loaded.
    async fn load_data_from_region_in_range(
        &self,
        region_id: RegionId,
        start: ReadBoundary,
        end: ReadBoundary,
        manifest_data: &mut ManifestData,
    ) -> Result<usize> {
        let mut reader = self.read_updates_from_region(region_id, start, end)?;
        let mut loaded = 0;

        while let Some(update) = reader.next_update().await? {
            if let Err(e) = manifest_data.apply_meta_update(update).context(ApplyUpdate) {
                if self.opts.paranoid_checks {
                    return Err(e);
                } else {
                    warn!("Manifest load meta update failed, err:{:?}", e);
                    continue;
                }
            }
            loaded += 1;
        }
        Ok(loaded)
    }

    /// Load data and create a snapshot.
    async fn create_snapshot(&self) -> Result<ManifestData> {
        info!("Manifest try to create snapshot");

        // Acquire snapshot lock.
        let mut snapshot_state = self.snapshot_state.lock().await;
        let last_snapshot_meta = snapshot_state.snapshot_meta;
        let next_snapshot_region_id = snapshot_state.next_snapshot_region_id();

        // Clean next snapshot region.
        self.clean_snapshot(next_snapshot_region_id).await?;

        // Load previous snapshot.
        let mut manifest_start = ReadBoundary::Min;
        let mut manifest_data = ManifestData::default();
        if let Some(snapshot_meta) = last_snapshot_meta {
            // Load manifest from last snapshot first.
            self.load_data_from_region(snapshot_meta.snapshot_region_id, &mut manifest_data)
                .await?;
            // The sequence after snapshot.
            manifest_start = ReadBoundary::Excluded(snapshot_meta.sequence);
        }

        // Get current sequence, data until this sequence can be loaded to create next
        // snapshot.
        let snapshot_sequence = self
            .wal_manager
            .sequence_num(self.manifest_region_id)
            .context(LoadSequence)?;

        // Load manifest up to `snapshot_sequence`.
        let num_loaded_from_manifest = self
            .load_data_from_region_in_range(
                self.manifest_region_id,
                manifest_start,
                ReadBoundary::Included(snapshot_sequence),
                &mut manifest_data,
            )
            .await?;

        // Store snapshot.
        self.store_snapshot_to_region(next_snapshot_region_id, &manifest_data)
            .await?;

        // Store snapshot state.
        let next_snapshot_meta = SnapshotManifestMeta {
            snapshot_region_id: next_snapshot_region_id,
            sequence: snapshot_sequence,
        };
        self.store_snapshot_state(next_snapshot_meta).await?;

        info!(
            "Manifest stored snapshot,
            next_snapshot_meta:{:?},
            last_snapshot_meta:{:?},
            snapshot_state_before_install:{:?},
            num_updates_since_snapshot:{}",
            next_snapshot_meta,
            last_snapshot_meta,
            snapshot_state,
            self.num_updates_since_snapshot()
        );

        // Install new snapshot, also bump next snapshot region id.
        snapshot_state.install_snapshot_meta(next_snapshot_meta);

        // Data before sequence of the snapshot can also be removed.
        self.wal_manager
            .mark_delete_entries_up_to(self.manifest_region_id, snapshot_sequence)
            .await
            .context(CleanWal)?;

        self.decrease_num_updates(num_loaded_from_manifest);

        info!(
            "Manifest create snapshot done,
            next_snapshot_meta:{:?},
            last_snapshot_meta:{:?},
            snapshot_state:{:?},
            num_loaded_from_manifest:{},
            num_updates:{}",
            next_snapshot_meta,
            last_snapshot_meta,
            snapshot_state,
            num_loaded_from_manifest,
            self.num_updates_since_snapshot()
        );

        Ok(manifest_data)
    }

    async fn clean_snapshot(&self, snapshot_region_id: RegionId) -> Result<()> {
        info!("Clean snapshot, snapshot_region_id:{}", snapshot_region_id);

        self.wal_manager
            .mark_delete_entries_up_to(snapshot_region_id, common_types::MAX_SEQUENCE_NUMBER)
            .await
            .context(CleanSnapshot {
                region_id: snapshot_region_id,
            })
            .map_err(|e| {
                error!(
                    "Failed to clean snapshot, region_id:{}, err:{}",
                    snapshot_region_id, e
                );
                e
            })
    }

    async fn store_snapshot_state(&self, snapshot_meta: SnapshotManifestMeta) -> Result<()> {
        // Get current snapshot state sequence.
        let snapshot_state_sequence = self
            .wal_manager
            .sequence_num(self.snapshot_state_region_id)
            .context(LoadSnapshotMetaSequence)?;
        // Write a snapshot entry into the region.

        self.store_update_to_region(
            self.snapshot_state_region_id,
            MetaUpdate::SnapshotManifest(snapshot_meta),
        )
        .await?;
        // Clean old snapshot state.
        self.wal_manager
            .mark_delete_entries_up_to(self.snapshot_state_region_id, snapshot_state_sequence)
            .await
            .context(CleanSnapshotState)
    }

    async fn store_update_to_region(
        &self,
        region_id: RegionId,
        update: MetaUpdate,
    ) -> Result<SequenceNumber> {
        info!(
            "Manifest impl store update, region_id:{}, update:{:?}",
            region_id, update
        );

        let mut log_batch = LogWriteBatch::new(region_id);
        log_batch.push(LogWriteEntry {
            payload: MetaUpdatePayload::from(update),
        });

        let write_ctx = WriteContext::default();

        self.wal_manager
            .write(&write_ctx, &log_batch)
            .await
            .context(WriteWal)
    }

    async fn store_updates_to_region(
        &self,
        region_id: RegionId,
        updates: &[MetaUpdate],
    ) -> Result<SequenceNumber> {
        let mut log_batch = LogWriteBatch::new(region_id);
        for update in updates {
            log_batch.push(LogWriteEntry {
                payload: MetaUpdatePayload::from(update),
            });
        }

        let write_ctx = WriteContext::default();

        self.wal_manager
            .write(&write_ctx, &log_batch)
            .await
            .context(WriteWal)
    }

    async fn store_snapshot_to_region(
        &self,
        region_id: RegionId,
        snapshot: &ManifestData,
    ) -> Result<()> {
        info!("Manifest store snapshot to region, region_id:{}", region_id);

        let mut meta_updates = Vec::with_capacity(STORE_UPDATE_BATCH);

        // Store all spaces.
        for (space_id, space_meta_data) in &snapshot.spaces {
            let space_meta = space_meta_data.space_meta.clone();
            // Add this space.
            meta_updates.push(MetaUpdate::AddSpace(space_meta));

            // Add all tables to the space.
            for (table_id, table_meta_data) in &space_meta_data.tables {
                let table_meta = table_meta_data.table_meta.clone();
                // Store table meta.
                meta_updates.push(MetaUpdate::AddTable(table_meta));

                // Store version edit.
                let version_meta = &table_meta_data.version_meta;
                let version_edit_meta = VersionEditMeta {
                    space_id: *space_id,
                    table_id: *table_id,
                    flushed_sequence: version_meta.flushed_sequence,
                    files_to_add: version_meta.ordered_files(),
                    files_to_delete: Vec::new(),
                };
                meta_updates.push(MetaUpdate::VersionEdit(version_edit_meta));

                if meta_updates.len() >= STORE_UPDATE_BATCH {
                    self.store_updates_to_region(region_id, &meta_updates)
                        .await?;
                    meta_updates.clear();
                }
            }
        }

        if !meta_updates.is_empty() {
            self.store_updates_to_region(region_id, &meta_updates)
                .await?;
            meta_updates.clear();
        }

        Ok(())
    }

    #[inline]
    fn num_updates_since_snapshot(&self) -> usize {
        self.num_updates_since_snapshot.load(Ordering::Relaxed)
    }

    // Guarded by snapshot state lock.
    #[inline]
    fn decrease_num_updates(&self, num: usize) {
        if num >= self.num_updates_since_snapshot() {
            self.num_updates_since_snapshot.store(0, Ordering::Relaxed);
        } else {
            self.num_updates_since_snapshot
                .fetch_sub(num, Ordering::Relaxed);
        }
    }
}

#[async_trait]
impl<W: WalManager + Send + Sync> Manifest for ManifestImpl<W> {
    type Error = Error;

    async fn store_update(&self, update: MetaUpdate) -> Result<()> {
        self.store_update_to_region(self.manifest_region_id, update)
            .await?;

        let num_updates = self
            .num_updates_since_snapshot
            .fetch_add(1, Ordering::Relaxed);
        if num_updates >= self.opts.snapshot_every_n_updates {
            info!(
                "Enough updates in manifest, trigger snapshot, num_updates:{}",
                num_updates
            );

            self.create_snapshot().await?;
        }

        Ok(())
    }

    async fn load_data(&self, do_snapshot: bool) -> Result<ManifestData> {
        if do_snapshot {
            let manifest_data = self.create_snapshot().await?;

            Ok(manifest_data)
        } else {
            let mut manifest_data = ManifestData::default();

            let last_snapshot_meta = {
                let snapshot_state = self.snapshot_state.lock().await;
                snapshot_state.snapshot_meta
            };
            let mut manifest_start = ReadBoundary::Min;
            // Load from snapshot.
            if let Some(snapshot_meta) = last_snapshot_meta {
                self.load_data_from_region(snapshot_meta.snapshot_region_id, &mut manifest_data)
                    .await?;
                // The sequence after snapshot.
                manifest_start = ReadBoundary::Excluded(snapshot_meta.sequence);
            }

            // Load remaining data from wal.
            self.load_data_from_region_in_range(
                self.manifest_region_id,
                manifest_start,
                ReadBoundary::Max,
                &mut manifest_data,
            )
            .await?;

            Ok(manifest_data)
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{path::PathBuf, sync::Arc};

    use common_types::{column_schema, datum::DatumKind, schema, schema::Schema};
    use common_util::{runtime, runtime::Runtime, tests::init_log_for_test};
    use table_engine::table::TableId;
    use wal::rocks_impl::manager::{Builder as WalBuilder, RocksImpl};

    use super::*;
    use crate::{
        meta::{
            details::{ManifestImpl, Options},
            meta_update::{
                AddSpaceMeta, AddTableMeta, AlterOptionsMeta, AlterSchemaMeta, DropTableMeta,
                MetaUpdate, VersionEditMeta,
            },
            Manifest,
        },
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

    async fn build_manifest(
        dir: impl Into<PathBuf>,
        runtime: Arc<Runtime>,
        opts: Options,
    ) -> ManifestImpl<RocksImpl> {
        let manifest_wal = WalBuilder::with_default_rocksdb_config(dir, runtime.clone())
            .build()
            .unwrap();

        ManifestImpl::open(manifest_wal, opts).await.unwrap()
    }

    async fn assert_expected(
        dir: impl Into<PathBuf>,
        runtime: Arc<Runtime>,
        opts: Options,
        expected: &str,
    ) -> Result<()> {
        let manifest = build_manifest(dir, runtime, opts).await;
        let data = manifest.load_data(false).await?;
        assert_eq!(format!("{:#?}", data), expected);
        Ok(())
    }

    async fn test_manifest_add_space(
        dir: impl Into<PathBuf>,
        runtime: Arc<Runtime>,
        opts: Options,
    ) {
        let space_id = 10;
        let space_name = "test".to_string();

        let manifest = build_manifest(dir, runtime, opts).await;
        let add_space = MetaUpdate::AddSpace(AddSpaceMeta {
            space_id,
            space_name: space_name.clone(),
        });
        manifest.store_update(add_space).await.unwrap();
        let data = manifest.load_data(false).await.unwrap();
        assert_eq!(data.spaces.len(), 1);
        assert_eq!(data.spaces.get(&10).unwrap().space_meta.space_id, space_id);
        assert_eq!(
            data.spaces.get(&10).unwrap().space_meta.space_name,
            space_name
        );
    }

    async fn check_add_table(
        dir: impl Into<PathBuf>,
        runtime: Arc<Runtime>,
        opts: Options,
    ) -> Result<()> {
        let expected = r#"ManifestData {
    spaces: {
        10: SpaceMetaData {
            space_meta: AddSpaceMeta {
                space_id: 10,
                space_name: "test",
            },
            tables: {
                TableId(100, 0, 100): TableMetaData {
                    table_meta: AddTableMeta {
                        space_id: 10,
                        table_id: TableId(100, 0, 100),
                        table_name: "test_table",
                        schema: Schema {
                            num_key_columns: 2,
                            timestamp_index: 1,
                            tsid_index: None,
                            enable_tsid_primary_key: false,
                            column_schemas: ColumnSchemas {
                                columns: [
                                    ColumnSchema {
                                        id: 1,
                                        name: "key1",
                                        data_type: Varbinary,
                                        is_nullable: false,
                                        is_tag: false,
                                        comment: "",
                                    },
                                    ColumnSchema {
                                        id: 2,
                                        name: "key2",
                                        data_type: Timestamp,
                                        is_nullable: false,
                                        is_tag: false,
                                        comment: "",
                                    },
                                    ColumnSchema {
                                        id: 3,
                                        name: "field1",
                                        data_type: Double,
                                        is_nullable: false,
                                        is_tag: false,
                                        comment: "",
                                    },
                                    ColumnSchema {
                                        id: 4,
                                        name: "field2",
                                        data_type: String,
                                        is_nullable: false,
                                        is_tag: false,
                                        comment: "",
                                    },
                                ],
                            },
                            version: 1,
                        },
                        opts: TableOptions {
                            segment_duration: None,
                            update_mode: Overwrite,
                            enable_ttl: true,
                            ttl: ReadableDuration(
                                604800s,
                            ),
                            arena_block_size: 2097152,
                            write_buffer_size: 33554432,
                            compaction_strategy: Default,
                            num_rows_per_row_group: 8192,
                            compression: Zstd,
                        },
                    },
                    version_meta: TableVersionMeta {
                        flushed_sequence: 0,
                        files: {},
                        max_file_id: 0,
                    },
                },
            },
        },
    },
    last_space_id: 10,
}"#;
        assert_expected(dir, runtime, opts, expected).await
    }

    async fn test_manifest_add_table(
        dir: impl Into<PathBuf>,
        runtime: Arc<Runtime>,
        opts: Options,
    ) -> Result<()> {
        let space_id = 10;
        let manifest = build_manifest(dir, runtime, opts).await;

        let table_id = TableId::from(100);
        let table_name = "test_table".to_string();
        let add_table = MetaUpdate::AddTable(AddTableMeta {
            space_id,
            table_id,
            table_name,
            schema: common_types::tests::build_schema(),
            opts: TableOptions::default(),
        });
        manifest.store_update(add_table).await
    }

    async fn check_drop_table(
        dir: impl Into<PathBuf>,
        runtime: Arc<Runtime>,
        opts: Options,
    ) -> Result<()> {
        let expected = r#"ManifestData {
    spaces: {
        10: SpaceMetaData {
            space_meta: AddSpaceMeta {
                space_id: 10,
                space_name: "test",
            },
            tables: {},
        },
    },
    last_space_id: 10,
}"#;
        assert_expected(dir, runtime, opts, expected).await
    }

    async fn test_manifest_drop_table(
        dir: impl Into<PathBuf>,
        runtime: Arc<Runtime>,
        opts: Options,
    ) -> Result<()> {
        let space_id = 10;

        let manifest = build_manifest(dir, runtime, opts).await;

        let table_id = TableId::from(100);
        let table_name = "test_table".to_string();
        let add_table = MetaUpdate::DropTable(DropTableMeta {
            space_id,
            table_id,
            table_name,
        });
        manifest.store_update(add_table).await
    }

    async fn check_version_edit_with_table(
        dir: impl Into<PathBuf>,
        runtime: Arc<Runtime>,
        opts: Options,
    ) -> Result<()> {
        let expected = r#"ManifestData {
    spaces: {
        10: SpaceMetaData {
            space_meta: AddSpaceMeta {
                space_id: 10,
                space_name: "test",
            },
            tables: {
                TableId(100, 0, 100): TableMetaData {
                    table_meta: AddTableMeta {
                        space_id: 10,
                        table_id: TableId(100, 0, 100),
                        table_name: "test_table",
                        schema: Schema {
                            num_key_columns: 2,
                            timestamp_index: 1,
                            tsid_index: None,
                            enable_tsid_primary_key: false,
                            column_schemas: ColumnSchemas {
                                columns: [
                                    ColumnSchema {
                                        id: 1,
                                        name: "key1",
                                        data_type: Varbinary,
                                        is_nullable: false,
                                        is_tag: false,
                                        comment: "",
                                    },
                                    ColumnSchema {
                                        id: 2,
                                        name: "key2",
                                        data_type: Timestamp,
                                        is_nullable: false,
                                        is_tag: false,
                                        comment: "",
                                    },
                                    ColumnSchema {
                                        id: 3,
                                        name: "field1",
                                        data_type: Double,
                                        is_nullable: false,
                                        is_tag: false,
                                        comment: "",
                                    },
                                    ColumnSchema {
                                        id: 4,
                                        name: "field2",
                                        data_type: String,
                                        is_nullable: false,
                                        is_tag: false,
                                        comment: "",
                                    },
                                ],
                            },
                            version: 1,
                        },
                        opts: TableOptions {
                            segment_duration: None,
                            update_mode: Overwrite,
                            enable_ttl: true,
                            ttl: ReadableDuration(
                                604800s,
                            ),
                            arena_block_size: 2097152,
                            write_buffer_size: 33554432,
                            compaction_strategy: Default,
                            num_rows_per_row_group: 8192,
                            compression: Zstd,
                        },
                    },
                    version_meta: TableVersionMeta {
                        flushed_sequence: 3,
                        files: {},
                        max_file_id: 0,
                    },
                },
            },
        },
    },
    last_space_id: 10,
}"#;
        assert_expected(dir, runtime, opts, expected).await
    }

    async fn check_version_edit_no_table(
        dir: impl Into<PathBuf>,
        runtime: Arc<Runtime>,
        opts: Options,
    ) -> Result<()> {
        let expected = r#"ManifestData {
    spaces: {
        10: SpaceMetaData {
            space_meta: AddSpaceMeta {
                space_id: 10,
                space_name: "test",
            },
            tables: {},
        },
    },
    last_space_id: 10,
}"#;
        assert_expected(dir, runtime, opts, expected).await
    }

    async fn test_manifest_version_edit(
        dir: impl Into<PathBuf>,
        runtime: Arc<Runtime>,
        opts: Options,
    ) -> Result<()> {
        let space_id = 10;

        let manifest = build_manifest(dir, runtime, opts).await;

        let table_id = TableId::from(100);
        let version_edit = MetaUpdate::VersionEdit(VersionEditMeta {
            space_id,
            table_id,
            flushed_sequence: 3,
            files_to_add: Vec::new(),
            files_to_delete: Vec::new(),
        });
        manifest.store_update(version_edit).await
    }

    async fn check_alter_schema(
        dir: impl Into<PathBuf>,
        runtime: Arc<Runtime>,
        opts: Options,
    ) -> Result<()> {
        let expected = r#"ManifestData {
    spaces: {
        10: SpaceMetaData {
            space_meta: AddSpaceMeta {
                space_id: 10,
                space_name: "test",
            },
            tables: {
                TableId(100, 0, 100): TableMetaData {
                    table_meta: AddTableMeta {
                        space_id: 10,
                        table_id: TableId(100, 0, 100),
                        table_name: "test_table",
                        schema: Schema {
                            num_key_columns: 2,
                            timestamp_index: 1,
                            tsid_index: None,
                            enable_tsid_primary_key: false,
                            column_schemas: ColumnSchemas {
                                columns: [
                                    ColumnSchema {
                                        id: 1,
                                        name: "key1",
                                        data_type: Varbinary,
                                        is_nullable: false,
                                        is_tag: false,
                                        comment: "",
                                    },
                                    ColumnSchema {
                                        id: 2,
                                        name: "key2",
                                        data_type: Timestamp,
                                        is_nullable: false,
                                        is_tag: false,
                                        comment: "",
                                    },
                                    ColumnSchema {
                                        id: 3,
                                        name: "field1",
                                        data_type: Double,
                                        is_nullable: false,
                                        is_tag: false,
                                        comment: "",
                                    },
                                    ColumnSchema {
                                        id: 4,
                                        name: "field2",
                                        data_type: String,
                                        is_nullable: false,
                                        is_tag: false,
                                        comment: "",
                                    },
                                    ColumnSchema {
                                        id: 5,
                                        name: "field3",
                                        data_type: String,
                                        is_nullable: false,
                                        is_tag: false,
                                        comment: "",
                                    },
                                ],
                            },
                            version: 1,
                        },
                        opts: TableOptions {
                            segment_duration: None,
                            update_mode: Overwrite,
                            enable_ttl: true,
                            ttl: ReadableDuration(
                                604800s,
                            ),
                            arena_block_size: 2097152,
                            write_buffer_size: 33554432,
                            compaction_strategy: Default,
                            num_rows_per_row_group: 8192,
                            compression: Zstd,
                        },
                    },
                    version_meta: TableVersionMeta {
                        flushed_sequence: 3,
                        files: {},
                        max_file_id: 0,
                    },
                },
            },
        },
    },
    last_space_id: 10,
}"#;
        assert_expected(dir, runtime, opts, expected).await
    }

    async fn test_manifest_alter_schema(
        dir: impl Into<PathBuf>,
        runtime: Arc<Runtime>,
        opts: Options,
    ) -> Result<()> {
        let space_id = 10;
        let manifest = build_manifest(dir, runtime, opts).await;

        let table_id = TableId::from(100);
        let alter_schema = MetaUpdate::AlterSchema(AlterSchemaMeta {
            space_id,
            table_id,
            schema: build_altered_schema(&common_types::tests::build_schema()),
            pre_schema_version: 1,
        });
        manifest.store_update(alter_schema).await
    }

    async fn check_alter_options(
        dir: impl Into<PathBuf>,
        runtime: Arc<Runtime>,
        opts: Options,
    ) -> Result<()> {
        let expected = r#"ManifestData {
    spaces: {
        10: SpaceMetaData {
            space_meta: AddSpaceMeta {
                space_id: 10,
                space_name: "test",
            },
            tables: {
                TableId(100, 0, 100): TableMetaData {
                    table_meta: AddTableMeta {
                        space_id: 10,
                        table_id: TableId(100, 0, 100),
                        table_name: "test_table",
                        schema: Schema {
                            num_key_columns: 2,
                            timestamp_index: 1,
                            tsid_index: None,
                            enable_tsid_primary_key: false,
                            column_schemas: ColumnSchemas {
                                columns: [
                                    ColumnSchema {
                                        id: 1,
                                        name: "key1",
                                        data_type: Varbinary,
                                        is_nullable: false,
                                        is_tag: false,
                                        comment: "",
                                    },
                                    ColumnSchema {
                                        id: 2,
                                        name: "key2",
                                        data_type: Timestamp,
                                        is_nullable: false,
                                        is_tag: false,
                                        comment: "",
                                    },
                                    ColumnSchema {
                                        id: 3,
                                        name: "field1",
                                        data_type: Double,
                                        is_nullable: false,
                                        is_tag: false,
                                        comment: "",
                                    },
                                    ColumnSchema {
                                        id: 4,
                                        name: "field2",
                                        data_type: String,
                                        is_nullable: false,
                                        is_tag: false,
                                        comment: "",
                                    },
                                    ColumnSchema {
                                        id: 5,
                                        name: "field3",
                                        data_type: String,
                                        is_nullable: false,
                                        is_tag: false,
                                        comment: "",
                                    },
                                ],
                            },
                            version: 1,
                        },
                        opts: TableOptions {
                            segment_duration: None,
                            update_mode: Overwrite,
                            enable_ttl: false,
                            ttl: ReadableDuration(
                                604800s,
                            ),
                            arena_block_size: 2097152,
                            write_buffer_size: 33554432,
                            compaction_strategy: Default,
                            num_rows_per_row_group: 8192,
                            compression: Zstd,
                        },
                    },
                    version_meta: TableVersionMeta {
                        flushed_sequence: 3,
                        files: {},
                        max_file_id: 0,
                    },
                },
            },
        },
    },
    last_space_id: 10,
}"#;
        assert_expected(dir, runtime, opts, expected).await
    }

    async fn test_manifest_alter_options(
        dir: impl Into<PathBuf>,
        runtime: Arc<Runtime>,
        opts: Options,
    ) -> Result<()> {
        let space_id = 10;

        let manifest = build_manifest(dir, runtime, opts).await;

        let table_id = TableId::from(100);
        let alter_options = MetaUpdate::AlterOptions(AlterOptionsMeta {
            space_id,
            table_id,
            options: TableOptions {
                enable_ttl: false,
                ..Default::default()
            },
        });
        manifest.store_update(alter_options).await
    }

    #[test]
    fn test_manifest() {
        init_log_for_test();
        let dir = tempfile::tempdir().unwrap();
        let runtime = build_runtime(2);
        let runtime_clone = runtime.clone();
        runtime.block_on(async move {
            let opts = Options {
                snapshot_every_n_updates: 2,
                paranoid_checks: false,
            };

            test_manifest_add_space(dir.path(), runtime_clone.clone(), opts.clone()).await;

            test_manifest_add_table(dir.path(), runtime_clone.clone(), opts.clone())
                .await
                .unwrap();
            assert!(
                check_add_table(dir.path(), runtime_clone.clone(), opts.clone())
                    .await
                    .is_ok()
            );

            test_manifest_drop_table(dir.path(), runtime_clone.clone(), opts.clone())
                .await
                .unwrap();
            assert!(
                check_drop_table(dir.path(), runtime_clone.clone(), opts.clone())
                    .await
                    .is_ok()
            );
            {
                let opts = Options {
                    snapshot_every_n_updates: 2,
                    paranoid_checks: true,
                };
                test_manifest_version_edit(dir.path(), runtime_clone.clone(), opts.clone())
                    .await
                    .unwrap();
                assert!(check_version_edit_no_table(
                    dir.path(),
                    runtime_clone.clone(),
                    opts.clone()
                )
                .await
                .is_ok());

                test_manifest_alter_schema(dir.path(), runtime_clone.clone(), opts.clone())
                    .await
                    .unwrap();
                assert!(
                    check_alter_schema(dir.path(), runtime_clone.clone(), opts.clone())
                        .await
                        .is_err()
                );

                test_manifest_alter_options(dir.path(), runtime_clone.clone(), opts.clone())
                    .await
                    .unwrap();
                assert!(check_alter_options(dir.path(), runtime_clone.clone(), opts)
                    .await
                    .is_err());
            }
            {
                test_manifest_add_table(dir.path(), runtime_clone.clone(), opts.clone())
                    .await
                    .unwrap();
                assert!(
                    check_add_table(dir.path(), runtime_clone.clone(), opts.clone())
                        .await
                        .is_ok()
                );

                test_manifest_version_edit(dir.path(), runtime_clone.clone(), opts.clone())
                    .await
                    .unwrap();
                assert!(check_version_edit_with_table(
                    dir.path(),
                    runtime_clone.clone(),
                    opts.clone()
                )
                .await
                .is_ok());

                test_manifest_alter_schema(dir.path(), runtime_clone.clone(), opts.clone())
                    .await
                    .unwrap();
                assert!(
                    check_alter_schema(dir.path(), runtime_clone.clone(), opts.clone())
                        .await
                        .is_ok()
                );

                test_manifest_alter_options(dir.path(), runtime_clone.clone(), opts.clone())
                    .await
                    .unwrap();
                assert!(check_alter_options(dir.path(), runtime_clone, opts)
                    .await
                    .is_ok());
            }
        });
    }
}
