// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

//! Meta data of manifest.

use std::collections::BTreeMap;

use common_util::define_result;
use log::{debug, info};
use snafu::{ensure, Backtrace, OptionExt, Snafu};
use table_engine::table::TableId;

use crate::{
    meta::meta_update::{AddSpaceMeta, AddTableMeta, MetaUpdate},
    space::SpaceId,
    table::version::TableVersionMeta,
};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display(
        "Space id corrupted (last >= given), last:{}, given:{}.\nBacktrace:\n{}",
        last,
        given,
        backtrace
    ))]
    SpaceIdCorrupted {
        last: SpaceId,
        given: SpaceId,
        backtrace: Backtrace,
    },

    #[snafu(display(
        "Space of table is missing, maybe corrupted, space_id:{}, table:{}.\nBacktrace:\n{}",
        space_id,
        table_name,
        backtrace,
    ))]
    TableSpaceMiss {
        space_id: SpaceId,
        table_name: String,
        backtrace: Backtrace,
    },

    #[snafu(display(
        "Space is missing, maybe corrupted, space_id:{}.\nBacktrace:\n{}",
        space_id,
        backtrace,
    ))]
    SpaceMiss {
        space_id: SpaceId,
        backtrace: Backtrace,
    },

    #[snafu(display(
        "Table is missing, maybe corrupted, space_id:{}, table_id:{}.\nBacktrace:\n{}",
        space_id,
        table_id,
        backtrace,
    ))]
    TableMiss {
        space_id: SpaceId,
        table_id: TableId,
        backtrace: Backtrace,
    },
}

define_result!(Error);

#[derive(Debug)]
pub struct TableMetaData {
    pub table_meta: AddTableMeta,
    pub version_meta: TableVersionMeta,
}

#[derive(Debug)]
pub struct SpaceMetaData {
    pub space_meta: AddSpaceMeta,
    // Use BTreeMap to order table meta by table id.
    pub tables: BTreeMap<TableId, TableMetaData>,
}

/// Holds the final view of the data in manifest.
#[derive(Debug, Default)]
pub struct ManifestData {
    // Use BTreeMap to order space meta by space id, so space with smaller id
    // can be processed first. This is necessary especially in creating snapshot.
    pub spaces: BTreeMap<SpaceId, SpaceMetaData>,
    pub last_space_id: SpaceId,
}

impl ManifestData {
    pub fn apply_meta_update(&mut self, update: MetaUpdate) -> Result<()> {
        debug!("Apply meta update, update:{:?}", update);

        // TODO(yingwen): Ignore space not found error when we support drop space.
        match update {
            MetaUpdate::AddSpace(meta) => {
                ensure!(
                    self.last_space_id <= meta.space_id,
                    SpaceIdCorrupted {
                        last: self.last_space_id,
                        given: meta.space_id,
                    }
                );

                self.last_space_id = meta.space_id;
                self.spaces.insert(
                    meta.space_id,
                    SpaceMetaData {
                        space_meta: meta,
                        tables: BTreeMap::new(),
                    },
                );
            }
            MetaUpdate::AddTable(meta) => {
                let space = self
                    .spaces
                    .get_mut(&meta.space_id)
                    .context(TableSpaceMiss {
                        space_id: meta.space_id,
                        table_name: &meta.table_name,
                    })?;
                space.tables.insert(
                    meta.table_id,
                    TableMetaData {
                        table_meta: meta,
                        version_meta: TableVersionMeta::default(),
                    },
                );
            }
            MetaUpdate::VersionEdit(meta) => {
                let space = self.spaces.get_mut(&meta.space_id).context(SpaceMiss {
                    space_id: meta.space_id,
                })?;
                // If there is a background compaction/flush job, then version edit
                // may be stored after a drop table entry being stored. We ignore
                // that case and won't return error if table is not found.
                let table = match space.tables.get_mut(&meta.table_id) {
                    Some(v) => v,
                    None => {
                        info!("Table of version edit not found, meta:{:?}", meta);

                        return Ok(());
                    }
                };
                let edit = meta.into_version_edit();
                table.version_meta.apply_edit(edit);
            }
            MetaUpdate::AlterSchema(meta) => {
                let space = self.spaces.get_mut(&meta.space_id).context(SpaceMiss {
                    space_id: meta.space_id,
                })?;
                let table = space.tables.get_mut(&meta.table_id).context(TableMiss {
                    space_id: meta.space_id,
                    table_id: meta.table_id,
                })?;

                // Update schema of AddTableMeta.
                table.table_meta.schema = meta.schema;
            }
            MetaUpdate::AlterOptions(meta) => {
                let space = self.spaces.get_mut(&meta.space_id).context(SpaceMiss {
                    space_id: meta.space_id,
                })?;
                let table = space.tables.get_mut(&meta.table_id).context(TableMiss {
                    space_id: meta.space_id,
                    table_id: meta.table_id,
                })?;

                // Update options of AddTableMeta.
                table.table_meta.opts = meta.options;
            }
            MetaUpdate::DropTable(meta) => {
                let space = self.spaces.get_mut(&meta.space_id).context(SpaceMiss {
                    space_id: meta.space_id,
                })?;

                let removed_table = space.tables.remove(&meta.table_id);

                debug!(
                    "Apply drop table meta update, removed table:{}, removed:{}",
                    meta.table_name,
                    removed_table.is_some()
                );
            }
            MetaUpdate::SnapshotManifest(_) => {
                // A snapshot record, no need to handle this.
            }
        }

        Ok(())
    }
}
