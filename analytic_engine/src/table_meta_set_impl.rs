// Copyright 2023 CeresDB Project Authors. Licensed under Apache-2.0.

//! Table data set impl based on spaces

use std::{fmt, num::NonZeroUsize, sync::Arc};

use generic_error::BoxError;
use id_allocator::IdAllocator;
use log::debug;
use snafu::{OptionExt, ResultExt};
use table_engine::table::TableId;

use crate::{
    manifest::{
        details::{
            ApplySnapshotToTableWithCause, ApplyUpdateToTableNoCause, ApplyUpdateToTableWithCause,
            BuildSnapshotNoCause, TableMetaSet,
        },
        meta_edit::{
            self, AddTableMeta, AlterOptionsMeta, AlterSchemaMeta, DropTableMeta, MetaEditRequest,
            MetaUpdate, VersionEditMeta,
        },
        meta_snapshot::MetaSnapshot,
    },
    space::{SpaceId, SpaceRef, SpacesRef},
    sst::file::FilePurgerRef,
    table::{
        data::{TableData, TableDataRef, TableShardInfo, DEFAULT_ALLOC_STEP},
        version::{TableVersionMeta, TableVersionSnapshot},
        version_edit::VersionEdit,
    },
};

#[derive(Clone)]
pub(crate) struct TableMetaSetImpl {
    pub(crate) spaces: SpacesRef,
    pub(crate) file_purger: FilePurgerRef,
    // TODO: maybe not suitable to place this parameter here?
    pub(crate) preflush_write_buffer_size_ratio: f32,
    pub(crate) manifest_snapshot_every_n_updates: NonZeroUsize,
}

impl fmt::Debug for TableMetaSetImpl {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("spaces table snapshot provider")
    }
}

enum TableKey<'a> {
    Name(&'a str),
    Id(TableId),
}

impl TableMetaSetImpl {
    fn find_table_and_apply_edit<F>(
        &self,
        space_id: SpaceId,
        table_key: TableKey<'_>,
        apply_edit: F,
    ) -> crate::manifest::details::Result<TableDataRef>
    where
        F: FnOnce(SpaceRef, TableDataRef) -> crate::manifest::details::Result<()>,
    {
        let spaces = self.spaces.read().unwrap();
        let space = spaces
            .get_by_id(space_id)
            .with_context(|| ApplyUpdateToTableNoCause {
                msg: format!("space not found, space_id:{space_id}"),
            })?;

        let table_data = match table_key {
            TableKey::Name(name) => {
                space
                    .find_table(name)
                    .with_context(|| ApplyUpdateToTableNoCause {
                        msg: format!("table not found, space_id:{space_id}, table_name:{name}"),
                    })?
            }
            TableKey::Id(id) => {
                space
                    .find_table_by_id(id)
                    .with_context(|| ApplyUpdateToTableNoCause {
                        msg: format!("table not found, space_id:{space_id}, table_id:{id}"),
                    })?
            }
        };

        apply_edit(space.clone(), table_data.clone())?;

        Ok(table_data)
    }

    fn apply_update(
        &self,
        meta_update: MetaUpdate,
        shard_info: TableShardInfo,
    ) -> crate::manifest::details::Result<TableDataRef> {
        match meta_update {
            MetaUpdate::AddTable(AddTableMeta {
                space_id,
                table_id,
                table_name,
                schema,
                opts,
            }) => {
                let spaces = self.spaces.read().unwrap();
                let space =
                    spaces
                        .get_by_id(space_id)
                        .with_context(|| ApplyUpdateToTableNoCause {
                            msg: format!("space not found, space_id:{space_id}"),
                        })?;

                let table_data = Arc::new(
                    TableData::new(
                        space.id,
                        table_id,
                        table_name,
                        schema,
                        shard_info.shard_id,
                        opts,
                        &self.file_purger,
                        self.preflush_write_buffer_size_ratio,
                        space.mem_usage_collector.clone(),
                        self.manifest_snapshot_every_n_updates,
                    )
                    .box_err()
                    .with_context(|| ApplyUpdateToTableWithCause {
                        msg: format!(
                            "failed to new table data, space_id:{}, table_id:{}",
                            space.id, table_id
                        ),
                    })?,
                );

                space.insert_table(table_data.clone());

                Ok(table_data)
            }
            MetaUpdate::DropTable(DropTableMeta {
                space_id,
                table_name,
                ..
            }) => {
                let table_name = &table_name;
                let drop_table = move |space: SpaceRef, table_data: TableDataRef| {
                    // Set the table dropped after finishing flushing and storing drop table meta
                    // information.
                    table_data.set_dropped();

                    // Clear the memory status after updating manifest and clearing wal so that
                    // the drop is retryable if fails to update and clear.
                    space.remove_table(&table_data.name);

                    Ok(())
                };

                self.find_table_and_apply_edit(space_id, TableKey::Name(table_name), drop_table)
            }
            MetaUpdate::VersionEdit(VersionEditMeta {
                space_id,
                table_id,
                flushed_sequence,
                files_to_add,
                files_to_delete,
                mems_to_remove,
                max_file_id,
            }) => {
                let version_edit = move |_space: SpaceRef, table_data: TableDataRef| {
                    let edit = VersionEdit {
                        flushed_sequence,
                        mems_to_remove,
                        files_to_add,
                        files_to_delete,
                        max_file_id,
                    };
                    table_data.current_version().apply_edit(edit);

                    Ok(())
                };

                self.find_table_and_apply_edit(space_id, TableKey::Id(table_id), version_edit)
            }
            MetaUpdate::AlterSchema(AlterSchemaMeta {
                space_id,
                table_id,
                schema,
                ..
            }) => {
                let alter_schema = move |_space: SpaceRef, table_data: TableDataRef| {
                    table_data.set_schema(schema);

                    Ok(())
                };

                self.find_table_and_apply_edit(space_id, TableKey::Id(table_id), alter_schema)
            }
            MetaUpdate::AlterOptions(AlterOptionsMeta {
                space_id,
                table_id,
                options,
            }) => {
                let alter_option = move |_space: SpaceRef, table_data: TableDataRef| {
                    table_data.set_table_options(options);

                    Ok(())
                };

                self.find_table_and_apply_edit(space_id, TableKey::Id(table_id), alter_option)
            }
        }
    }

    fn apply_snapshot(
        &self,
        meta_snapshot: MetaSnapshot,
        shard_info: TableShardInfo,
    ) -> crate::manifest::details::Result<TableDataRef> {
        debug!("TableMetaSet apply snapshot, snapshot :{:?}", meta_snapshot);

        let MetaSnapshot {
            table_meta,
            version_meta,
        } = meta_snapshot;

        let space_id = table_meta.space_id;
        let spaces = self.spaces.read().unwrap();
        let space = spaces
            .get_by_id(space_id)
            .with_context(|| ApplyUpdateToTableNoCause {
                msg: format!("space not found, space_id:{space_id}"),
            })?;

        // Apply max file id to the allocator
        let allocator = match version_meta.clone() {
            Some(version_meta) => {
                let max_file_id = version_meta.max_file_id_to_add();
                IdAllocator::new(max_file_id, max_file_id, DEFAULT_ALLOC_STEP)
            }
            None => IdAllocator::new(0, 0, DEFAULT_ALLOC_STEP),
        };

        let table_name = table_meta.table_name.clone();
        let table_data = Arc::new(
            TableData::recover_from_add(
                table_meta,
                &self.file_purger,
                shard_info.shard_id,
                self.preflush_write_buffer_size_ratio,
                space.mem_usage_collector.clone(),
                allocator,
                self.manifest_snapshot_every_n_updates,
            )
            .box_err()
            .with_context(|| ApplySnapshotToTableWithCause {
                msg: format!(
                    "failed to new table_data, space_id:{}, table_name:{}",
                    space.id, table_name
                ),
            })?,
        );

        // Apply version meta to the table.
        if let Some(version_meta) = version_meta {
            debug!(
                "TableMetaSet apply version meta, version meta:{:?}",
                version_meta
            );

            table_data.current_version().apply_meta(version_meta);
        }

        debug!(
            "TableMetaSet success to apply snapshot, table_id:{}, table_name:{}",
            table_data.id, table_data.name
        );

        space.insert_table(table_data.clone());

        Ok(table_data)
    }
}

impl TableMetaSet for TableMetaSetImpl {
    fn get_table_snapshot(
        &self,
        space_id: SpaceId,
        table_id: TableId,
    ) -> crate::manifest::details::Result<Option<MetaSnapshot>> {
        let table_data = {
            let spaces = self.spaces.read().unwrap();
            spaces
                .get_by_id(space_id)
                .context(BuildSnapshotNoCause {
                    msg: format!("space not exist, space_id:{space_id}, table_id:{table_id}",),
                })?
                .find_table_by_id(table_id)
                .context(BuildSnapshotNoCause {
                    msg: format!("table data not exist, space_id:{space_id}, table_id:{table_id}",),
                })?
        };

        // When table has been dropped, we should return None.
        let table_manifest_data_opt = if !table_data.is_dropped() {
            let table_meta = AddTableMeta {
                space_id,
                table_id,
                table_name: table_data.name.to_string(),
                schema: table_data.schema(),
                opts: table_data.table_options().as_ref().clone(),
            };

            let version_snapshot = table_data.current_version().snapshot();
            let TableVersionSnapshot {
                flushed_sequence,
                files,
                max_file_id,
            } = version_snapshot;
            let version_meta = TableVersionMeta {
                flushed_sequence,
                files,
                max_file_id,
            };

            Some(MetaSnapshot {
                table_meta,
                version_meta: Some(version_meta),
            })
        } else {
            None
        };

        Ok(table_manifest_data_opt)
    }

    fn apply_edit_to_table(
        &self,
        request: crate::manifest::meta_edit::MetaEditRequest,
    ) -> crate::manifest::details::Result<TableDataRef> {
        let MetaEditRequest {
            shard_info,
            meta_edit,
        } = request;

        match meta_edit {
            meta_edit::MetaEdit::Update(update) => self.apply_update(update, shard_info),
            meta_edit::MetaEdit::Snapshot(manifest_data) => {
                self.apply_snapshot(manifest_data, shard_info)
            }
        }
    }
}
