// Copyright 2023 CeresDB Project Authors. Licensed under Apache-2.0.

//! Table data set impl based on spaces

use std::{fmt, sync::Arc};

use common_util::error::BoxError;
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
            self, AddTableMeta, AlterOptionsMeta, AlterSchemaMeta, AlterSstIdMeta, DropTableMeta,
            MetaEditRequest, MetaUpdate, VersionEditMeta,
        },
        meta_snapshot::MetaSnapshot,
    },
    space::{Space, SpaceId, SpacesRef},
    sst::file::FilePurgerRef,
    table::{
        data::{TableData, TableShardInfo},
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
}

impl fmt::Debug for TableMetaSetImpl {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("spaces table snapshot provider")
    }
}

impl TableMetaSetImpl {
    fn find_space_and_apply_edit<F>(
        &self,
        space_id: SpaceId,
        apply_edit: F,
    ) -> crate::manifest::details::Result<()>
    where
        F: FnOnce(Arc<Space>) -> crate::manifest::details::Result<()>,
    {
        let spaces = self.spaces.read().unwrap();
        let space = spaces
            .get_by_id(space_id)
            .with_context(|| ApplyUpdateToTableNoCause {
                msg: format!("space not found, space_id:{space_id}"),
            })?;
        apply_edit(space.clone())
    }

    fn apply_update(
        &self,
        meta_update: MetaUpdate,
        shard_info: TableShardInfo,
    ) -> crate::manifest::details::Result<()> {
        match meta_update {
            MetaUpdate::AddTable(AddTableMeta {
                space_id,
                table_id,
                table_name,
                schema,
                opts,
            }) => {
                let add_table = move |space: Arc<Space>| {
                    let table_data = TableData::new(
                        space.id,
                        table_id,
                        table_name,
                        schema,
                        shard_info.shard_id,
                        opts,
                        &self.file_purger,
                        self.preflush_write_buffer_size_ratio,
                        space.mem_usage_collector.clone(),
                    )
                    .box_err()
                    .with_context(|| ApplyUpdateToTableWithCause {
                        msg: format!(
                            "failed to new table data, space_id:{}, table_id:{}",
                            space.id, table_id
                        ),
                    })?;
                    space.insert_table(Arc::new(table_data));
                    Ok(())
                };

                self.find_space_and_apply_edit(space_id, add_table)
            }
            MetaUpdate::DropTable(DropTableMeta {
                space_id,
                table_name,
                ..
            }) => {
                let drop_table = move |space: Arc<Space>| {
                    let table_data = match space.find_table(table_name.as_str()) {
                        Some(v) => v,
                        None => return Ok(()),
                    };

                    // Set the table dropped after finishing flushing and storing drop table meta
                    // information.
                    table_data.set_dropped();

                    // Clear the memory status after updating manifest and clearing wal so that
                    // the drop is retryable if fails to update and clear.
                    space.remove_table(&table_data.name);

                    Ok(())
                };

                self.find_space_and_apply_edit(space_id, drop_table)
            }
            MetaUpdate::VersionEdit(VersionEditMeta {
                space_id,
                table_id,
                flushed_sequence,
                files_to_add,
                files_to_delete,
                mems_to_remove,
            }) => {
                let version_edit = move |space: Arc<Space>| {
                    let table_data = space.find_table_by_id(table_id).with_context(|| {
                        ApplyUpdateToTableNoCause {
                            msg: format!(
                                "table not found, space_id:{space_id}, table_id:{table_id}"
                            ),
                        }
                    })?;
                    let edit = VersionEdit {
                        flushed_sequence,
                        mems_to_remove,
                        files_to_add,
                        files_to_delete,
                    };
                    table_data.current_version().apply_edit(edit);

                    Ok(())
                };

                self.find_space_and_apply_edit(space_id, version_edit)
            }
            MetaUpdate::AlterSchema(AlterSchemaMeta {
                space_id,
                table_id,
                schema,
                ..
            }) => {
                let alter_schema = move |space: Arc<Space>| {
                    let table_data = space.find_table_by_id(table_id).with_context(|| {
                        ApplyUpdateToTableNoCause {
                            msg: format!(
                                "table not found, space_id:{space_id}, table_id:{table_id}"
                            ),
                        }
                    })?;
                    table_data.set_schema(schema);

                    Ok(())
                };
                self.find_space_and_apply_edit(space_id, alter_schema)
            }
            MetaUpdate::AlterOptions(AlterOptionsMeta {
                space_id,
                table_id,
                options,
            }) => {
                let alter_option = move |space: Arc<Space>| {
                    let table_data = space.find_table_by_id(table_id).with_context(|| {
                        ApplyUpdateToTableNoCause {
                            msg: format!(
                                "table not found, space_id:{space_id}, table_id:{table_id}"
                            ),
                        }
                    })?;
                    table_data.set_table_options(options);

                    Ok(())
                };
                self.find_space_and_apply_edit(space_id, alter_option)
            }
            MetaUpdate::AlterSstId(AlterSstIdMeta {
                space_id,
                table_id,
                last_file_id,
                max_file_id,
            }) => {
                let alter_sst_id = move |space: Arc<Space>| {
                    let table_data = space.find_table_by_id(table_id).with_context(|| {
                        ApplyUpdateToTableNoCause {
                            msg: format!(
                                "table not found, space_id:{space_id}, table_id:{table_id}"
                            ),
                        }
                    })?;
                    table_data.set_last_file_id(last_file_id);
                    table_data.set_max_file_id(max_file_id);
                    Ok(())
                };
                self.find_space_and_apply_edit(space_id, alter_sst_id)
            }
        }
    }

    fn apply_snapshot(
        &self,
        meta_snapshot: MetaSnapshot,
        shard_info: TableShardInfo,
    ) -> crate::manifest::details::Result<()> {
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

        let table_name = table_meta.table_name.clone();
        let table_data = Arc::new(
            TableData::recover_from_add(
                table_meta,
                &self.file_purger,
                shard_info.shard_id,
                self.preflush_write_buffer_size_ratio,
                space.mem_usage_collector.clone(),
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

            let max_file_id = version_meta.max_file_id_to_add();
            table_data.current_version().apply_meta(version_meta);
            // In recovery case, we need to maintain last file id of the table manually.
            if table_data.last_file_id() < max_file_id {
                table_data.set_last_file_id(max_file_id);
            }
        }

        debug!(
            "TableMetaSet success to apply snapshot, table_id:{}, table_name:{}",
            table_data.id, table_data.name
        );

        space.insert_table(table_data);

        Ok(())
    }
}

impl TableMetaSet for TableMetaSetImpl {
    fn get_table_snapshot(
        &self,
        space_id: SpaceId,
        table_id: TableId,
    ) -> crate::manifest::details::Result<Option<MetaSnapshot>> {
        let spaces = self.spaces.read().unwrap();
        let table_data = spaces
            .get_by_id(space_id)
            .context(BuildSnapshotNoCause {
                msg: format!("space not exist, space_id:{space_id}, table_id:{table_id}",),
            })?
            .find_table_by_id(table_id)
            .context(BuildSnapshotNoCause {
                msg: format!("table data not exist, space_id:{space_id}, table_id:{table_id}",),
            })?;

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
            } = version_snapshot;
            let version_meta = TableVersionMeta {
                flushed_sequence,
                files,
                max_file_id: table_data.max_file_id(),
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
    ) -> crate::manifest::details::Result<()> {
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
