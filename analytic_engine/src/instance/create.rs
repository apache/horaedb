// Copyright 2022-2023 CeresDB Project Authors. Licensed under Apache-2.0.

//! Create table logic of instance

use common_util::error::BoxError;
use log::info;
use snafu::{OptionExt, ResultExt};
use table_engine::engine::CreateTableRequest;

use crate::{
    instance::{
        engine::{CreateOpenFailedTable, InvalidOptions, Result, TableNotExist, WriteManifest},
        Instance,
    },
    manifest::meta_edit::{AddTableMeta, MetaEdit, MetaEditRequest, MetaUpdate},
    space::SpaceRef,
    table::data::{TableDataRef, TableShardInfo},
    table_options,
};

impl Instance {
    /// Create table need to be handled by write worker.
    pub async fn do_create_table(
        &self,
        space: SpaceRef,
        request: CreateTableRequest,
    ) -> Result<TableDataRef> {
        info!("Instance create table, request:{:?}", request);

        if space.is_open_failed_table(&request.table_name) {
            return CreateOpenFailedTable {
                table: request.table_name,
            }
            .fail();
        }

        let mut table_opts =
            table_options::merge_table_options_for_create(&request.options, &self.table_opts)
                .box_err()
                .context(InvalidOptions {
                    space_id: space.id,
                    table: &request.table_name,
                    table_id: request.table_id,
                })?;
        // Sanitize options before creating table.
        table_opts.sanitize();

        if let Some(table_data) = space.find_table_by_id(request.table_id) {
            return Ok(table_data);
        }

        // Store table info into meta both memory and storage.
        let edit_req = {
            let meta_update = MetaUpdate::AddTable(AddTableMeta {
                space_id: space.id,
                table_id: request.table_id,
                table_name: request.table_name.clone(),
                schema: request.table_schema,
                opts: table_opts,
            });
            MetaEditRequest {
                shard_info: TableShardInfo::new(request.shard_id),
                meta_edit: MetaEdit::Update(meta_update),
            }
        };
        self.space_store
            .manifest
            .apply_edit(edit_req)
            .await
            .with_context(|| WriteManifest {
                space_id: space.id,
                table: request.table_name.clone(),
                table_id: request.table_id,
            })?;

        // Table is sure to exist here.
        space
            .find_table_by_id(request.table_id)
            .with_context(|| TableNotExist {
                msg: format!(
                    "table not exist, space_id:{}, table_id:{}, table_name:{}",
                    space.id, request.table_id, request.table_name
                ),
            })
    }
}
