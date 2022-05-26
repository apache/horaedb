// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

//! Table engine logic of instance

use std::sync::Arc;

use common_util::define_result;
use log::info;
use object_store::ObjectStore;
use snafu::{ResultExt, Snafu};
use table_engine::engine::{CreateTableRequest, DropTableRequest};
use wal::manager::WalManager;

use crate::{
    context::CommonContext,
    instance::{write_worker::WriteGroup, Instance},
    meta::{
        meta_update::{AddSpaceMeta, MetaUpdate},
        Manifest,
    },
    space::{Space, SpaceAndTable, SpaceNameRef, SpaceRef},
    sst::factory::Factory,
    table_options,
};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Space failed to create table, err:{}", source))]
    SpaceCreateTable { source: crate::space::Error },

    #[snafu(display("Failed to drop table, err:{}", source))]
    DoDropTable {
        source: crate::instance::drop::Error,
    },

    #[snafu(display("Failed to store meta of space, space:{}, err:{}", space, source))]
    SpaceWriteMeta {
        space: String,
        source: Box<dyn std::error::Error + Send + Sync>,
    },
    #[snafu(display("Invalid options, table:{}, err:{}", table, source))]
    InvalidOptions {
        table: String,
        source: Box<dyn std::error::Error + Send + Sync>,
    },
}

define_result!(Error);

impl From<Error> for table_engine::engine::Error {
    fn from(err: Error) -> Self {
        match err {
            Error::SpaceCreateTable { source } => Self::from(source),

            // FIXME(xikai): should map drop table error to a more reasonable table engine error.
            Error::DoDropTable { .. } => Self::Unexpected {
                source: Box::new(err),
            },

            Error::SpaceWriteMeta { .. } => Self::WriteMeta {
                source: Box::new(err),
            },

            Error::InvalidOptions { ref table, .. } => Self::InvalidArguments {
                table: table.clone(),
                source: Box::new(err),
            },
        }
    }
}

impl<
        Wal: WalManager + Send + Sync + 'static,
        Meta: Manifest + Send + Sync + 'static,
        Store: ObjectStore,
        Fa: Factory + Send + Sync + 'static,
    > Instance<Wal, Meta, Store, Fa>
{
    /// Find space by name, create if the space is not exists
    pub async fn find_or_create_space(
        self: &Arc<Self>,
        _ctx: &CommonContext,
        space_name: SpaceNameRef<'_>,
    ) -> Result<SpaceRef> {
        // Find space first
        if let Some(space) = self.get_space_by_read_lock(space_name) {
            return Ok(space);
        }

        // Persist space data into meta, done with `meta_state` guarded
        let mut meta_state = self.space_store.meta_state.lock().await;
        // The space may already been created by other thread
        if let Some(space) = self.get_space_by_read_lock(space_name) {
            return Ok(space);
        }
        // Now we are the one responsible to create and persist the space info into meta

        let space_id = meta_state.alloc_space_id();
        // Create write group for the space
        // TODO(yingwen): Expose options
        let write_group_opts = self.write_group_options(space_id);
        let write_group = WriteGroup::new(write_group_opts, self.clone());

        // Create space
        let space = Arc::new(Space::new(
            space_id,
            space_name.to_string(),
            self.space_write_buffer_size,
            write_group,
            self.mem_usage_collector.clone(),
        ));

        // Create a meta update and store it
        let update = MetaUpdate::AddSpace(AddSpaceMeta {
            space_id,
            space_name: space_name.to_string(),
        });
        info!("Instance create space, update:{:?}", update);
        self.space_store
            .manifest
            .store_update(update)
            .await
            .map_err(|e| Box::new(e) as _)
            .context(SpaceWriteMeta { space: space_name })?;

        let mut spaces = self.space_store.spaces.write().unwrap();
        spaces.insert(space_name.to_string(), space.clone());
        // Now we can release the meta state lock

        Ok(space)
    }

    /// Find space by name
    pub fn find_space(
        &self,
        _ctx: &CommonContext,
        space: SpaceNameRef,
    ) -> Result<Option<SpaceRef>> {
        let spaces = self.space_store.spaces.read().unwrap();
        Ok(spaces.get_by_name(space).cloned())
    }

    /// Create a table under given space
    pub async fn create_table(
        self: &Arc<Self>,
        ctx: &CommonContext,
        space: SpaceNameRef<'_>,
        request: CreateTableRequest,
    ) -> Result<SpaceAndTable> {
        let mut table_opts =
            table_options::merge_table_options_for_create(&request.options, &self.table_opts)
                .map_err(|e| Box::new(e) as _)
                .context(InvalidOptions {
                    table: &request.table_name,
                })?;
        // Sanitize options before creating table.
        table_opts.sanitize();

        info!(
            "Instance create table, space:{}, request:{:?}, table_opts:{:?}",
            space, request, table_opts
        );

        let space = self.find_or_create_space(ctx, space).await?;

        let table_data = space
            .create_table(
                request,
                &self.space_store.manifest,
                &table_opts,
                &self.file_purger,
            )
            .await
            .context(SpaceCreateTable)?;

        Ok(SpaceAndTable::new(space, table_data))
    }

    /// Drop a table under given space
    pub async fn drop_table(
        self: &Arc<Self>,
        ctx: &CommonContext,
        space: SpaceNameRef<'_>,
        request: DropTableRequest,
    ) -> Result<bool> {
        info!(
            "Instance drop table, space:{}, request:{:?}",
            space, request
        );

        let space = match self.find_space(ctx, space)? {
            Some(v) => v,
            None => return Ok(false),
        };

        // Checks whether the table is exists
        let table = match space.find_table(&request.table_name) {
            Some(v) => v,
            None => return Ok(false),
        };

        let space_table = SpaceAndTable::new(space.clone(), table);
        self.do_drop_table(space_table, request)
            .await
            .context(DoDropTable)?;

        Ok(true)
    }

    /// Find the table under given space by its table name
    ///
    /// Return None if space or table is not found
    pub fn find_table(
        &self,
        ctx: &CommonContext,
        space: SpaceNameRef,
        table: &str,
    ) -> Result<Option<SpaceAndTable>> {
        let space = match self.find_space(ctx, space)? {
            Some(s) => s,
            None => return Ok(None),
        };

        let space_table = space
            .find_table(table)
            .map(|table_data| SpaceAndTable::new(space, table_data));

        Ok(space_table)
    }
}
