// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

//! Alter schema logic of instance

use std::{collections::HashMap, sync::Arc};

use common_types::schema::Version;
use common_util::define_result;
use log::info;
use object_store::ObjectStore;
use snafu::{ensure, Backtrace, ResultExt, Snafu};
use table_engine::table::AlterSchemaRequest;
use tokio::sync::oneshot;
use wal::manager::WalManager;

use crate::{
    instance::{
        flush_compaction::TableFlushOptions,
        write_worker,
        write_worker::{AlterOptionsCommand, AlterSchemaCommand, WorkerLocal},
        Instance,
    },
    meta::{
        meta_update::{AlterOptionsMeta, AlterSchemaMeta, MetaUpdate},
        Manifest,
    },
    space::SpaceAndTable,
    sst::factory::Factory,
    table::data::TableDataRef,
    table_options,
};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Failed to alter schema, source:{}", source,))]
    AlterSchema { source: write_worker::Error },

    #[snafu(display("Failed to alter options, source:{}", source,))]
    AlterOptions { source: write_worker::Error },

    #[snafu(display(
        "Try to update schema to elder version, table:{}, current_version:{}, given_version:{}.\nBacktrace:\n{}",
        table,
        current_version,
        given_version,
        backtrace,
    ))]
    InvalidSchemaVersion {
        table: String,
        current_version: Version,
        given_version: Version,
        backtrace: Backtrace,
    },

    #[snafu(display(
        "Invalid previous schema version, table:{}, current_version:{}, pre_version:{}.\nBacktrace:\n{}",
        table,
        current_version,
        pre_version,
        backtrace,
    ))]
    InvalidPreVersion {
        table: String,
        current_version: Version,
        pre_version: Version,
        backtrace: Backtrace,
    },

    #[snafu(display("Alter schema of a dropped table:{}", table))]
    AlterDroppedTable { table: String },

    #[snafu(display("Failed to flush table, table:{}, err:{}", table, source))]
    FlushTable {
        table: String,
        source: crate::instance::flush_compaction::Error,
    },

    #[snafu(display("Failed to persist alter update, err:{}", source))]
    PersistAlter {
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display("Invalid options, table:{}, err:{}", table, source))]
    InvalidOptions {
        table: String,
        source: Box<dyn std::error::Error + Send + Sync>,
    },
}

define_result!(Error);

impl<
        Wal: WalManager + Send + Sync + 'static,
        Meta: Manifest + Send + Sync + 'static,
        Store: ObjectStore,
        Fa: Factory + Send + Sync + 'static,
    > Instance<Wal, Meta, Store, Fa>
{
    // Alter schema need to be handled by write worker.
    pub async fn alter_schema_of_table(
        &self,
        space_table: &SpaceAndTable,
        request: AlterSchemaRequest,
    ) -> Result<()> {
        info!(
            "Instance alter schema, space_table:{:?}, request:{:?}",
            space_table, request
        );

        // Create a oneshot channel to send/receive alter schema result.
        let (tx, rx) = oneshot::channel();
        let cmd = AlterSchemaCommand {
            space_table: space_table.clone(),
            request,
            tx,
        };

        // Send alter schema request to write worker, actual works done in
        // Self::process_alter_schema_command()
        write_worker::process_command_in_write_worker(
            cmd.into_command(),
            space_table.table_data(),
            rx,
        )
        .await
        .context(AlterSchema)
    }

    /// Do the actual alter schema job, must called by write worker in write
    /// thread sequentially.
    pub(crate) async fn process_alter_schema_command(
        self: &Arc<Self>,
        worker_local: &mut WorkerLocal,
        space_table: &SpaceAndTable,
        request: AlterSchemaRequest,
    ) -> Result<()> {
        let table_data = space_table.table_data();
        // Validate alter schema request.
        self.validate_before_alter(table_data, &request)?;

        let opts = TableFlushOptions {
            block_on_write_thread: true,
            ..Default::default()
        };
        // We are in write thread now and there is no write request being processed, but
        // we need to trigger a flush to ensure all wal entries with old schema
        // are flushed, so we won't need to handle them during replaying wal.
        self.flush_table_in_worker(worker_local, table_data, opts)
            .await
            .context(FlushTable {
                table: &table_data.name,
            })?;

        // Now we can persist and update the schema, since this function is called by
        // write worker, so there is no other concurrent writer altering the
        // schema.
        let meta_update = MetaUpdate::AlterSchema(AlterSchemaMeta {
            space_id: space_table.space().id,
            table_id: table_data.id,
            schema: request.schema.clone(),
            pre_schema_version: request.pre_schema_version,
        });
        self.space_store
            .manifest
            .store_update(meta_update)
            .await
            .map_err(|e| Box::new(e) as _)
            .context(PersistAlter)?;

        info!(
            "Instance update table schema, new_schema:{:?}",
            request.schema
        );

        // Update schema in memory.
        table_data.set_schema(request.schema);

        Ok(())
    }

    // Most validation should be done by catalog module, so we don't do too much
    // duplicate check here, especially the schema compatibility.
    fn validate_before_alter(
        &self,
        table_data: &TableDataRef,
        request: &AlterSchemaRequest,
    ) -> Result<()> {
        ensure!(
            !table_data.is_dropped(),
            AlterDroppedTable {
                table: &table_data.name,
            }
        );

        let current_version = table_data.schema_version();
        ensure!(
            current_version < request.schema.version(),
            InvalidSchemaVersion {
                table: &table_data.name,
                current_version,
                given_version: request.schema.version(),
            }
        );

        ensure!(
            current_version == request.pre_schema_version,
            InvalidPreVersion {
                table: &table_data.name,
                current_version,
                pre_version: request.pre_schema_version,
            }
        );

        Ok(())
    }

    pub async fn alter_options_of_table(
        &self,
        space_table: &SpaceAndTable,
        options: HashMap<String, String>,
    ) -> Result<()> {
        info!(
            "Instance alter options of table, space_table:{:?}, options:{:?}",
            space_table, options
        );

        // Create a oneshot channel to send/receive alter options result.
        let (tx, rx) = oneshot::channel();
        let cmd = AlterOptionsCommand {
            space_table: space_table.clone(),
            options,
            tx,
        };

        // Send alter options request to write worker, actual works done in
        // Self::process_alter_options_command()
        write_worker::process_command_in_write_worker(
            cmd.into_command(),
            space_table.table_data(),
            rx,
        )
        .await
        .context(AlterOptions)
    }

    /// Do the actual alter options job, must called by write worker in write
    /// thread sequentially.
    pub(crate) async fn process_alter_options_command(
        self: &Arc<Self>,
        worker_local: &mut WorkerLocal,
        space_table: &SpaceAndTable,
        options: HashMap<String, String>,
    ) -> Result<()> {
        let table_data = space_table.table_data();
        let current_table_options = table_data.table_options();
        info!(
            "Instance alter options, space:{:?}, tables:{:?}, old_table_opts:{:?}, options:{:?}",
            space_table.space().name,
            space_table.table_data().name,
            current_table_options,
            options
        );
        let mut table_opts =
            table_options::merge_table_options_for_alter(&options, &*current_table_options)
                .map_err(|e| Box::new(e) as _)
                .context(InvalidOptions {
                    table: &table_data.name,
                })?;
        table_opts.sanitize();

        // Now we can persist and update the options, since this function is called by
        // write worker, so there is no other concurrent writer altering the
        // options.
        let meta_update = MetaUpdate::AlterOptions(AlterOptionsMeta {
            space_id: space_table.space().id,
            table_id: table_data.id,
            options: table_opts.clone(),
        });
        self.space_store
            .manifest
            .store_update(meta_update)
            .await
            .map_err(|e| Box::new(e) as _)
            .context(PersistAlter)?;

        table_data.set_table_options(worker_local, table_opts);
        Ok(())
    }
}
