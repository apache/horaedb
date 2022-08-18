// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

//! Alter [Schema] and [TableOptions] logic of instance.

use std::{collections::HashMap, sync::Arc};

use log::info;
use snafu::{ensure, ResultExt};
use table_engine::table::AlterSchemaRequest;
use tokio::sync::oneshot;
use wal::manager::WriteContext;

use crate::{
    instance::{
        engine::{
            AlterDroppedTable, EncodePayloads, FlushTable, GetLogBatchEncoder, InvalidOptions,
            InvalidPreVersion, InvalidSchemaVersion, OperateByWriteWorker, Result, WriteManifest,
            WriteWal,
        },
        flush_compaction::TableFlushOptions,
        write_worker,
        write_worker::{AlterOptionsCommand, AlterSchemaCommand, WorkerLocal},
        Instance,
    },
    meta::meta_update::{AlterOptionsMeta, AlterSchemaMeta, MetaUpdate},
    payload::WritePayload,
    space::SpaceAndTable,
    table::data::TableDataRef,
    table_options,
};

/// Policy of how to perform flush operation.
#[derive(Default, Debug, Clone, Copy)]
pub enum TableAlterSchemaPolicy {
    /// Unknown flush policy, this is the default value.
    #[default]
    Unknown,
    /// Perform Alter Schema operation.
    Alter,
    // TODO: use this policy and remove "allow(dead_code)"
    /// Ignore this operation.
    #[allow(dead_code)]
    Noop,
}

impl Instance {
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
            // space_table: space_table.clone(),
            table_data: space_table.table_data().clone(),
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
        .context(OperateByWriteWorker {
            space_id: space_table.space().id,
            table: &space_table.table_data().name,
            table_id: space_table.table_data().id,
        })
    }

    /// Do the actual alter schema job, must called by write worker in write
    /// thread sequentially.
    pub(crate) async fn process_alter_schema_command(
        self: &Arc<Self>,
        worker_local: &mut WorkerLocal,
        table_data: &TableDataRef,
        request: AlterSchemaRequest,
        #[allow(unused_variables)] policy: TableAlterSchemaPolicy,
    ) -> Result<()> {
        // let table_data = space_table.table_data();
        // Validate alter schema request.
        self.validate_before_alter(table_data, &request)?;

        // Now we can persist and update the schema, since this function is called by
        // write worker, so there is no other concurrent writer altering the
        // schema.

        // First trigger a flush before alter schema, to ensure ensure all wal entries
        // with old schema are flushed
        let opts = TableFlushOptions {
            block_on_write_thread: true,
            ..Default::default()
        };
        self.flush_table_in_worker(worker_local, table_data, opts)
            .await
            .context(FlushTable {
                space_id: table_data.space_id,
                table: &table_data.name,
                table_id: table_data.id,
            })?;

        // Build alter op
        let manifest_update = AlterSchemaMeta {
            space_id: table_data.space_id,
            table_id: table_data.id,
            schema: request.schema.clone(),
            pre_schema_version: request.pre_schema_version,
        };

        // Write AlterSchema to Data Wal
        let alter_schema_pb = manifest_update.clone().into_pb();
        let payload = WritePayload::AlterSchema(&alter_schema_pb);

        // Encode payloads
        let region_id = table_data.wal_region_id();
        let log_batch_encoder =
            self.space_store
                .wal_manager
                .encoder(region_id)
                .context(GetLogBatchEncoder {
                    table: &table_data.name,
                    region_id: table_data.wal_region_id(),
                })?;

        let log_batch = log_batch_encoder
            .encode(&[payload])
            .context(EncodePayloads {
                table: &table_data.name,
                region_id: table_data.wal_region_id(),
            })?;

        // Write log batch
        let write_ctx = WriteContext::default();
        self.space_store
            .wal_manager
            .write(&write_ctx, &log_batch)
            .await
            .map_err(|e| Box::new(e) as _)
            .context(WriteWal {
                space_id: table_data.space_id,
                table: &table_data.name,
                table_id: table_data.id,
            })?;

        info!(
            "Instance update table schema, new_schema:{:?}",
            request.schema
        );

        // Write to Manifest
        let update = MetaUpdate::AlterSchema(manifest_update);
        self.space_store
            .manifest
            .store_update(update)
            .await
            .context(WriteManifest {
                space_id: table_data.space_id,
                table: &table_data.name,
                table_id: table_data.id,
            })?;

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
        // todo: encapsulate this into a struct like other functions.
        options: HashMap<String, String>,
    ) -> Result<()> {
        info!(
            "Instance alter options of table, space_table:{:?}, options:{:?}",
            space_table, options
        );

        // Create a oneshot channel to send/receive alter options result.
        let (tx, rx) = oneshot::channel();
        let cmd = AlterOptionsCommand {
            table_data: space_table.table_data().clone(),
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
        .context(OperateByWriteWorker {
            space_id: space_table.space().id,
            table: &space_table.table_data().name,
            table_id: space_table.table_data().id,
        })
    }

    /// Do the actual alter options job, must called by write worker in write
    /// thread sequentially.
    pub(crate) async fn process_alter_options_command(
        self: &Arc<Self>,
        worker_local: &mut WorkerLocal,
        table_data: &TableDataRef,
        options: HashMap<String, String>,
    ) -> Result<()> {
        ensure!(
            !table_data.is_dropped(),
            AlterDroppedTable {
                table: &table_data.name,
            }
        );

        // AlterOptions doesn't need a flush.

        // Generate options after alter op
        let current_table_options = table_data.table_options();
        info!(
            "Instance alter options, space_id:{}, tables:{:?}, old_table_opts:{:?}, options:{:?}",
            table_data.space_id, table_data.name, current_table_options, options
        );
        let mut table_opts =
            table_options::merge_table_options_for_alter(&options, &current_table_options)
                .map_err(|e| Box::new(e) as _)
                .context(InvalidOptions {
                    space_id: table_data.space_id,
                    table: &table_data.name,
                    table_id: table_data.id,
                })?;
        table_opts.sanitize();
        let manifest_update = AlterOptionsMeta {
            space_id: table_data.space_id,
            table_id: table_data.id,
            options: table_opts.clone(),
        };

        // Now we can persist and update the options, since this function is called by
        // write worker, so there is no other concurrent writer altering the
        // options.

        // Write AlterOptions to Data Wal
        let alter_options_pb = manifest_update.clone().into_pb();
        let payload = WritePayload::AlterOption(&alter_options_pb);

        // Encode payload
        let region_id = table_data.wal_region_id();
        let log_batch_encoder =
            self.space_store
                .wal_manager
                .encoder(region_id)
                .context(GetLogBatchEncoder {
                    table: &table_data.name,
                    region_id: table_data.wal_region_id(),
                })?;

        let log_batch = log_batch_encoder
            .encode(&[payload])
            .context(EncodePayloads {
                table: &table_data.name,
                region_id: table_data.wal_region_id(),
            })?;

        // Write log batch
        let write_ctx = WriteContext::default();
        self.space_store
            .wal_manager
            .write(&write_ctx, &log_batch)
            .await
            .map_err(|e| Box::new(e) as _)
            .context(WriteWal {
                space_id: table_data.space_id,
                table: &table_data.name,
                table_id: table_data.id,
            })?;

        // Write to Manifest
        let meta_update = MetaUpdate::AlterOptions(manifest_update);
        self.space_store
            .manifest
            .store_update(meta_update)
            .await
            .context(WriteManifest {
                space_id: table_data.space_id,
                table: &table_data.name,
                table_id: table_data.id,
            })?;

        // Update memory status
        table_data.set_table_options(worker_local, table_opts);
        Ok(())
    }
}
