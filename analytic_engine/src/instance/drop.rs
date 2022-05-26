// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

//! Drop table logic of instance

use std::sync::Arc;

use common_util::define_result;
use log::{info, warn};
use object_store::ObjectStore;
use snafu::{ResultExt, Snafu};
use table_engine::engine::DropTableRequest;
use tokio::sync::oneshot;
use wal::manager::WalManager;

use crate::{
    instance::{
        flush_compaction::TableFlushOptions,
        write_worker::{self, DropTableCommand, WorkerLocal},
        Instance,
    },
    meta::{
        meta_update::{DropTableMeta, MetaUpdate},
        Manifest,
    },
    space::SpaceAndTable,
    sst::factory::Factory,
};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display(
        "Failed to drop table space:{}, table:{}, err:{}",
        space,
        table,
        source,
    ))]
    DropTable {
        space: String,
        table: String,
        source: write_worker::Error,
    },

    #[snafu(display("Flush before drop failed, table:{}, err:{}", table, source))]
    FlushTable {
        table: String,
        source: crate::instance::flush_compaction::Error,
    },

    #[snafu(display("Failed to persist drop table update, err:{}", source))]
    PersistDrop {
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
    /// Drop table need to be handled by write worker.
    pub async fn do_drop_table(
        &self,
        space_table: SpaceAndTable,
        request: DropTableRequest,
    ) -> Result<()> {
        info!(
            "Instance drop table, space_table:{:?}, request:{:?}",
            space_table, request
        );

        // Create a oneshot channel to send/receive alter schema result.
        let (tx, rx) = oneshot::channel();
        let cmd = DropTableCommand {
            space_table: space_table.clone(),
            request,
            tx,
        };

        write_worker::process_command_in_write_worker(
            cmd.into_command(),
            space_table.table_data(),
            rx,
        )
        .await
        .context(DropTable {
            space: &space_table.space().name,
            table: &space_table.table_data().name,
        })?;

        Ok(())
    }

    /// Do the actual drop table job, must be called by write worker in write
    /// thread sequentially.
    pub(crate) async fn process_drop_table_command(
        self: &Arc<Self>,
        worker_local: &mut WorkerLocal,
        space_table: &SpaceAndTable,
        _request: DropTableRequest,
    ) -> Result<()> {
        let table_data = space_table.table_data();
        if table_data.is_dropped() {
            warn!(
                "Process drop table command tries to drop a dropped table, space_table:{:?}",
                space_table
            );
            return Ok(());
        }

        // Fixme(xikai): Trigger a force flush so that the data of the table in the wal
        //  is marked for deletable. However, the overhead of the flushing can
        //  be avoided.
        let opts = TableFlushOptions {
            block_on_write_thread: true,
            // The table will be dropped, no need to trigger a compaction.
            compact_after_flush: false,
            ..Default::default()
        };
        self.flush_table_in_worker(worker_local, table_data, opts)
            .await
            .context(FlushTable {
                table: &table_data.name,
            })?;

        // Store the dropping information into meta
        let update = MetaUpdate::DropTable(DropTableMeta {
            space_id: space_table.space().id,
            table_id: table_data.id,
            table_name: table_data.name.clone(),
        });
        self.space_store
            .manifest
            .store_update(update)
            .await
            .map_err(|e| Box::new(e) as _)
            .context(PersistDrop)?;

        // Set the table dropped after finishing flushing and storing drop table meta
        // information.
        table_data.set_dropped();

        // Clear the memory status after updating manifest and clearing wal so that
        // the drop is retryable if fails to update and clear.
        space_table.space().remove_table(&table_data.name);

        Ok(())
    }
}
