// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

//! Close table logic of instance

use std::sync::Arc;

use log::{info, warn};
use object_store::ObjectStore;
use snafu::ResultExt;
use table_engine::engine::CloseTableRequest;
use tokio::sync::oneshot;
use wal::manager::WalManager;

use crate::{
    instance::{
        engine::{FlushTable, OperateByWriteWorker, Result},
        flush_compaction::TableFlushOptions,
        write_worker::{self, CloseTableCommand, WorkerLocal},
        Instance,
    },
    meta::Manifest,
    space::SpaceRef,
    sst::factory::Factory,
};

impl<Wal, Meta, Store, Fa> Instance<Wal, Meta, Store, Fa>
where
    Wal: WalManager + Send + Sync + 'static,
    Meta: Manifest + Send + Sync + 'static,
    Store: ObjectStore,
    Fa: Factory + Send + Sync + 'static,
{
    /// Close table need to be handled by write worker.
    pub async fn do_close_table(&self, space: SpaceRef, request: CloseTableRequest) -> Result<()> {
        info!("Instance close table, request:{:?}", request);

        let table_data = match space.find_table_by_id(request.table_id) {
            Some(v) => v,
            None => return Ok(()),
        };

        let (tx, rx) = oneshot::channel::<Result<()>>();
        let cmd = CloseTableCommand { space, request, tx };
        write_worker::process_command_in_write_worker(cmd.into_command(), &table_data, rx)
            .await
            .context(OperateByWriteWorker {
                space_id: table_data.space_id,
                table: &table_data.name,
                table_id: table_data.id,
            })
    }

    /// Do the actual close table job, must be called by write worker in write
    /// thread sequentially.
    pub(crate) async fn process_close_table_command(
        self: &Arc<Self>,
        worker_local: &mut WorkerLocal,
        space: SpaceRef,
        request: CloseTableRequest,
    ) -> Result<()> {
        let table_data = match space.find_table_by_id(request.table_id) {
            Some(v) => v,
            None => {
                warn!("try to close a closed table, request:{:?}", request);
                return Ok(());
            }
        };

        let opts = TableFlushOptions {
            block_on_write_thread: true,
            // The table will be dropped, no need to trigger a compaction.
            compact_after_flush: false,
            ..Default::default()
        };
        self.flush_table_in_worker(worker_local, &table_data, opts)
            .await
            .context(FlushTable {
                space_id: space.id,
                table: &table_data.name,
                table_id: table_data.id,
            })?;

        // table has been closed so remove it from the space
        let removed_table = space.remove_table(&request.table_name);
        assert!(removed_table.is_some());

        info!(
            "table:{}-{} has been removed from the space_id:{}",
            table_data.name, table_data.id, space.id
        );
        Ok(())
    }
}
