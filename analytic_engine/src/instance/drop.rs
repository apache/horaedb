// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

//! Drop table logic of instance

use std::sync::Arc;

use log::{info, warn};
use snafu::ResultExt;
use table_engine::engine::DropTableRequest;
use tokio::sync::oneshot;

use crate::{
    instance::{
        engine::{FlushTable, OperateByWriteWorker, Result, WriteManifest},
        flush_compaction::TableFlushOptions,
        write_worker::{self, DropTableCommand, WorkerLocal},
        Instance,
    },
    meta::meta_update::{DropTableMeta, MetaUpdate, MetaUpdateRequest},
    space::SpaceRef,
};

impl Instance {
    /// Drop a table under given space
    pub async fn do_drop_table(
        self: &Arc<Self>,
        space: SpaceRef,
        request: DropTableRequest,
    ) -> Result<bool> {
        info!("Instance drop table begin, request:{:?}", request);

        let table_data = match space.find_table(&request.table_name) {
            Some(v) => v,
            None => {
                warn!("No need to drop a dropped table, request:{:?}", request);
                return Ok(false);
            }
        };

        // Create a oneshot channel to send/receive alter schema result.
        let (tx, rx) = oneshot::channel::<Result<bool>>();
        let cmd = DropTableCommand { space, request, tx };

        write_worker::process_command_in_write_worker(cmd.into_command(), &table_data, rx)
            .await
            .context(OperateByWriteWorker {
                space_id: table_data.space_id,
                table: &table_data.name,
                table_id: table_data.id,
            })
    }

    /// Do the actual drop table job, must be called by write worker in write
    /// thread sequentially.
    pub(crate) async fn process_drop_table_command(
        self: &Arc<Self>,
        worker_local: &mut WorkerLocal,
        space: SpaceRef,
        request: DropTableRequest,
    ) -> Result<bool> {
        let table_data = match space.find_table(&request.table_name) {
            Some(v) => v,
            None => {
                warn!("Try to drop a dropped table, request:{:?}", request);
                return Ok(false);
            }
        };

        if table_data.is_dropped() {
            warn!(
                "Process drop table command tries to drop a dropped table, table:{:?}",
                table_data.name,
            );
            return Ok(false);
        }

        worker_local
            .ensure_permission(
                &table_data.name,
                table_data.id.as_u64() as usize,
                self.write_group_worker_num,
            )
            .context(OperateByWriteWorker {
                space_id: table_data.space_id,
                table: &table_data.name,
                table_id: table_data.id,
            })?;

        // Fixme(xikai): Trigger a force flush so that the data of the table in the wal
        //  is marked for deletable. However, the overhead of the flushing can
        //  be avoided.
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

        // Store the dropping information into meta
        let update = MetaUpdate::DropTable(DropTableMeta {
            space_id: space.id,
            table_id: table_data.id,
            table_name: table_data.name.clone(),
        });
        self.space_store
            .manifest
            .store_update(MetaUpdateRequest::new(table_data.location(), update))
            .await
            .context(WriteManifest {
                space_id: space.id,
                table: &table_data.name,
                table_id: table_data.id,
            })?;

        // Set the table dropped after finishing flushing and storing drop table meta
        // information.
        table_data.set_dropped();

        // Clear the memory status after updating manifest and clearing wal so that
        // the drop is retryable if fails to update and clear.
        space.remove_table(&table_data.name);

        Ok(true)
    }
}
