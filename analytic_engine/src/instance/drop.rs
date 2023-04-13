// Copyright 2022-2023 CeresDB Project Authors. Licensed under Apache-2.0.

//! Drop table logic of instance

use log::{info, warn};
use snafu::ResultExt;
use table_engine::engine::DropTableRequest;

use crate::{
    instance::{
        engine::{FlushTable, Result, WriteManifest},
        flush_compaction::{Flusher, TableFlushOptions},
        SpaceStoreRef,
    },
    manifest::meta_update::{DropTableMeta, MetaUpdate, MetaUpdateRequest},
    space::SpaceRef,
};

pub(crate) struct Dropper {
    pub space: SpaceRef,
    pub space_store: SpaceStoreRef,

    pub flusher: Flusher,
}

impl Dropper {
    /// Drop a table under given space
    pub async fn drop(&self, request: DropTableRequest) -> Result<bool> {
        info!("Try to drop table, request:{:?}", request);

        let table_data = match self.space.find_table(&request.table_name) {
            Some(v) => v,
            None => {
                warn!("No need to drop a dropped table, request:{:?}", request);
                return Ok(false);
            }
        };

        let mut serializer = table_data.serializer.lock().await;

        if table_data.is_dropped() {
            warn!(
                "Process drop table command tries to drop a dropped table, table:{:?}",
                table_data.name,
            );
            return Ok(false);
        }

        // Fixme(xikai): Trigger a force flush so that the data of the table in the wal
        //  is marked for deletable. However, the overhead of the flushing can
        //  be avoided.
        let opts = TableFlushOptions::default();
        let flush_scheduler = serializer.flush_scheduler();
        self.flusher
            .do_flush(flush_scheduler, &table_data, opts)
            .await
            .context(FlushTable {
                space_id: self.space.id,
                table: &table_data.name,
                table_id: table_data.id,
            })?;

        // Store the dropping information into meta
        let update_req = {
            let meta_update = MetaUpdate::DropTable(DropTableMeta {
                space_id: self.space.id,
                table_id: table_data.id,
                table_name: table_data.name.clone(),
            });
            MetaUpdateRequest {
                shard_info: table_data.shard_info,
                meta_update,
            }
        };
        self.space_store
            .manifest
            .store_update(update_req)
            .await
            .context(WriteManifest {
                space_id: self.space.id,
                table: &table_data.name,
                table_id: table_data.id,
            })?;

        // Set the table dropped after finishing flushing and storing drop table meta
        // information.
        table_data.set_dropped();

        // Clear the memory status after updating manifest and clearing wal so that
        // the drop is retryable if fails to update and clear.
        self.space.remove_table(&table_data.name);

        Ok(true)
    }
}
