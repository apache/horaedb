// Copyright 2022-2023 CeresDB Project Authors. Licensed under Apache-2.0.

//! Close table logic of instance

use log::{info, warn};
use snafu::ResultExt;
use table_engine::engine::CloseTableRequest;

use crate::{
    instance::{
        engine::{DoManifestSnapshot, FlushTable, Result},
        flush_compaction::{Flusher, TableFlushOptions},
    },
    manifest::{ManifestRef, DoSnapshotRequest},
    space::SpaceRef,
};

pub(crate) struct Closer {
    pub space: SpaceRef,
    pub manifest: ManifestRef,

    pub flusher: Flusher,
}

impl Closer {
    /// Close table need to be handled by write worker.
    pub async fn close(&self, request: CloseTableRequest) -> Result<()> {
        info!("Try to close table, request:{:?}", request);

        let table_data = match self.space.find_table_by_id(request.table_id) {
            Some(v) => v,
            None => {
                warn!("try to close a closed table, request:{:?}", request);
                return Ok(());
            }
        };

        // Flush table.
        let opts = TableFlushOptions::default();
        let mut serial_exec = table_data.serial_exec.lock().await;
        let flush_scheduler = serial_exec.flush_scheduler();

        self.flusher
            .do_flush(flush_scheduler, &table_data, opts)
            .await
            .context(FlushTable {
                space_id: self.space.id,
                table: &table_data.name,
                table_id: table_data.id,
            })?;

        // Force manifest to do snapshot.
        let snapshot_request = DoSnapshotRequest {
            space_id: self.space.id,
            table_id: table_data.id,
            shard_id: table_data.shard_info.shard_id,
        };
        self.manifest
            .do_snapshot(snapshot_request)
            .await
            .context(DoManifestSnapshot {
                space_id: self.space.id,
                table: &table_data.name,
            })?;

        // Table has been closed so remove it from the space.
        let removed_table = self.space.remove_table(&request.table_name);
        assert!(removed_table.is_some());

        info!(
            "table:{}-{} has been removed from the space_id:{}",
            table_data.name, table_data.id, self.space.id
        );
        Ok(())
    }
}
