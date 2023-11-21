// Copyright 2023 The CeresDB Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Close table logic of instance

use logger::{info, warn};
use snafu::ResultExt;
use table_engine::engine::CloseTableRequest;

use crate::{
    instance::{
        engine::{DoManifestSnapshot, FlushTable, Result},
        flush_compaction::{Flusher, TableFlushOptions},
    },
    manifest::{ManifestRef, SnapshotRequest},
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
        let snapshot_request = SnapshotRequest {
            space_id: self.space.id,
            table_id: table_data.id,
            shard_id: table_data.shard_info.shard_id,
            table_catalog_info: table_data.table_catalog_info.clone(),
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

        // Table is already moved out of space, we should close it to stop background
        // jobs.
        table_data.set_closed();

        info!(
            "table:{}-{} has been removed from the space_id:{}",
            table_data.name, table_data.id, self.space.id
        );
        Ok(())
    }
}
