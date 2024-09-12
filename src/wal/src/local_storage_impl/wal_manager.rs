// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use std::{
    fmt,
    fmt::{Debug, Formatter},
    path::{Path, PathBuf},
    sync::Arc,
};

use async_trait::async_trait;
use common_types::SequenceNumber;
use generic_error::BoxError;
use logger::{debug, info};
use runtime::Runtime;
use snafu::ResultExt;

use crate::{
    config::{Config, StorageConfig},
    local_storage_impl::{config::LocalStorageConfig, segment::RegionManager},
    log_batch::LogWriteBatch,
    manager::{
        error::*, BatchLogIteratorAdapter, Open, OpenedWals, ReadContext, ReadRequest, RegionId,
        ScanContext, ScanRequest, WalLocation, WalManager, WalManagerRef, WalRuntimes, WalsOpener,
        WriteContext, MANIFEST_DIR_NAME, WAL_DIR_NAME,
    },
};

pub struct LocalStorageImpl {
    config: LocalStorageConfig,
    _runtime: Arc<Runtime>,
    region_manager: RegionManager,
}

impl LocalStorageImpl {
    pub fn new(
        wal_path: PathBuf,
        config: LocalStorageConfig,
        runtime: Arc<Runtime>,
    ) -> Result<Self> {
        let LocalStorageConfig {
            cache_size,
            segment_size,
            ..
        } = config.clone();
        let wal_path_str = wal_path.to_str().unwrap().to_string();
        let region_manager = RegionManager::new(
            wal_path_str.clone(),
            cache_size,
            segment_size,
            runtime.clone(),
        )
        .box_err()
        .context(Open {
            wal_path: wal_path_str,
        })?;
        Ok(Self {
            config,
            _runtime: runtime,
            region_manager,
        })
    }
}

impl Drop for LocalStorageImpl {
    fn drop(&mut self) {
        info!("LocalStorage dropped, config:{:?}", self.config);
    }
}

impl Debug for LocalStorageImpl {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("LocalStorageImpl")
            .field("config", &self.config)
            .finish()
    }
}

#[async_trait]
impl WalManager for LocalStorageImpl {
    async fn sequence_num(&self, location: WalLocation) -> Result<u64> {
        self.region_manager
            .sequence_num(location)
            .box_err()
            .context(Read)
    }

    async fn mark_delete_entries_up_to(
        &self,
        location: WalLocation,
        sequence_num: SequenceNumber,
    ) -> Result<()> {
        debug!(
            "Mark delete entries up to {} for location:{:?}",
            sequence_num, location
        );
        self.region_manager
            .mark_delete_entries_up_to(location, sequence_num)
            .box_err()
            .context(Delete)
    }

    async fn close_region(&self, region_id: RegionId) -> Result<()> {
        debug!("Close region {} for LocalStorage based WAL", region_id);
        self.region_manager
            .close(region_id)
            .box_err()
            .context(Close)?;
        Ok(())
    }

    async fn close_gracefully(&self) -> Result<()> {
        info!("Close local storage wal gracefully");
        self.region_manager.close_all().box_err().context(Close)?;
        Ok(())
    }

    async fn read_batch(
        &self,
        ctx: &ReadContext,
        req: &ReadRequest,
    ) -> Result<BatchLogIteratorAdapter> {
        debug!(
            "Read batch from LocalStorage based WAL, ctx:{:?}, req:{:?}",
            ctx, req
        );
        self.region_manager.read(ctx, req).box_err().context(Read)
    }

    async fn write(&self, ctx: &WriteContext, batch: &LogWriteBatch) -> Result<SequenceNumber> {
        debug!("Write batch to LocalStorage based WAL, ctx:{:?}", ctx);
        self.region_manager
            .write(ctx, batch)
            .box_err()
            .context(Write)
    }

    async fn scan(&self, ctx: &ScanContext, req: &ScanRequest) -> Result<BatchLogIteratorAdapter> {
        debug!(
            "Scan from LocalStorage based WAL, ctx:{:?}, req:{:?}",
            ctx, req
        );
        self.region_manager.scan(ctx, req).box_err().context(Read)
    }

    async fn get_statistics(&self) -> Option<String> {
        None
    }
}

#[derive(Default)]
pub struct LocalStorageWalsOpener;

impl LocalStorageWalsOpener {
    fn build_manager(
        wal_path: PathBuf,
        runtime: Arc<Runtime>,
        config: LocalStorageConfig,
    ) -> Result<WalManagerRef> {
        Ok(Arc::new(LocalStorageImpl::new(wal_path, config, runtime)?))
    }
}

#[async_trait]
impl WalsOpener for LocalStorageWalsOpener {
    async fn open_wals(&self, config: &Config, runtimes: WalRuntimes) -> Result<OpenedWals> {
        let local_storage_wal_config = match &config.storage {
            StorageConfig::Local(config) => config.clone(),
            _ => {
                return InvalidWalConfig {
                    msg: format!(
                        "invalid wal storage config while opening local storage wal, config:{config:?}"
                    ),
                }
                    .fail();
            }
        };

        let write_runtime = runtimes.write_runtime.clone();
        let data_path = Path::new(&local_storage_wal_config.data_dir);

        let data_wal = if config.disable_data {
            Arc::new(crate::dummy::DoNothing)
        } else {
            Self::build_manager(
                data_path.join(WAL_DIR_NAME),
                write_runtime.clone(),
                *local_storage_wal_config.clone(),
            )?
        };

        let manifest_wal = Self::build_manager(
            data_path.join(MANIFEST_DIR_NAME),
            write_runtime.clone(),
            *local_storage_wal_config.clone(),
        )?;

        Ok(OpenedWals {
            data_wal,
            manifest_wal,
        })
    }
}
