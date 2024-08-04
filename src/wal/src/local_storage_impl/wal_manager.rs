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
    sync::Arc,
};

use async_trait::async_trait;
use common_types::{table::TableId, SequenceNumber, MIN_SEQUENCE_NUMBER};
use logger::{debug, info};
use rocksdb::{DBIterator, ReadOptions};
use snafu::ResultExt;
use generic_error::BoxError;
use runtime::Runtime;

use crate::{
    kv_encoder::{CommonLogEncoding, CommonLogKey},
    log_batch::LogWriteBatch,
    manager::{
        error::*, BatchLogIteratorAdapter, ReadContext, ReadRequest, RegionId, ScanContext, ScanRequest,
        WalLocation, WalManager, WriteContext,
    },
    rocksdb_impl::manager::{RocksImpl, RocksLogIterator},
};
use crate::local_storage_impl::config::LocalStorageConfig;
use crate::local_storage_impl::segment::SegmentManager;
use crate::log_batch::LogEntry;
use crate::manager::SyncLogIterator;

pub struct LocalStorageImpl {
    config: LocalStorageConfig,
    runtime: Arc<Runtime>,
    segment_manager: SegmentManager,
}

impl LocalStorageImpl {
    pub fn new(config: LocalStorageConfig, runtime: Arc<Runtime>) -> Self {
        Self {
            config: config.clone(),
            runtime: runtime.clone(),
            segment_manager: SegmentManager::new(config.cache_size, config.path, runtime).unwrap(), // TODO: remove unwrap
        }
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
        self.segment_manager.sequence_num(location).box_err().context(Read)
    }

    async fn mark_delete_entries_up_to(
        &self,
        location: WalLocation,
        sequence_num: SequenceNumber,
    ) -> Result<()> {
        debug!("Mark delete entries up to for LocalStorage based WAL is noop operation, location:{:?}, sequence_num:{}", location, sequence_num);
        Ok(())
    }

    async fn close_region(&self, region_id: RegionId) -> Result<()> {
        debug!(
            "Close region for LocalStorage based WAL is noop operation, region_id:{}",
            region_id
        );

        Ok(())
    }

    async fn close_gracefully(&self) -> Result<()> {
        info!("Close local storage wal gracefully");
        Ok(())
    }

    async fn read_batch(
        &self,
        ctx: &ReadContext,
        req: &ReadRequest,
    ) -> Result<BatchLogIteratorAdapter> {
        self.segment_manager
            .read(ctx, req)
            .await
            .box_err()
            .context(Read)
    }

    async fn write(
        &self,
        ctx: &WriteContext,
        batch: &LogWriteBatch,
    ) -> Result<SequenceNumber> {
        self.segment_manager.write(ctx, batch).box_err().context(Write)
    }

    async fn scan(
        &self,
        ctx: &ScanContext,
        req: &ScanRequest,
    ) -> Result<BatchLogIteratorAdapter> {
        self.segment_manager.scan(ctx, req).box_err().context(Read)
    }

    async fn get_statistics(&self) -> Option<String> {
        None
    }
}


struct LocalStorageLogIterator {
    log_encoding: CommonLogEncoding,
    no_more_data: bool,
    min_log_key: CommonLogKey,
    max_log_key: CommonLogKey,
    seeked: bool,
}

impl Debug for LocalStorageLogIterator {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        todo!()
    }
}

impl SyncLogIterator for LocalStorageLogIterator {
    fn next_log_entry(&mut self) -> Result<Option<LogEntry<&'_ [u8]>>> {
        todo!()
    }
}
