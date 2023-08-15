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

//! Wal based on message queue

use std::sync::Arc;

use async_trait::async_trait;
use common_types::SequenceNumber;
use generic_error::BoxError;
use message_queue::{ConsumeIterator, MessageQueue};
use runtime::Runtime;
use snafu::ResultExt;

use crate::{
    log_batch::{LogEntry, LogWriteBatch},
    manager::{
        error::*, AsyncLogIterator, BatchLogIteratorAdapter, ReadContext, ReadRequest, RegionId,
        ScanContext, ScanRequest, WalLocation, WalManager, WriteContext,
    },
    message_queue_impl::{
        config::Config,
        namespace::{Namespace, ReadTableIterator, ScanRegionIterator},
    },
};

#[derive(Debug)]
pub struct MessageQueueImpl<M: MessageQueue>(Namespace<M>);

impl<M: MessageQueue> MessageQueueImpl<M> {
    pub fn new(
        namespace: String,
        message_queue: M,
        default_runtime: Arc<Runtime>,
        config: Config,
    ) -> Self {
        MessageQueueImpl(Namespace::open(
            namespace,
            Arc::new(message_queue),
            default_runtime,
            config,
        ))
    }
}

#[async_trait]
impl<M: MessageQueue> WalManager for MessageQueueImpl<M> {
    async fn sequence_num(&self, location: WalLocation) -> Result<SequenceNumber> {
        self.0.sequence_num(location).await.box_err().context(Read)
    }

    async fn mark_delete_entries_up_to(
        &self,
        location: WalLocation,
        sequence_num: SequenceNumber,
    ) -> Result<()> {
        self.0
            .mark_delete_to(location, sequence_num + 1)
            .await
            .box_err()
            .context(Delete)
    }

    async fn close_region(&self, region_id: RegionId) -> Result<()> {
        self.0
            .close_region(region_id)
            .await
            .box_err()
            .context(Close)
    }

    async fn close_gracefully(&self) -> Result<()> {
        self.0.close().await.box_err().context(Close)
    }

    async fn read_batch(
        &self,
        ctx: &ReadContext,
        req: &ReadRequest,
    ) -> Result<BatchLogIteratorAdapter> {
        let iter = self.0.read(ctx, req).await.box_err().context(Read)?;
        Ok(BatchLogIteratorAdapter::new_with_async(
            Box::new(iter),
            ctx.batch_size,
        ))
    }

    async fn scan(&self, ctx: &ScanContext, req: &ScanRequest) -> Result<BatchLogIteratorAdapter> {
        let iter = self.0.scan(ctx, req).await.box_err().context(Read)?;
        Ok(BatchLogIteratorAdapter::new_with_async(
            Box::new(iter),
            ctx.batch_size,
        ))
    }

    async fn write(&self, ctx: &WriteContext, batch: &LogWriteBatch) -> Result<SequenceNumber> {
        self.0.write(ctx, batch).await.box_err().context(Write)
    }

    async fn get_statistics(&self) -> Option<String> {
        let wal_stats = self.0.get_statistics().await;
        let stats = format!("#MessageQueueWal stats:\n{wal_stats}\n");

        Some(stats)
    }
}

#[async_trait]
impl<C: ConsumeIterator> AsyncLogIterator for ScanRegionIterator<C> {
    async fn next_log_entry(&mut self) -> Result<Option<LogEntry<&'_ [u8]>>> {
        self.next_log_entry().await.box_err().context(Read)
    }
}

#[async_trait]
impl<C: ConsumeIterator> AsyncLogIterator for ReadTableIterator<C> {
    async fn next_log_entry(&mut self) -> Result<Option<LogEntry<&'_ [u8]>>> {
        self.next_log_entry().await.box_err().context(Read)
    }
}
