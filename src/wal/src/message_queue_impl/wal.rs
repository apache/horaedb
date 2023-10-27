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
use message_queue::{kafka::kafka_impl::KafkaImpl, ConsumeIterator, MessageQueue};
use runtime::Runtime;
use snafu::ResultExt;

use crate::{
    config::StorageConfig,
    log_batch::{LogEntry, LogWriteBatch},
    manager::{
        self, error::*, AsyncLogIterator, BatchLogIteratorAdapter, OpenedWals, ReadContext,
        ReadRequest, RegionId, ScanContext, ScanRequest, WalLocation, WalManager, WalRuntimes,
        WalsOpener, WriteContext, MANIFEST_DIR_NAME, WAL_DIR_NAME,
    },
    message_queue_impl::{
        config::KafkaWalConfig,
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
        config: KafkaWalConfig,
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
        manager::collect_write_log_metrics(batch);
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

#[derive(Default)]
pub struct KafkaWalsOpener;

#[async_trait]
impl WalsOpener for KafkaWalsOpener {
    async fn open_wals(&self, config: &StorageConfig, runtimes: WalRuntimes) -> Result<OpenedWals> {
        let kafka_wal_config = match config {
            StorageConfig::Kafka(config) => config.clone(),
            _ => {
                return InvalidWalConfig {
                    msg: format!(
                        "invalid wal storage config while opening kafka wal, config:{config:?}"
                    ),
                }
                .fail();
            }
        };

        let default_runtime = &runtimes.default_runtime;

        let kafka = KafkaImpl::new(kafka_wal_config.kafka.clone())
            .await
            .context(OpenKafka)?;
        let data_wal = MessageQueueImpl::new(
            WAL_DIR_NAME.to_string(),
            kafka.clone(),
            default_runtime.clone(),
            kafka_wal_config.data_namespace,
        );

        let manifest_wal = MessageQueueImpl::new(
            MANIFEST_DIR_NAME.to_string(),
            kafka,
            default_runtime.clone(),
            kafka_wal_config.meta_namespace,
        );

        Ok(OpenedWals {
            data_wal: Arc::new(data_wal),
            manifest_wal: Arc::new(manifest_wal),
        })
    }
}
