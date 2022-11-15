// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

//! Wal based on message queue

use std::sync::Arc;

use async_trait::async_trait;
use common_types::{table::Location, SequenceNumber};
use common_util::runtime::Runtime;
use message_queue::{ConsumeIterator, MessageQueue};
use snafu::ResultExt;

use super::{
    config::Config,
    namespace::{Namespace, ReadTableIterator, ScanRegionIterator},
};
use crate::{
    log_batch::{LogEntry, LogWriteBatch},
    manager::{
        error::*, AsyncLogIterator, BatchLogIteratorAdapter, ReadContext, ReadRequest, ScanContext,
        ScanRequest, WalManager, WriteContext,
    },
};

#[derive(Debug)]
pub struct MessageQueueImpl<M: MessageQueue>(Namespace<M>);

impl<M: MessageQueue> MessageQueueImpl<M> {
    pub fn new(
        namespace: String,
        message_queue: M,
        bg_runtime: Arc<Runtime>,
        config: Config,
    ) -> Self {
        MessageQueueImpl(Namespace::open(
            namespace,
            Arc::new(message_queue),
            bg_runtime,
            config,
        ))
    }
}

#[async_trait]
impl<M: MessageQueue> WalManager for MessageQueueImpl<M> {
    async fn sequence_num(&self, location: Location) -> Result<SequenceNumber> {
        self.0
            .sequence_num(location)
            .await
            .map_err(|e| Box::new(e) as _)
            .context(Read)
    }

    async fn mark_delete_entries_up_to(
        &self,
        location: Location,
        sequence_num: SequenceNumber,
    ) -> Result<()> {
        self.0
            .mark_delete_to(location, sequence_num + 1)
            .await
            .map_err(|e| Box::new(e) as _)
            .context(Delete)
    }

    async fn close_gracefully(&self) -> Result<()> {
        self.0
            .close()
            .await
            .map_err(|e| Box::new(e) as _)
            .context(Close)
    }

    async fn read_batch(
        &self,
        ctx: &ReadContext,
        req: &ReadRequest,
    ) -> Result<BatchLogIteratorAdapter> {
        let iter = self
            .0
            .read(ctx, req)
            .await
            .map_err(|e| Box::new(e) as _)
            .context(Read)?;
        Ok(BatchLogIteratorAdapter::new_with_async(
            Box::new(iter),
            ctx.batch_size,
        ))
    }

    async fn scan(&self, ctx: &ScanContext, req: &ScanRequest) -> Result<BatchLogIteratorAdapter> {
        let iter = self
            .0
            .scan(ctx, req)
            .await
            .map_err(|e| Box::new(e) as _)
            .context(Read)?;
        Ok(BatchLogIteratorAdapter::new_with_async(
            Box::new(iter),
            ctx.batch_size,
        ))
    }

    async fn write(&self, ctx: &WriteContext, batch: &LogWriteBatch) -> Result<SequenceNumber> {
        self.0
            .write(ctx, batch)
            .await
            .map_err(|e| Box::new(e) as _)
            .context(Write)
    }
}

#[async_trait]
impl<C: ConsumeIterator> AsyncLogIterator for ScanRegionIterator<C> {
    async fn next_log_entry(&mut self) -> Result<Option<LogEntry<&'_ [u8]>>> {
        self.next_log_entry()
            .await
            .map_err(|e| Box::new(e) as _)
            .context(Read)
    }
}

#[async_trait]
impl<C: ConsumeIterator> AsyncLogIterator for ReadTableIterator<C> {
    async fn next_log_entry(&mut self) -> Result<Option<LogEntry<&'_ [u8]>>> {
        self.next_log_entry()
            .await
            .map_err(|e| Box::new(e) as _)
            .context(Read)
    }
}
