// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

//! Iterators for row.

use std::{
    pin::Pin,
    task::{Context, Poll},
};

use async_trait::async_trait;
use common_types::{record_batch::RecordBatchWithKey, schema::RecordSchemaWithKey};
use common_util::runtime::Runtime;
use futures::stream::Stream;
use log::{debug, error};
use tokio::sync::mpsc::{self, Receiver};

use crate::sst::builder::{RecordBatchStream, RecordBatchStreamItem};

pub mod chain;
pub mod dedup;
pub mod merge;
pub mod record_batch_stream;
#[cfg(test)]
pub mod tests;

const RECORD_BATCH_READ_BUF_SIZE: usize = 10;

#[derive(Debug, Clone)]
pub struct IterOptions {
    pub batch_size: usize,
    pub sst_background_read_parallelism: usize,
}

impl IterOptions {
    pub fn new(batch_size: usize, sst_background_read_parallelism: usize) -> Self {
        Self {
            batch_size,
            sst_background_read_parallelism,
        }
    }
}

impl Default for IterOptions {
    fn default() -> Self {
        Self::new(500, 1)
    }
}

/// The iterator for reading RecordBatch from a table.
///
/// The `schema()` should be the same as the RecordBatch from `read()`.
/// The reader is exhausted if the `read()` returns the `Ok(None)`.
#[async_trait]
pub trait RecordBatchWithKeyIterator: Send {
    type Error: std::error::Error + Send + Sync + 'static;

    fn schema(&self) -> &RecordSchemaWithKey;

    async fn next_batch(&mut self) -> std::result::Result<Option<RecordBatchWithKey>, Self::Error>;
}

struct ReceiverStream {
    rx: Receiver<RecordBatchStreamItem>,
}

impl Stream for ReceiverStream {
    type Item = RecordBatchStreamItem;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        Pin::new(&mut this.rx).poll_recv(cx)
    }
}

// TODO(yingwen): This is a hack way to convert an async trait to stream.
pub fn record_batch_with_key_iter_to_stream<I: RecordBatchWithKeyIterator + 'static>(
    mut iter: I,
    runtime: &Runtime,
) -> RecordBatchStream {
    let (tx, rx) = mpsc::channel(RECORD_BATCH_READ_BUF_SIZE);
    runtime.spawn(async move {
        while let Some(record_batch) = iter.next_batch().await.transpose() {
            let record_batch = record_batch.map_err(|e| Box::new(e) as _);

            debug!(
                "compact table send next record batch, batch:{:?}",
                record_batch
            );
            if tx.send(record_batch).await.is_err() {
                error!("Failed to send record batch from the merge iterator");
                break;
            }
        }
    });

    Box::new(ReceiverStream { rx })
}
