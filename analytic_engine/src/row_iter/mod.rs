// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

//! Iterators for row.

use async_trait::async_trait;
use common_types::{record_batch::RecordBatchWithKey, schema::RecordSchemaWithKey};
use common_util::error::BoxError;

use crate::sst::writer::RecordBatchStream;

pub mod chain;
pub mod dedup;
pub mod merge;
pub mod record_batch_stream;
#[cfg(test)]
pub mod tests;

#[derive(Debug, Clone)]
pub struct IterOptions {
    pub batch_size: usize,
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

pub fn record_batch_with_key_iter_to_stream<I: RecordBatchWithKeyIterator + Unpin + 'static>(
    iter: I,
) -> RecordBatchStream {
    let record_batch_stream = futures::stream::unfold(iter, |mut iter| async {
        let item = iter.next_batch().await.box_err().transpose();
        item.map(|item| (item, iter))
    });
    Box::new(Box::pin(record_batch_stream))
}
