// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

//! Log splitter

use std::{
    collections::{BTreeMap, HashSet, VecDeque},
    time::Duration,
};

use common_types::{
    bytes::MemBuf,
    table::{ShardId, TableId},
};
use log::{debug, info};
use snafu::ResultExt;

use super::LogSenderRef;
use crate::{
    log_batch::PayloadDecoder,
    manager::BatchLogIteratorAdapter,
    shard_based_impl::{
        config::Config,
        read_buffer::{error::*, LogMessage},
    },
};

/// It will split the logs of a shard, according to table id,
/// and push them to corresponding table channel.
// TODO: Following exception should be taken in consideration:
// + read log from wal_manager timeout(not considered right now).
// + send message timeout(now return error directly)
// + wait for the receiver to pull all messages timeout(not considered right now),
///  maybe we can use watch channel to kill such fetchers?
pub struct Splitter {
    shard_id: ShardId,
    log_iter: BatchLogIteratorAdapter,
    senders: BTreeMap<TableId, LogSenderRef>,
    context: SplitContext,
}

impl Splitter {
    pub fn new(
        shard_id: ShardId,
        log_iter: BatchLogIteratorAdapter,
        senders: BTreeMap<TableId, LogSenderRef>,
        context: SplitContext,
    ) -> Self {
        Self {
            shard_id,
            log_iter,
            senders,
            context,
        }
    }

    pub async fn start(&mut self) -> Result<()> {
        info!("start to read and split wals on shard:{}, read logs from wal manager firstly, context:{:?}", self.shard_id, self.context);
        // Pull logs firstly, then split adn push them to table channel by table id.
        let mut buffer = VecDeque::with_capacity(self.context.read_batch_size);
        loop {
            buffer = self
                .log_iter
                .next_log_entries(CloneDecoder, buffer)
                .await
                .context(SplitRead {
                    shard_id: self.shard_id,
                })?;

            for log_entry in &mut buffer {
                let raw_payload = std::mem::take(&mut log_entry.payload);
                if let Some(sender) = self.senders.get(&log_entry.location.table_id).cloned() {
                    // TODO: if tables failed to recover, I think we should collect them
                    // and notify high-level rather than just return an error.
                    sender
                        .send_timeout(
                            LogMessage::Content(raw_payload),
                            Duration::from_millis(self.context.max_per_table_wait_ms),
                        )
                        .await
                        .map_err(|e| Box::new(e) as _)
                        .context(SplitSend {
                            shard_id: self.shard_id,
                            table_id: log_entry.location.table_id,
                        })?;
                }
            }

            if buffer.is_empty() {
                break;
            }
        }

        // All logs has been pulled, now just wait until all table finished to get its
        // logs.
        info!(
            "start to wait all tables on shard:{} to pull their logs",
            self.shard_id
        );

        let mut removed = HashSet::with_capacity(self.senders.len());
        let mut pre_removed = HashSet::with_capacity(self.senders.len());
        loop {
            let mut has_new_removed = false;
            for (table_id, sender) in self.senders.iter() {
                // It means sender of the table has been removed.
                if removed.contains(table_id) {
                    continue;
                }

                // It means the channel is empty, send the `End` to notify `Collector`.
                // And then remove current sender(will not close due to `Arc`).
                if sender.capacity() == self.context.channel_size && !pre_removed.contains(table_id)
                {
                    // Not wait them to stop, check in next iterator.
                    sender
                        .send_timeout(
                            LogMessage::End,
                            Duration::from_millis(self.context.max_per_table_wait_ms),
                        )
                        .await
                        .map_err(|e| Box::new(e) as _)
                        .context(SplitSend {
                            shard_id: self.shard_id,
                            table_id: *table_id,
                        })?;
                    pre_removed.insert(table_id);
                }

                if sender.is_closed() {
                    removed.insert(table_id);
                    has_new_removed = true;
                }
            }

            if has_new_removed {
                debug!(
                    "Tables:{:?} 's splitting has finished, remove them from splitter",
                    removed
                );
            }

            // If all table channels are removed, splitter should stop.
            if removed.len() == self.senders.len() {
                info!("Split finished on shard:{}", self.shard_id);
                break;
            }

            tokio::time::sleep(Duration::from_millis(50)).await;
        }

        Ok(())
    }
}

pub(crate) struct CloneDecoder;

impl PayloadDecoder for CloneDecoder {
    type Error = Error;
    type Target = Vec<u8>;

    /// Just clone and return, impossible to return `Err`.
    fn decode<B: MemBuf>(&self, buf: &mut B) -> std::result::Result<Self::Target, Self::Error> {
        Ok(buf.remaining_slice().to_owned())
    }
}

#[derive(Debug)]
pub struct SplitContext {
    pub read_batch_size: usize,
    pub max_per_table_wait_ms: u64,
    pub channel_size: usize,
}

impl From<Config> for SplitContext {
    fn from(config: Config) -> Self {
        Self {
            read_batch_size: config.splitter_config.read_log_batch_size,
            max_per_table_wait_ms: config.splitter_config.max_per_table_wait_ms,
            channel_size: config.channel_size,
        }
    }
}
