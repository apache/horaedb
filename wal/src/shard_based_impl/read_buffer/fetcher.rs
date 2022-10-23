// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

// ! Log fetcher

use std::collections::VecDeque;

use common_types::table::{ShardId, TableId};
use log::debug;

use super::{error::*, LogMessage, LogReceiverRef};
use crate::shard_based_impl::config::Config;

pub(crate) struct Fetcher {
    shard_id: ShardId,
    table_id: TableId,
    receiver: LogReceiverRef,
    context: FetchContext,
    terminate: bool,
}

impl Fetcher {
    pub fn new(
        shard_id: ShardId,
        table_id: TableId,
        receiver: LogReceiverRef,
        context: FetchContext,
    ) -> Self {
        Self {
            shard_id,
            table_id,
            receiver,
            context,
            terminate: false,
        }
    }

    pub async fn fetch(&mut self, mut buffer: VecDeque<Vec<u8>>) -> Result<VecDeque<Vec<u8>>> {
        buffer.clear();

        if self.terminate {
            return Ok(buffer);
        }

        let mut receiver = self.receiver.lock().await;
        loop {
            match receiver.recv().await {
                Some(LogMessage::Content(raw_log)) => {
                    buffer.push_back(raw_log);
                }

                Some(LogMessage::End) => {
                    self.terminate = true;
                    break;
                }

                None => {
                    Unknown {
                        msg: format!(
                            "table:{} on shard:{} fetch None message, it is impossible in normal",
                            self.shard_id, self.table_id
                        ),
                    }
                    .fail()?;
                }
            }

            if buffer.len() == self.context.fetch_batch_size {
                self.terminate = true;
                break;
            }
        }
        debug!("end!");

        Ok(buffer)
    }
}

#[derive(Debug)]
pub struct FetchContext {
    fetch_batch_size: usize,
}

impl From<Config> for FetchContext {
    fn from(config: Config) -> Self {
        Self {
            fetch_batch_size: config.fetcher_config.fetch_batch_size,
        }
    }
}
