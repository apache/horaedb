// Copyright 2022-2023 CeresDB Project Authors. Licensed under Apache-2.0.

//! Log cleaner

use std::sync::Arc;

use generic_error::{BoxError, GenericError};
use log::info;
use macros::define_result;
use message_queue::{MessageQueue, Offset};
use snafu::{ensure, Backtrace, ResultExt, Snafu};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display(
        "Failed to clean logs of region, region id:{}, topic:{}, err:{}",
        region_id,
        topic,
        source
    ))]
    CleanLogsWithCause {
        region_id: u64,
        topic: String,
        source: GenericError,
    },

    #[snafu(display(
        "Failed to clean logs of region with no cause, region id:{}, topic:{}, msg:{}, \nBacktrace:\n{}",
        region_id,
        topic,
        msg,
        backtrace,
    ))]
    CleanLogsNoCause {
        region_id: u64,
        topic: String,
        msg: String,
        backtrace: Backtrace,
    },
}

define_result!(Error);

/// Check and clean the outdated logs
pub struct LogCleaner<M: MessageQueue> {
    region_id: u64,
    message_queue: Arc<M>,
    log_topic: String,
    last_deleted_offset: Option<Offset>,
}

impl<M: MessageQueue> LogCleaner<M> {
    pub fn new(region_id: u64, message_queue: Arc<M>, log_topic: String) -> Self {
        info!(
            "Log cleaner init, region id:{}, log topic:{}",
            region_id, log_topic
        );

        Self {
            region_id,
            message_queue,
            log_topic,
            last_deleted_offset: None,
        }
    }

    pub async fn maybe_clean_logs(&mut self, safe_delete_offset: Offset) -> Result<()> {
        info!(
            "Region clean logs begin, region id:{}, topic:{}, safe delete offset:{:?}",
            self.region_id, self.log_topic, safe_delete_offset
        );

        // Decide whether cleaning should be done.
        let mut do_clean = true;
        if let Some(last_deleted_offset) = self.last_deleted_offset {
            ensure!(safe_delete_offset >= last_deleted_offset, CleanLogsNoCause {
                region_id: self.region_id,
                topic: self.log_topic.clone(),
                msg: format!("the new safe delete offset should be larger than the last deleted offset, inner state inconsistent, 
                safe delete offset:{safe_delete_offset}, last deleted offset:{last_deleted_offset}"),
            });

            if safe_delete_offset == last_deleted_offset {
                do_clean = false;
            }
        }

        if do_clean {
            // Delete logs to above offset and sync snapshot after.
            self.message_queue
                .delete_to(&self.log_topic, safe_delete_offset)
                .await
                .box_err()
                .context(CleanLogsWithCause {
                    region_id: self.region_id,
                    topic: self.log_topic.clone(),
                })?;

            self.last_deleted_offset = Some(safe_delete_offset);
        }

        info!(
            "Region clean logs finish, do clean:{}, region id:{}, topic:{}, prepare delete to offset:{:?}",
            do_clean, self.region_id, self.log_topic, safe_delete_offset
        );

        Ok(())
    }
}
