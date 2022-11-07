// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

//! Log cleaner

use std::{cmp, sync::Arc};

use common_util::define_result;
use log::info;
use message_queue::{MessageQueue, Offset};
use snafu::{ensure, Backtrace, ResultExt, Snafu};

use crate::{manager::RegionId, message_queue_impl::region_meta::RegionMetaSnapshot};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display(
        "Failed to clean logs of region, region id:{}, topic:{}, err:{}",
        region_id,
        topic,
        source
    ))]
    CleanLogsWithCause {
        region_id: RegionId,
        topic: String,
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display(
        "Failed to clean logs of region with no cause, region id:{}, topic:{}, msg:{}, \nBacktrace:\n{}",
        region_id,
        topic,
        msg,
        backtrace,
    ))]
    CleanLogsNoCause {
        region_id: RegionId,
        topic: String,
        msg: String,
        backtrace: Backtrace,
    },
}

define_result!(Error);

/// Check and clean the outdated logs
pub struct LogCleaner<M: MessageQueue> {
    region_id: RegionId,
    message_queue: Arc<M>,
    log_topic: String,
    last_deleted_offset: Option<Offset>,
}

impl<M: MessageQueue> LogCleaner<M> {
    pub fn new(region_id: RegionId, message_queue: Arc<M>, log_topic: String) -> Self {
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

    pub async fn maybe_clean_logs(&mut self, snapshot: &RegionMetaSnapshot) -> Result<()> {
        info!(
            "Begin to check and clean logs, region id:{} snapshot:{:?}, topic:{}",
            self.region_id, snapshot, self.log_topic
        );

        // Get offset preparing to delete to.
        let safe_delete_offset = Self::calc_safe_delete_offset(snapshot);

        // Decide whether cleaning should be done.
        let mut do_clean = true;
        if let Some(last_deleted_offset) = self.last_deleted_offset {
            ensure!(safe_delete_offset >= last_deleted_offset, CleanLogsNoCause {
                region_id: self.region_id,
                topic: self.log_topic.clone(),
                msg: format!("the new safe delete offset should be larger than the last deleted offset, inner state inconsistent, 
                safe delete offset:{}, last deleted offset:{}", safe_delete_offset, last_deleted_offset),
            });

            if safe_delete_offset == last_deleted_offset {
                do_clean = false;
            }
        }

        if do_clean {
            // Delete logs to above offset and sync snapshot after.
            self.message_queue
                .delete_up_to(&self.log_topic, safe_delete_offset)
                .await
                .map_err(|e| Box::new(e) as _)
                .context(CleanLogsWithCause {
                    region_id: self.region_id,
                    topic: self.log_topic.clone(),
                })?;

            self.last_deleted_offset = Some(safe_delete_offset);
        }

        info!(
            "Finished to check and clean logs, do clean:{}, region id:{}, topic:{}, prepare delete to offset:{:?}",
            do_clean, self.region_id, self.log_topic, safe_delete_offset
        );

        Ok(())
    }

    fn calc_safe_delete_offset(snapshot: &RegionMetaSnapshot) -> Offset {
        let mut safe_delete_offset = Offset::MAX;
        let mut high_watermark = 0;
        // Calc the min offset in message queue.
        for table_meta in &snapshot.entries {
            if let Some(offset) = table_meta.safe_delete_offset {
                safe_delete_offset = cmp::min(safe_delete_offset, offset);
            }
            high_watermark = cmp::max(high_watermark, table_meta.current_high_watermark);
        }

        if safe_delete_offset == Offset::MAX {
            // All tables are in such states: after init/flush, but not written.
            // So, we can directly delete it up to the high_watermark.
            high_watermark
        } else {
            safe_delete_offset
        }
    }
}
