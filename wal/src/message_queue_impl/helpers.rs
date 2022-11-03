// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

//! Some helper components

use std::{cmp, sync::Arc};

use common_types::bytes::BytesMut;
use common_util::define_result;
use log::info;
use message_queue::{MessageQueue, Offset};
use snafu::{ResultExt, Snafu};

use super::{
    encoding::{MetaEncoding, MetaKey},
    region_meta::RegionMetaSnapshot,
    to_message,
};
use crate::manager::RegionId;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display(
        "Failed to sync snapshot of region, region id:{}, topic:{}, err:{}",
        region_id,
        topic,
        source
    ))]
    SyncSnapshot {
        region_id: RegionId,
        topic: String,
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display(
        "Failed to clean logs of region, region id:{}, topic:{}, err:{}",
        region_id,
        topic,
        source
    ))]
    CleanLogs {
        region_id: RegionId,
        topic: String,
        source: Box<dyn std::error::Error + Send + Sync>,
    },
}

define_result!(Error);

/// Encode the region snapshot and push to message queue
///
/// It will be locked before being called to keep the order of snapshots.
pub struct SnapshotSynchronizer<Mq: MessageQueue> {
    region_id: RegionId,
    message_queue: Arc<Mq>,
    meta_topic: String,
    meta_encoding: MetaEncoding,
}

impl<Mq: MessageQueue> SnapshotSynchronizer<Mq> {
    pub fn new(
        region_id: RegionId,
        message_queue: Arc<Mq>,
        meta_topic: String,
        meta_encoding: MetaEncoding,
    ) -> Self {
        info!(
            "Snapshot synchronizer init, region id:{}, meta topic:{}",
            region_id, meta_topic
        );

        Self {
            region_id,
            message_queue,
            meta_topic,
            meta_encoding,
        }
    }

    pub async fn sync(&self, snapshot: RegionMetaSnapshot) -> Result<()> {
        info!(
            "Begin to sync snapshot to meta topic, snapshot:{:?}, topic:{}",
            snapshot, self.meta_topic
        );

        let mut key_buf = BytesMut::new();
        let mut value_buf = BytesMut::new();
        self.meta_encoding
            .encode_key(&mut key_buf, &MetaKey(self.region_id))
            .map_err(|e| Box::new(e) as _)
            .context(SyncSnapshot {
                region_id: self.region_id,
                topic: self.meta_topic.clone(),
            })?;
        self.meta_encoding
            .encode_value(&mut value_buf, snapshot)
            .map_err(|e| Box::new(e) as _)
            .context(SyncSnapshot {
                region_id: self.region_id,
                topic: self.meta_topic.clone(),
            })?;

        // Try to write to message queue.
        let messages = vec![to_message(key_buf.to_vec(), value_buf.to_vec())];
        let offsets = self
            .message_queue
            .produce(&self.meta_topic, messages)
            .await
            .map_err(|e| Box::new(e) as _)
            .context(SyncSnapshot {
                region_id: self.region_id,
                topic: self.meta_topic.clone(),
            })?;
        assert_eq!(offsets.len(), 1);

        // Delete old snapshots.
        self.message_queue
            .delete_up_to(&self.meta_topic, offsets[0])
            .await
            .map_err(|e| Box::new(e) as _)
            .context(SyncSnapshot {
                region_id: self.region_id,
                topic: self.meta_topic.clone(),
            })?;

        info!(
            "Finished to sync snapshot to meta topic, topic:{}",
            self.meta_topic
        );

        Ok(())
    }
}

/// Check and clean the outdated logs
pub struct LogCleaner<Mq: MessageQueue> {
    region_id: RegionId,
    message_queue: Arc<Mq>,
    log_topic: String,
    last_deleted_offset: Option<Offset>,
}

impl<Mq: MessageQueue> LogCleaner<Mq> {
    pub fn new(region_id: RegionId, message_queue: Arc<Mq>, log_topic: String) -> Self {
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

    pub async fn maybe_clean_logs(&self, snapshot: &RegionMetaSnapshot) -> Result<()> {
        info!(
            "Begin to check and clean logs, region id:{} snapshot:{:?}, topic:{}",
            self.region_id, snapshot, self.log_topic
        );

        // Get prepare delete to offset.
        let safe_delete_offset = Self::calc_safe_delete_offset(snapshot);
        let prepare_delete_to_offset = match self.last_deleted_offset {
            Some(last_deleted_offset) => {
                assert!(safe_delete_offset >= last_deleted_offset,
                    "impossible the new safe delete offset less than the last deleted offset, inner state inconsistent, 
                    safe delete offset:{}, last deleted offset:{}", safe_delete_offset, last_deleted_offset
                );

                if safe_delete_offset > last_deleted_offset {
                    Some(last_deleted_offset)
                } else {
                    None
                }
            }

            None => Some(safe_delete_offset),
        };

        // Delete to and sync snapshot
        if let Some(prepare_delete_to_offset) = prepare_delete_to_offset {
            self.message_queue
                .delete_up_to(&self.log_topic, prepare_delete_to_offset)
                .await
                .map_err(|e| Box::new(e) as _)
                .context(CleanLogs {
                    region_id: self.region_id,
                    topic: self.log_topic.clone(),
                })?;
        }

        info!(
            "Finished to check and clean logs, region id:{}, topic:{}, prepare delete to offset:{:?}",
            self.region_id, self.log_topic, prepare_delete_to_offset
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
