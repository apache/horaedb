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

//! Snapshot synchronizer

use std::sync::Arc;

use bytes_ext::BytesMut;
use generic_error::{BoxError, GenericError};
use log::debug;
use macros::define_result;
use message_queue::MessageQueue;
use snafu::{ensure, Backtrace, ResultExt, Snafu};

use crate::message_queue_impl::{
    self,
    encoding::{MetaEncoding, MetaKey},
    region_context::RegionMetaSnapshot,
};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display(
        "Failed to sync snapshot of region, region id:{}, topic:{}, err:{}",
        region_id,
        topic,
        source
    ))]
    SyncSnapshotWithCause {
        region_id: u64,
        topic: String,
        source: GenericError,
    },

    #[snafu(display(
        "Failed to sync snapshot of region with cause, region id:{}, topic:{}, msg:{}, \nBacktrace:\n{}",
        region_id,
        topic,
        msg,
        backtrace
    ))]
    SyncSnapshotNoCause {
        region_id: u64,
        topic: String,
        msg: String,
        backtrace: Backtrace,
    },
}

define_result!(Error);

/// Encode the region snapshot and push to message queue
///
/// It will be locked before being called to keep the order of snapshots.
pub struct SnapshotSynchronizer<Mq: MessageQueue> {
    region_id: u64,
    message_queue: Arc<Mq>,
    meta_topic: String,
    meta_encoding: MetaEncoding,
}

impl<Mq: MessageQueue> SnapshotSynchronizer<Mq> {
    pub fn new(
        region_id: u64,
        message_queue: Arc<Mq>,
        meta_topic: String,
        meta_encoding: MetaEncoding,
    ) -> Self {
        Self {
            region_id,
            message_queue,
            meta_topic,
            meta_encoding,
        }
    }

    pub async fn sync(&self, snapshot: RegionMetaSnapshot) -> Result<()> {
        debug!(
            "Begin to sync snapshot to meta topic, snapshot:{:?}, topic:{}",
            snapshot, self.meta_topic
        );

        let mut key_buf = BytesMut::new();
        let mut value_buf = BytesMut::new();
        self.meta_encoding
            .encode_key(&mut key_buf, &MetaKey(self.region_id))
            .box_err()
            .context(SyncSnapshotWithCause {
                region_id: self.region_id,
                topic: self.meta_topic.clone(),
            })?;
        self.meta_encoding
            .encode_value(&mut value_buf, snapshot)
            .box_err()
            .context(SyncSnapshotWithCause {
                region_id: self.region_id,
                topic: self.meta_topic.clone(),
            })?;

        // Try to write to message queue.
        let messages = vec![message_queue_impl::to_message(
            key_buf.to_vec(),
            value_buf.to_vec(),
        )];
        let offsets = self
            .message_queue
            .produce(&self.meta_topic, messages)
            .await
            .box_err()
            .context(SyncSnapshotWithCause {
                region_id: self.region_id,
                topic: self.meta_topic.clone(),
            })?;

        ensure!(
            offsets.len() == 1,
            SyncSnapshotNoCause {
                region_id: self.region_id,
                topic: self.meta_topic.clone(),
                msg: format!(
                    "offsets returned from message queue with a invalid len, real:{}, expected:1",
                    offsets.len()
                ),
            }
        );

        // Delete old snapshots.
        self.message_queue
            .delete_to(&self.meta_topic, offsets[0])
            .await
            .box_err()
            .context(SyncSnapshotWithCause {
                region_id: self.region_id,
                topic: self.meta_topic.clone(),
            })?;

        debug!(
            "Finished to sync snapshot to meta topic, topic:{}",
            self.meta_topic
        );

        Ok(())
    }
}
