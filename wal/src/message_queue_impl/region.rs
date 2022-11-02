// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

//! Region in wal on message queue

use std::sync::Arc;

use common_types::{bytes::BytesMut, table::TableId, SequenceNumber};
pub use error::*;
use log::{debug, info};
use message_queue::{ConsumeIterator, MessageQueue, Offset, OffsetType, StartOffset};
use snafu::{ensure, ResultExt};
use tokio::sync::{Mutex, RwLock};

use super::{
    encoding::{format_wal_data_topic_name, format_wal_meta_topic_name, MetaEncoding},
    helpers::{LogCleaner, SnapshotSynchronizer},
    region_meta::{RegionMeta, RegionMetaSnapshot, TableMetaData},
};
use crate::{
    kv_encoder::{CommonLogEncoding, CommonLogKey},
    log_batch::{LogEntry, LogWriteBatch},
    manager::{self, RegionId},
    message_queue_impl::{region_meta::OffsetRange, to_message},
};

pub mod error {
    use common_types::table::TableId;
    use common_util::define_result;
    use snafu::{Backtrace, Snafu};

    use crate::{
        manager::RegionId,
        message_queue_impl::{helpers, region_meta},
    };

    #[derive(Debug, Snafu)]
    #[snafu(visibility(pub))]
    pub enum Error {
        #[snafu(display(
            "Failed to write log to region, region id:{}, table id:{}, err:{}",
            region_id,
            table_id,
            source
        ))]
        WriteLog {
            region_id: RegionId,
            table_id: TableId,
            source: Box<dyn std::error::Error + Send + Sync>,
        },

        #[snafu(display(
            "Failed to read log from region, region id:{}, msg:{}\nBacktrace:{}",
            region_id,
            msg,
            backtrace
        ))]
        ReadAllLogsNoCause {
            region_id: RegionId,
            msg: String,
            backtrace: Backtrace,
        },

        #[snafu(display(
            "Failed to read log from region, region id:{}, err:{}",
            region_id,
            source
        ))]
        ReadAllLogsWithCause {
            region_id: RegionId,
            source: Box<dyn std::error::Error + Send + Sync>,
        },

        #[snafu(display(
            "Failed to get table meta data, region id:{}, table id:{}, err:{}",
            region_id,
            table_id,
            source
        ))]
        GetTableMeta {
            region_id: RegionId,
            table_id: TableId,
            source: region_meta::Error,
        },

        #[snafu(display(
            "Failed to get snapshot from region, region id:{}, err:{}",
            region_id,
            source
        ))]
        GetMetaSnapshot {
            region_id: RegionId,
            source: region_meta::Error,
        },

        #[snafu(display(
            "Failed to get snapshot from region, region id:{}, location:{:?}, err:{}",
            region_id,
            table_id,
            source
        ))]
        MarkDeleted {
            region_id: RegionId,
            table_id: TableId,
            source: region_meta::Error,
        },

        #[snafu(display("Failed to sync snapshot of region, err:{}", source))]
        SyncSnapshot { source: helpers::Error },

        #[snafu(display("Failed to clean logs of region, err:{}", source))]
        CleanLogs { source: helpers::Error },

        #[snafu(display("Failed to open region, namespace:{}, region id:{}, err:{}", namespace, region_id, source))]
        Open { 
            namespace: String,
            region_id: RegionId,
            source: Box<dyn std::error::Error + Send + Sync>,
        },
    }

    define_result!(Error);
}

/// Region in wal(message queue based)
#[allow(unused)]
pub struct Region<Mq: MessageQueue> {
    /// Region inner, see [RegionInner]
    ///
    /// Most of time, lock by `read lock`.
    /// While needing to freeze the region(such as, make a snapshot),
    /// `write lock` will be used.
    inner: RwLock<RegionInner<Mq>>,

    /// Will synchronize the snapshot to message queue by it
    ///
    /// Lock for forcing the snapshots to be synchronized sequentially.
    snapshot_synchronizer: Mutex<SnapshotSynchronizer<Mq>>,

    /// Clean the outdated logs which are marked delete
    log_cleaner: LogCleaner<Mq>,
}

#[allow(unused)]
impl<Mq: MessageQueue> Region<Mq> {
    /// Init the region.
    pub async fn open(
        namespace: &str,
        region_id: RegionId,
        message_queue: Arc<Mq>,
    ) -> Result<Self> {
        // Format to the topic name.
        let log_topic = format_wal_data_topic_name(namespace, region_id);
        let meta_topic = format_wal_meta_topic_name(namespace, region_id);
        message_queue
            .create_topic_if_not_exist(&log_topic)
            .await
            .map_err(|e| Box::new(e) as _)
            .context(Open {
                namespace,
                region_id,
            });
        message_queue
            .create_topic_if_not_exist(&meta_topic)
            .await
            .map_err(|e| Box::new(e) as _)
            .context(Open {
                namespace,
                region_id,
            });

        // Init region inner.
        let inner = RwLock::new(RegionInner::new(
            region_id,
            CommonLogEncoding::newest(),
            message_queue.clone(),
            log_topic.clone(),
        ));

        // Init others.
        let snapshot_synchronizer = Mutex::new(SnapshotSynchronizer::new(
            region_id,
            message_queue.clone(),
            meta_topic,
            MetaEncoding::newest(),
        ));
        let log_cleaner = LogCleaner::new(region_id, message_queue.clone(), log_topic);

        Ok(Region {
            inner,
            snapshot_synchronizer,
            log_cleaner,
        })
    }

    /// See [RegionInner]'s related method.
    pub async fn write(
        &self,
        ctx: &manager::WriteContext,
        log_batch: &LogWriteBatch,
    ) -> Result<SequenceNumber> {
        let inner = self.inner.read().await;
        inner.write(ctx, log_batch).await
    }

    /// See [RegionInner]'s related method.
    pub async fn read_all(
        &self,
        ctx: &manager::ReadContext,
    ) -> Result<MessageQueueLogIterator<Mq::ConsumeIterator>> {
        let inner = self.inner.read().await;
        inner.read_all(ctx).await
    }

    /// See [RegionInner]'s related method.
    pub async fn mark_delete_up_to(
        &self,
        table_id: TableId,
        sequence_num: SequenceNumber,
    ) -> Result<()> {
        let (snapshot, synchronizer) = {
            let inner = self.inner.write().await;
            inner
                .mark_delete_up_to(table_id, sequence_num)
                .await
                .unwrap();

            (
                inner.make_meta_snapshot().await,
                self.snapshot_synchronizer.lock().await,
            )
        };

        // TODO: a temporary and stupid implementation...
        // just need to sync the snapshot while dropping table, but now we sync while
        // every flushing... Just sync here now, obviously it is not enough.
        synchronizer.sync(snapshot).await.context(SyncSnapshot)
    }

    /// See [RegionInner]'s related method.
    pub async fn get_table_meta(&self, table_id: TableId) -> Result<TableMetaData> {
        let inner = self.inner.read().await;
        inner.get_table_meta(table_id).await
    }

    /// Clean outdated logs according to the information in region snapshot.
    pub async fn clean_logs(&self) -> Result<()> {
        // Get current snapshot, scan to get safe delete sequence number.
        let (snapshot, synchronizer) = {
            let inner = self.inner.write().await;
            (
                inner.make_meta_snapshot().await,
                self.snapshot_synchronizer.lock().await,
            )
        };

        // Get prepare delete to offset.
        self.log_cleaner
            .maybe_clean_logs(&snapshot)
            .await
            .context(CleanLogs)?;

        // Delete to and sync snapshot
        synchronizer.sync(snapshot).await.context(CleanLogs)
    }

    #[cfg(test)]
    async fn make_meta_snapshot(&self) -> RegionMetaSnapshot {
        let inner = self.inner.write().await;
        inner.make_meta_snapshot().await
    }
}

/// Region's inner, all methods of [Region] are mainly implemented in it.
#[allow(unused)]
struct RegionInner<Mq> {
    /// Id of region
    region_id: RegionId,
    /// Region meta data(such as, tables' next sequence numbers)
    region_meta: RegionMeta,
    /// Used to encode/decode the logs
    log_encoding: CommonLogEncoding,
    /// Message queue's Client
    message_queue: Arc<Mq>,
    /// Topic storing logs in message queue
    log_topic: String,
}

#[allow(unused)]
impl<Mq: MessageQueue> RegionInner<Mq> {
    pub fn new(
        region_id: RegionId,
        log_encoding: CommonLogEncoding,
        message_queue: Arc<Mq>,
        log_topic: String,
    ) -> Self {
        // TODO: use snapshot to recover `region_meta`.
        Self {
            region_id,
            region_meta: RegionMeta::default(),
            log_encoding,
            message_queue,
            log_topic,
        }
    }

    /// Write logs of table.
    async fn write(
        &self,
        _ctx: &manager::WriteContext,
        log_batch: &LogWriteBatch,
    ) -> Result<SequenceNumber> {
        // TODO: make use of ctx.
        debug!(
            "Begin to write to wal region, ctx:{:?}, region_id:{}, location:{:?}, log_entries_num:{}",
            _ctx,
            self.region_id,
            log_batch.location,
            log_batch.entries.len()
        );
        let location = &log_batch.location;
        let log_write_entries = &log_batch.entries;

        // Create messages and prepare for write.
        let mut sequence_num = self
            .region_meta
            .prepare_for_table_write(location.table_id)
            .await;
        let mut messages = Vec::with_capacity(log_batch.entries.len());
        let mut key_buf = BytesMut::new();
        for entry in log_write_entries {
            let log_key = CommonLogKey::new(self.region_id, location.table_id, sequence_num);
            self.log_encoding
                .encode_key(&mut key_buf, &log_key)
                .map_err(|e| Box::new(e) as _)
                .context(WriteLog {
                    region_id: self.region_id,
                    table_id: location.table_id,
                })?;

            let message = to_message(key_buf.to_vec(), entry.payload.clone());
            messages.push(message);

            sequence_num += 1;
        }

        // Write.
        let offsets = self
            .message_queue
            .produce(&self.log_topic, messages)
            .await
            .map_err(|e| Box::new(e) as _)
            .context(WriteLog {
                region_id: self.region_id,
                table_id: location.table_id,
            })?;
        debug!(
            "Produce to topic success, ctx:{:?}, region_id:{}, location:{:?}, topic:{}",
            _ctx, self.region_id, log_batch.location, self.log_topic,
        );

        // Update after write.
        self.region_meta
            .update_after_table_write(
                location.table_id,
                OffsetRange::new(*offsets.first().unwrap(), *offsets.last().unwrap()),
            )
            .await
            .map_err(|e| Box::new(e) as _)
            .context(WriteLog {
                region_id: self.region_id,
                table_id: location.table_id,
            })?;

        Ok(sequence_num - 1)
    }

    /// Read all logs from region, will return a iterator for high-level to
    /// poll.
    // TODO: take each read's timeout in consideration.
    async fn read_all(
        &self,
        _ctx: &manager::ReadContext,
    ) -> Result<MessageQueueLogIterator<Mq::ConsumeIterator>> {
        info!(
            "Begin to read all logs from region, region id:{}, log topic:{}, ctx:{:?}",
            self.region_id, self.log_topic, _ctx
        );

        let current_high_watermark = self
            .message_queue
            .fetch_offset(&self.log_topic, OffsetType::HighWaterMark)
            .await
            .map_err(|e| Box::new(e) as _)
            .context(ReadAllLogsWithCause {
                region_id: self.region_id,
            })?;
        let consume_iter = self
            .message_queue
            .consume(&self.log_topic, StartOffset::Earliest)
            .await
            .map_err(|e| Box::new(e) as _)
            .context(ReadAllLogsWithCause {
                region_id: self.region_id,
            })?;

        info!("Finished to read all logs from region, region id:{}, log topic:{}, terminate offset:{}", self.region_id, 
            self.log_topic, current_high_watermark);

        Ok(MessageQueueLogIterator::new(
            self.region_id,
            Some(current_high_watermark),
            consume_iter,
            self.log_encoding.clone(),
        ))
    }

    /// Mark the delete to sequence number of table.
    async fn mark_delete_up_to(
        &self,
        table_id: TableId,
        sequence_num: SequenceNumber,
    ) -> Result<()> {
        debug!(
            "Mark deleted entries up to sequence_num:{}, region_id:{}",
            sequence_num, self.region_id
        );

        self.region_meta
            .mark_table_deleted(table_id, sequence_num)
            .await
            .context(MarkDeleted {
                region_id: self.region_id,
                table_id,
            })
    }

    /// Get meta data by table id.
    async fn get_table_meta(&self, table_id: TableId) -> Result<TableMetaData> {
        self.region_meta
            .get_table_meta_data(table_id)
            .await
            .context(GetTableMeta {
                region_id: self.region_id,
                table_id,
            })
    }

    /// Get meta data snapshot of whole region.
    ///
    /// NOTICE: should freeze whole region before calling.
    async fn make_meta_snapshot(&self) -> RegionMetaSnapshot {
        self.region_meta.make_snapshot().await
    }
}

#[allow(unused)]
#[derive(Debug)]
pub struct MessageQueueLogIterator<C: ConsumeIterator> {
    /// Id of region
    region_id: RegionId,

    /// Polling's end point
    ///
    /// While fetching in slave node, it will be set to `None`, and
    /// reading will not stop.
    /// Otherwise, it will be set to high watermark.
    terminate_offset: Option<Offset>,

    /// Terminated flag
    is_terminated: bool,
    /// Consume Iterator of message queue
    iter: C,
    /// Used to encode/decode the logs
    log_encoding: CommonLogEncoding,
    /// See the same problem in https://github.com/CeresDB/ceresdb/issues/120
    previous_value: Vec<u8>,
    // TODO: timeout
}

#[allow(unused)]
impl<C: ConsumeIterator> MessageQueueLogIterator<C> {
    fn new(
        region_id: RegionId,
        terminate_offset: Option<Offset>,
        iter: C,
        log_encoding: CommonLogEncoding,
    ) -> Self {
        Self {
            region_id,
            terminate_offset,
            iter,
            is_terminated: false,
            log_encoding,
            previous_value: Vec::new(),
        }
    }
}

#[allow(unused)]
impl<C: ConsumeIterator> MessageQueueLogIterator<C> {
    pub async fn next_log_entry(&mut self) -> Result<Option<LogEntry<&'_ [u8]>>> {
        if self.is_terminated && self.terminate_offset.is_some() {
            info!(
                "Finished to poll log from message queue, region id:{}, terminate offset:{:?}",
                self.region_id, self.terminate_offset
            );
            return Ok(None);
        }

        let (message_and_offset, high_watermark) = self.iter.next_message().await.unwrap();

        // Check has race happened.
        if let Some(terminate_offset) = &self.terminate_offset {
            ensure!(*terminate_offset == high_watermark, ReadAllLogsNoCause {
                region_id: self.region_id,
                msg: format!("while terminate offset is set, remote high watermark should not change, terminate offset:{}, remote high watermark:{}",
                    terminate_offset, high_watermark),
            });

            if message_and_offset.offset + 1 == *terminate_offset {
                self.is_terminated = true;
            }
        }

        // Decode to log key and value, then create the returned log entry, key and
        // value absolutely exist.
        let log_key = self
            .log_encoding
            .decode_key(&message_and_offset.message.key.unwrap())
            .map_err(|e| Box::new(e) as _)
            .context(ReadAllLogsWithCause {
                region_id: self.region_id,
            })?;

        let log_value = message_and_offset.message.value.unwrap();
        let payload = self
            .log_encoding
            .decode_value(&log_value)
            .map_err(|e| Box::new(e) as _)
            .context(ReadAllLogsWithCause {
                region_id: self.region_id,
            })?;

        self.previous_value = payload.to_owned();

        Ok(Some(LogEntry {
            sequence: log_key.sequence_num,
            payload: self.previous_value.as_slice(),
        }))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use common_types::table::{ShardId, TableId};
    use message_queue::{
        kafka::{config::Config, kafka_impl::KafkaImpl},
        ConsumeIterator, MessageQueue, OffsetType, StartOffset,
    };

    use crate::{
        log_batch::PayloadDecoder,
        manager::{ReadContext, RegionId, WriteContext},
        message_queue_impl::{encoding::MetaEncoding, test_util::TestContext},
    };

    #[tokio::test]
    #[ignore]
    async fn test_region_kafka_impl() {
        // Test region
        let mut config = Config::default();
        config.client_config.boost_broker = Some("127.0.0.1:9011".to_string());
        let kafka_impl = KafkaImpl::new(config).await.unwrap();
        let message_queue = Arc::new(kafka_impl);
        test_region(message_queue).await;
    }

    async fn test_region<Mq: MessageQueue>(message_queue: Arc<Mq>) {
        let shard_id = 42;
        let region_id = 42;
        let test_payloads = vec![42, 43, 44, 45, 46];

        let mut test_datas = Vec::new();
        for table_id in 0..5_u64 {
            test_datas.push((table_id, test_payloads.clone()));
        }

        test_read_write(
            region_id,
            shard_id,
            test_datas.clone(),
            message_queue.clone(),
        )
        .await;

        test_mark_and_delete(
            region_id,
            shard_id,
            test_datas.clone(),
            message_queue.clone(),
        )
        .await;
    }

    async fn test_read_write<Mq: MessageQueue>(
        region_id: RegionId,
        shard_id: ShardId,
        test_datas: Vec<(TableId, Vec<u32>)>,
        message_queue: Arc<Mq>,
    ) {
        let table_num = test_datas.len();
        let test_context = TestContext::new(region_id, shard_id, test_datas, message_queue).await;

        // Write.
        let mut mixed_test_payloads = Vec::new();
        for i in 0..table_num {
            let test_log_batch = &test_context.test_datas[i].1.test_log_batch;
            let test_payloads = &test_context.test_datas[i].1.test_payloads;
            // Write.
            let sequence_num = test_context
                .region
                .write(&WriteContext::default(), test_log_batch)
                .await
                .unwrap();
            assert_eq!(sequence_num, test_log_batch.len() as u64 - 1);

            mixed_test_payloads.extend_from_slice(test_payloads);
        }

        // Read and compare.
        let mut mixed_decoded_res = Vec::new();
        let mut msg_iter = test_context
            .region
            .read_all(&ReadContext::default())
            .await
            .unwrap();
        while let Some(log_entry) = msg_iter.next_log_entry().await.unwrap() {
            let mut payload = log_entry.payload;
            let decoded_payload = test_context
                .test_payload_encoder
                .decode(&mut payload)
                .unwrap();
            mixed_decoded_res.push(decoded_payload.val);
        }

        assert_eq!(mixed_test_payloads, mixed_decoded_res);
    }

    async fn test_mark_and_delete<Mq: MessageQueue>(
        region_id: RegionId,
        shard_id: ShardId,
        test_datas: Vec<(TableId, Vec<u32>)>,
        message_queue: Arc<Mq>,
    ) {
        let table_num = test_datas.len();
        let test_context = TestContext::new(region_id, shard_id, test_datas, message_queue).await;

        // Mark deleted and check
        for table_idx in 0..table_num {
            mark_deleted_and_check(&test_context, table_idx).await;
        }

        // Check logs have been deleted, its earliest offset should have changed.
        test_context.region.clean_logs().await.unwrap();
        let new_earliest = test_context
            .message_queue
            .fetch_offset(&test_context.log_topic, OffsetType::EarliestOffset)
            .await
            .unwrap();
        assert_eq!(
            new_earliest,
            test_context.test_datas[0].1.test_log_batch.len() as i64
        );

        check_sync_snapshot(&test_context).await;
    }

    async fn check_sync_snapshot<Mq: MessageQueue>(test_context: &TestContext<Mq>) {
        // Only one meta record will exist in normal.
        let earliest = test_context
            .message_queue
            .fetch_offset(&test_context.meta_topic, OffsetType::EarliestOffset)
            .await
            .unwrap();
        let latest = test_context
            .message_queue
            .fetch_offset(&test_context.meta_topic, OffsetType::HighWaterMark)
            .await
            .unwrap();
        assert_eq!(earliest + 1, latest);

        // Compare local snapshot and remote one.
        // Local
        let local_snapshot = test_context.region.make_meta_snapshot().await;

        // Remote
        let meta_encoding = MetaEncoding::newest();
        let mut iter = test_context
            .message_queue
            .consume(&test_context.meta_topic, StartOffset::Earliest)
            .await
            .unwrap();
        let (message_and_offset, high_watermark) = iter.next_message().await.unwrap();
        assert_eq!(message_and_offset.offset + 1, high_watermark);
        let decoded_meta_key = meta_encoding
            .decode_key(&message_and_offset.message.key.unwrap())
            .unwrap();
        assert_eq!(test_context.region_id, decoded_meta_key.0);
        let remote_snapshot = meta_encoding
            .decode_value(&message_and_offset.message.value.unwrap())
            .unwrap();

        assert_eq!(local_snapshot, remote_snapshot);
    }

    async fn mark_deleted_and_check<Mq: MessageQueue>(
        test_context: &TestContext<Mq>,
        table_idx: usize,
    ) {
        let test_log_batch = &test_context.test_datas[table_idx].1.test_log_batch;
        let table_id = test_context.test_datas[table_idx].0;

        // Write.
        let base_offset = test_log_batch.len() as i64 * 2 * table_idx as i64;
        let sequence_num = test_context
            .region
            .write(&WriteContext::default(), test_log_batch)
            .await
            .unwrap();
        assert_eq!(sequence_num, test_log_batch.len() as u64 - 1);
        let sequence_num = test_context
            .region
            .write(&WriteContext::default(), test_log_batch)
            .await
            .unwrap();
        assert_eq!(sequence_num, test_log_batch.len() as u64 * 2 - 1);

        // Mark deleted.
        test_context
            .region
            .mark_delete_up_to(table_id, test_log_batch.len() as u64)
            .await
            .unwrap();
        let table_meta = test_context.region.get_table_meta(table_id).await.unwrap();
        assert_eq!(
            table_meta.next_sequence_num,
            test_log_batch.len() as u64 * 2
        );
        assert_eq!(
            table_meta.latest_marked_deleted,
            test_log_batch.len() as u64
        );
        assert_eq!(
            table_meta.current_high_watermark,
            base_offset + test_log_batch.len() as i64 * 2
        );
        assert_eq!(
            table_meta.safe_delete_offset,
            Some(base_offset + test_log_batch.len() as i64)
        );

        check_sync_snapshot(test_context).await;
    }
}
