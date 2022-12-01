// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

//! Test tools for wal on message queue

use std::sync::Arc;

use common_types::table::TableId;
use message_queue::MessageQueue;

use super::{
    encoding::{format_wal_data_topic_name, format_wal_meta_topic_name},
    region::Region,
};
use crate::{
    kv_encoder::LogBatchEncoder,
    log_batch::LogWriteBatch,
    manager::{RegionId, RegionVersion, WalLocation},
    tests::util::{TestPayload, TestPayloadDecoder},
};

pub struct TestContext<Mq: MessageQueue> {
    pub region_id: RegionId,
    pub region_version: RegionVersion,
    pub table_id: TableId,
    pub test_datas: Vec<(TableId, TestDataOfTable)>,
    pub test_payload_encoder: TestPayloadDecoder,
    pub region: Region<Mq>,
    pub message_queue: Arc<Mq>,
    pub log_topic: String,
    pub meta_topic: String,
}

pub struct TestDataOfTable {
    pub test_payloads: Vec<u32>,
    pub test_log_batch: LogWriteBatch,
}

impl TestDataOfTable {
    fn new(test_payloads: Vec<u32>, test_log_batch: LogWriteBatch) -> Self {
        Self {
            test_payloads,
            test_log_batch,
        }
    }
}

impl<Mq: MessageQueue> TestContext<Mq> {
    pub async fn new(
        namespace: String,
        region_id: RegionId,
        region_version: RegionVersion,
        table_id: TableId,
        test_datas: Vec<(TableId, Vec<u32>)>,
        message_queue: Arc<Mq>,
    ) -> Self {
        // Test data
        let test_payload_encoder = TestPayloadDecoder;
        let test_datas = test_datas
            .into_iter()
            .map(|(table_id, data)| {
                let log_batch_encoder =
                    LogBatchEncoder::create(WalLocation::new(region_id, region_version, table_id));
                let log_write_batch = log_batch_encoder
                    .encode_batch::<TestPayload, u32>(&data)
                    .unwrap();

                (table_id, TestDataOfTable::new(data, log_write_batch))
            })
            .collect();

        let region = Region::open(&namespace, region_id, message_queue.clone())
            .await
            .unwrap();

        let log_topic = format_wal_data_topic_name(&namespace, region_id);
        let meta_topic = format_wal_meta_topic_name(&namespace, region_id);

        TestContext {
            region_id,
            region_version,
            table_id,
            test_datas,
            test_payload_encoder,
            region,
            message_queue,
            log_topic,
            meta_topic,
        }
    }
}
