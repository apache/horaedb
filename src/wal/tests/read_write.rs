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

use std::{
    collections::{HashMap, VecDeque},
    ops::Deref,
    path::Path,
    str::FromStr,
    sync::Arc,
};

use async_trait::async_trait;
use common_types::{
    table::{TableId, DEFAULT_SHARD_ID},
    SequenceNumber,
};
use message_queue::kafka::{config::Config as KafkaConfig, kafka_impl::KafkaImpl};
use runtime::{self, Runtime};
use table_kv::memory::MemoryImpl;
use tempfile::TempDir;
use time_ext::ReadableDuration;
use wal::{
    kv_encoder::LogBatchEncoder,
    log_batch::{LogWriteBatch, MemoryPayload, MemoryPayloadDecoder},
    manager::{
        BatchLogIteratorAdapter, ReadBoundary, ReadContext, ReadRequest, ScanRequest, WalLocation,
        WalManager, WalManagerRef, WalRuntimes, WriteContext,
    },
    message_queue_impl::{config::KafkaWalConfig, wal::MessageQueueImpl},
    rocksdb_impl::manager::RocksImpl,
    table_kv_impl::{model::NamespaceConfig, wal::WalNamespaceImpl},
};

#[test]
fn test_rocksdb_wal() {
    let builder = RocksWalBuilder;
    test_all(builder, false);
}

#[test]
fn test_memory_table_wal_default() {
    let builder = MemoryTableWalBuilder::default();
    test_all(builder, true);
}

#[test]
fn test_memory_table_wal_with_ttl() {
    let builder = MemoryTableWalBuilder::with_ttl("1d");
    test_all(builder, true);
}

#[test]
#[ignore = "this test needs a kafka cluster"]
fn test_kafka_wal() {
    let builder = KafkaWalBuilder::new();
    test_all(builder, true);
}

fn test_all<B: WalBuilder>(builder: B, is_distributed: bool) {
    test_simple_read_write_default_batch(builder.clone());
    test_simple_read_write_different_batch_size(builder.clone());
    test_read_with_boundary(builder.clone());
    test_write_multiple_regions(builder.clone());
    test_reopen(builder.clone());
    test_complex_read_write(builder.clone());
    test_simple_write_delete(builder.clone());
    test_write_delete_half(builder.clone());
    test_write_delete_multiple_regions(builder.clone());
    test_sequence_increase_monotonically_multiple_writes(builder.clone());
    test_sequence_increase_monotonically_delete_write(builder.clone());
    test_sequence_increase_monotonically_delete_reopen_write(builder.clone());
    test_write_scan(builder.clone());
    if is_distributed {
        test_move_from_nodes(builder);
    }
}

fn test_simple_read_write_default_batch<B: WalBuilder>(builder: B) {
    let table_id = 0;
    let env = TestEnv::new(2, builder);
    env.runtime.block_on(simple_read_write(
        &env,
        WalLocation::new(DEFAULT_SHARD_ID as u64, table_id),
    ));
}

fn test_simple_read_write_different_batch_size<B: WalBuilder>(builder: B) {
    let table_id = 0;
    let batch_sizes = [1, 2, 4, 10, 100];

    for batch_size in batch_sizes {
        let mut env = TestEnv::new(2, builder.clone());
        env.read_ctx.batch_size = batch_size;
        env.runtime.block_on(simple_read_write(
            &env,
            WalLocation::new(DEFAULT_SHARD_ID as u64, table_id),
        ));
    }
}

fn test_read_with_boundary<B: WalBuilder>(builder: B) {
    let env = TestEnv::new(2, builder);
    env.runtime.block_on(read_with_boundary(&env));
}

fn test_write_multiple_regions<B: WalBuilder>(builder: B) {
    let env = Arc::new(TestEnv::new(4, builder));
    env.runtime
        .block_on(write_multiple_regions_parallelly(env.clone()));
}

fn test_reopen<B: WalBuilder>(builder: B) {
    let env = TestEnv::new(2, builder);
    env.runtime.block_on(reopen(&env, 5));
}

fn test_complex_read_write<B: WalBuilder>(builder: B) {
    let env = TestEnv::new(2, builder);
    env.runtime.block_on(complex_read_write(&env));
}

fn test_simple_write_delete<B: WalBuilder>(builder: B) {
    let env = TestEnv::new(2, builder);
    env.runtime.block_on(simple_write_delete(&env));
}

fn test_write_delete_half<B: WalBuilder>(builder: B) {
    let env = TestEnv::new(2, builder);
    env.runtime.block_on(write_delete_half(&env));
}

fn test_write_delete_multiple_regions<B: WalBuilder>(builder: B) {
    let env = TestEnv::new(2, builder);
    env.runtime.block_on(write_delete_multiple_regions(&env));
}

fn test_sequence_increase_monotonically_multiple_writes<B: WalBuilder>(builder: B) {
    let env = TestEnv::new(2, builder);
    env.runtime
        .block_on(sequence_increase_monotonically_multiple_writes(&env));
}

fn test_sequence_increase_monotonically_delete_write<B: WalBuilder>(builder: B) {
    let env = TestEnv::new(2, builder);
    env.runtime
        .block_on(sequence_increase_monotonically_delete_write(&env));
}

fn test_sequence_increase_monotonically_delete_reopen_write<B: WalBuilder>(builder: B) {
    let env = TestEnv::new(2, builder);
    env.runtime
        .block_on(sequence_increase_monotonically_delete_reopen_write(&env));
}

fn test_write_scan<B: WalBuilder>(builder: B) {
    let env = TestEnv::new(2, builder);
    env.runtime.block_on(write_scan(&env));
}

fn test_move_from_nodes<B: WalBuilder>(builder: B) {
    let env = TestEnv::new(2, builder);
    let region_id = 1;
    let table_id = 0;

    env.runtime.block_on(async {
        // Use two wal managers to represent datanode 1 and datanode 2.
        // At first, write some things in node 1.
        let wal_1 = env.build_wal().await;
        simple_read_write_with_range_and_wal(
            &env,
            wal_1.clone(),
            WalLocation::new(region_id, table_id),
            0,
            10,
        )
        .await;

        // The table are move to node 2 but in the same shard, so its region id is still
        // 0.
        wal_1.close_region(region_id).await.unwrap();
        let wal_2 = env.build_wal().await;
        simple_read_write_with_range_and_wal(
            &env,
            wal_2.clone(),
            WalLocation::new(region_id, table_id),
            10,
            20,
        )
        .await;

        // Finally, the table with the same shard is moved to node 1 again.
        wal_2.close_region(region_id).await.unwrap();
        simple_read_write_with_range_and_wal(
            &env,
            wal_1,
            WalLocation::new(region_id, table_id),
            20,
            30,
        )
        .await;
    });
}

async fn check_write_batch_with_read_request<B: WalBuilder>(
    env: &TestEnv<B>,
    wal: WalManagerRef,
    read_req: ReadRequest,
    max_seq: SequenceNumber,
    payload_batch: &[MemoryPayload],
) {
    let iter = wal
        .read_batch(&env.read_ctx, &read_req)
        .await
        .expect("should succeed to read");

    let test_table_data =
        TestTableData::new(read_req.location.table_id, payload_batch.to_vec(), max_seq);
    env.check_log_entries(vec![test_table_data], iter).await;
}

async fn check_write_batch<B: WalBuilder>(
    env: &TestEnv<B>,
    wal: WalManagerRef,
    location: WalLocation,
    max_seq: SequenceNumber,
    payload_batch: &[MemoryPayload],
) {
    let read_req = ReadRequest {
        location,
        start: ReadBoundary::Included(max_seq + 1 - payload_batch.len() as u64),
        end: ReadBoundary::Included(max_seq),
    };
    check_write_batch_with_read_request(env, wal, read_req, max_seq, payload_batch).await
}

async fn simple_read_write_with_wal<B: WalBuilder>(
    env: impl Deref<Target = TestEnv<B>>,
    wal: WalManagerRef,
    location: WalLocation,
) {
    simple_read_write_with_range_and_wal_internal(env, wal, location, 0, 10).await;
}

async fn simple_read_write<B: WalBuilder>(env: &TestEnv<B>, location: WalLocation) {
    let wal = env.build_wal().await;

    simple_read_write_with_range_and_wal(env, wal.clone(), location, 0, 10).await;

    wal.close_gracefully().await.unwrap();
}

async fn simple_read_write_with_range_and_wal<B: WalBuilder>(
    env: &TestEnv<B>,
    wal: WalManagerRef,
    location: WalLocation,
    last_end_seq: SequenceNumber,
    current_end_seq: SequenceNumber,
) {
    // Empty region has 0 sequence num.
    let last_seq = wal.sequence_num(location).await.unwrap();
    assert_eq!(last_end_seq, last_seq);

    simple_read_write_with_range_and_wal_internal(
        env,
        wal.clone(),
        location,
        last_end_seq as u32,
        current_end_seq as u32,
    )
    .await;

    let last_seq = wal.sequence_num(location).await.unwrap();
    assert_eq!(current_end_seq, last_seq);
}

async fn simple_read_write_with_range_and_wal_internal<B: WalBuilder>(
    env: impl Deref<Target = TestEnv<B>>,
    wal: WalManagerRef,
    location: WalLocation,
    start: u32,
    end: u32,
) {
    let (payload_batch, write_batch) = env.build_log_batch(location, start, end).await;
    let seq = wal
        .write(&env.write_ctx, &write_batch)
        .await
        .expect("should succeed to write");

    check_write_batch(&env, wal, location, seq, &payload_batch).await
}

/// Test the read with different kinds of boundaries.
async fn read_with_boundary<B: WalBuilder>(env: &TestEnv<B>) {
    let wal = env.build_wal().await;
    let location = WalLocation {
        region_id: DEFAULT_SHARD_ID as u64,
        table_id: TableId::MIN,
    };
    let (payload_batch, write_batch) = env.build_log_batch(location, 0, 10).await;
    let end_seq = wal
        .write(&env.write_ctx, &write_batch)
        .await
        .expect("should succeed to write");

    let last_seq = wal.sequence_num(location).await.unwrap();
    assert_eq!(end_seq, last_seq);

    let start_seq = end_seq + 1 - write_batch.entries.len() as u64;

    // [min, max]
    {
        let read_req = ReadRequest {
            location,
            start: ReadBoundary::Min,
            end: ReadBoundary::Max,
        };
        check_write_batch_with_read_request(env, wal.clone(), read_req, end_seq, &payload_batch)
            .await;
    }

    // [0, 10]
    {
        let read_req = ReadRequest {
            location,
            start: ReadBoundary::Included(start_seq),
            end: ReadBoundary::Included(end_seq),
        };
        check_write_batch_with_read_request(env, wal.clone(), read_req, end_seq, &payload_batch)
            .await;
    }

    // (0, 10]
    {
        let read_req = ReadRequest {
            location,
            start: ReadBoundary::Excluded(start_seq),
            end: ReadBoundary::Included(end_seq),
        };
        let payload_batch = env.build_payload_batch(1, 10);
        check_write_batch_with_read_request(env, wal.clone(), read_req, end_seq, &payload_batch)
            .await;
    }

    // [0, 10)
    {
        let read_req = ReadRequest {
            location,
            start: ReadBoundary::Included(start_seq),
            end: ReadBoundary::Excluded(end_seq),
        };
        let payload_batch = env.build_payload_batch(0, 9);
        check_write_batch_with_read_request(
            env,
            wal.clone(),
            read_req,
            end_seq - 1,
            &payload_batch,
        )
        .await;
    }

    // (0, 10)
    {
        let read_req = ReadRequest {
            location,
            start: ReadBoundary::Excluded(start_seq),
            end: ReadBoundary::Excluded(end_seq),
        };
        let payload_batch = env.build_payload_batch(1, 9);
        check_write_batch_with_read_request(
            env,
            wal.clone(),
            read_req,
            end_seq - 1,
            &payload_batch,
        )
        .await;
    }

    wal.close_gracefully().await.unwrap();
}

/// Test read and write across multiple regions parallely.
async fn write_multiple_regions_parallelly<B: WalBuilder + 'static>(env: Arc<TestEnv<B>>) {
    let wal = env.build_wal().await;
    let mut handles = Vec::with_capacity(10);
    for i in 0..5 {
        let read_write_0 = env.runtime.spawn(simple_read_write_with_wal(
            env.clone(),
            wal.clone(),
            WalLocation::new(DEFAULT_SHARD_ID as u64, i),
        ));
        let read_write_1 = env.runtime.spawn(simple_read_write_with_wal(
            env.clone(),
            wal.clone(),
            WalLocation::new(DEFAULT_SHARD_ID as u64, i),
        ));
        handles.push(read_write_0);
        handles.push(read_write_1);
    }

    for handle in handles {
        handle.await.expect("should succeed to join the write")
    }

    wal.close_gracefully().await.unwrap();
}

/// Test whether the written logs can be read after reopen.
async fn reopen<B: WalBuilder>(env: &TestEnv<B>, result_len: usize) {
    let mut write_results = Vec::with_capacity(result_len);
    // Write logs.
    {
        let wal = env.build_wal().await;
        for result_idx in 0..result_len {
            let region_id = result_idx as u64;
            let table_id = result_idx as u64;
            let (payload_batch, write_batch) = env
                .build_log_batch(WalLocation::new(region_id, table_id), 0, 10)
                .await;
            let seq = wal
                .write(&env.write_ctx, &write_batch)
                .await
                .expect("should succeed to write");

            let last_seq = wal
                .sequence_num(WalLocation::new(region_id, table_id))
                .await
                .unwrap();
            assert_eq!(seq, last_seq);

            write_results.push((region_id, table_id, payload_batch, write_batch, seq));
        }
        wal.close_gracefully().await.unwrap();
    }

    // Reopen the wal.
    let wal = env.build_wal().await;

    for (region_id, table_id, payload_batch, write_batch, seq) in write_results {
        let read_req = ReadRequest {
            location: WalLocation::new(region_id, table_id),
            start: ReadBoundary::Included(seq + 1 - write_batch.entries.len() as u64),
            end: ReadBoundary::Included(seq),
        };
        let iter = wal
            .read_batch(&env.read_ctx, &read_req)
            .await
            .expect("should succeed to read");

        let test_table_data = TestTableData::new(table_id, payload_batch, seq);
        env.check_log_entries(vec![test_table_data], iter).await;
    }

    wal.close_gracefully().await.unwrap();
}

/// A complex test case for read and write:
///  - Write two log batch
///  - Read the first batch and then read the second batch.
///  - Read the whole batch.
///  - Read the part of first batch and second batch.
async fn complex_read_write<B: WalBuilder>(env: &TestEnv<B>) {
    let wal = env.build_wal().await;
    let table_id = 0;

    // write two batches
    let (start_val, mid_val, end_val) = (0, 10, 50);
    let (payload_batch1, write_batch_1) = env
        .build_log_batch(
            WalLocation::new(DEFAULT_SHARD_ID as u64, table_id),
            start_val,
            mid_val,
        )
        .await;
    let seq_1 = wal
        .write(&env.write_ctx, &write_batch_1)
        .await
        .expect("should succeed to write");
    let (payload_batch2, write_batch_2) = env
        .build_log_batch(
            WalLocation::new(DEFAULT_SHARD_ID as u64, table_id),
            mid_val,
            end_val,
        )
        .await;
    let seq_2 = wal
        .write(&env.write_ctx, &write_batch_2)
        .await
        .expect("should succeed to write");

    // read the first batch
    check_write_batch(
        env,
        wal.clone(),
        WalLocation::new(DEFAULT_SHARD_ID as u64, table_id),
        seq_1,
        &payload_batch1,
    )
    .await;
    // read the second batch
    check_write_batch(
        env,
        wal.clone(),
        WalLocation::new(DEFAULT_SHARD_ID as u64, table_id),
        seq_2,
        &payload_batch2,
    )
    .await;

    // read the whole batch
    let (seq_3, payload_batch3) = (seq_2, env.build_payload_batch(start_val, end_val));
    check_write_batch(
        env,
        wal.clone(),
        WalLocation::new(DEFAULT_SHARD_ID as u64, table_id),
        seq_3,
        &payload_batch3,
    )
    .await;

    // read the part of batch1 and batch2
    let (seq_4, payload_batch4) = {
        let new_start = (start_val + mid_val) / 2;
        let new_end = (mid_val + end_val) / 2;
        let seq = seq_2 - (end_val - new_end) as u64;
        (seq, env.build_payload_batch(new_start, new_end))
    };
    check_write_batch(
        env,
        wal.clone(),
        WalLocation::new(DEFAULT_SHARD_ID as u64, table_id),
        seq_4,
        &payload_batch4,
    )
    .await;

    wal.close_gracefully().await.unwrap();
}

/// Test whether data can be deleted.
async fn simple_write_delete<B: WalBuilder>(env: &TestEnv<B>) {
    let table_id = 0;
    let wal = env.build_wal().await;
    let (payload_batch, write_batch) = env
        .build_log_batch(WalLocation::new(DEFAULT_SHARD_ID as u64, table_id), 0, 10)
        .await;
    let seq = wal
        .write(&env.write_ctx, &write_batch)
        .await
        .expect("should succeed to write");
    check_write_batch(
        env,
        wal.clone(),
        WalLocation::new(DEFAULT_SHARD_ID as u64, table_id),
        seq,
        &payload_batch,
    )
    .await;

    let last_seq = wal
        .sequence_num(WalLocation::new(DEFAULT_SHARD_ID as u64, table_id))
        .await
        .unwrap();
    assert_eq!(seq, last_seq);

    // delete all logs
    wal.mark_delete_entries_up_to(WalLocation::new(DEFAULT_SHARD_ID as u64, table_id), seq)
        .await
        .expect("should succeed to delete");
    let read_req = ReadRequest {
        location: WalLocation::new(DEFAULT_SHARD_ID as u64, table_id),
        start: ReadBoundary::Min,
        end: ReadBoundary::Max,
    };
    let iter = wal
        .read_batch(&env.read_ctx, &read_req)
        .await
        .expect("should succeed to read");

    let test_table_data = TestTableData::new(table_id, Vec::new(), seq);
    env.check_log_entries(vec![test_table_data], iter).await;

    // Sequence num remains unchanged.
    let last_seq = wal
        .sequence_num(WalLocation::new(DEFAULT_SHARD_ID as u64, table_id))
        .await
        .unwrap();
    assert_eq!(seq, last_seq);

    wal.close_gracefully().await.unwrap();
}

/// Delete half of the written data and check the remaining half can be read.
async fn write_delete_half<B: WalBuilder>(env: &TestEnv<B>) {
    let table_id = 0;
    let wal = env.build_wal().await;
    let (mut payload_batch, write_batch) = env
        .build_log_batch(WalLocation::new(DEFAULT_SHARD_ID as u64, table_id), 0, 10)
        .await;
    let seq = wal
        .write(&env.write_ctx, &write_batch)
        .await
        .expect("should succeed to write");
    check_write_batch(
        env,
        wal.clone(),
        WalLocation::new(DEFAULT_SHARD_ID as u64, table_id),
        seq,
        &payload_batch,
    )
    .await;

    // delete all logs
    wal.mark_delete_entries_up_to(WalLocation::new(DEFAULT_SHARD_ID as u64, table_id), seq / 2)
        .await
        .expect("should succeed to delete");
    let read_req = ReadRequest {
        location: WalLocation::new(DEFAULT_SHARD_ID as u64, table_id),
        start: ReadBoundary::Min,
        end: ReadBoundary::Max,
    };
    let iter = wal
        .read_batch(&env.read_ctx, &read_req)
        .await
        .expect("should succeed to read");
    // write_batch.entries.drain(..write_batch.entries.len() / 2);
    payload_batch.drain(..write_batch.entries.len() / 2);
    let test_table_data = TestTableData::new(table_id, payload_batch.to_vec(), seq);
    env.check_log_entries(vec![test_table_data], iter).await;

    // Sequence num remains unchanged.
    let last_seq = wal
        .sequence_num(WalLocation::new(DEFAULT_SHARD_ID as u64, table_id))
        .await
        .unwrap();
    assert_eq!(seq, last_seq);

    wal.close_gracefully().await.unwrap();
}

/// Test delete across multiple regions.
async fn write_delete_multiple_regions<B: WalBuilder>(env: &TestEnv<B>) {
    let (table_id_1, table_id_2) = (1, 2);
    let wal = env.build_wal().await;
    let (_, write_batch_1) = env
        .build_log_batch(WalLocation::new(DEFAULT_SHARD_ID as u64, table_id_1), 0, 10)
        .await;
    let seq_1 = wal
        .write(&env.write_ctx, &write_batch_1)
        .await
        .expect("should succeed to write");

    let (payload_batch2, write_batch_2) = env
        .build_log_batch(
            WalLocation::new(DEFAULT_SHARD_ID as u64, table_id_2),
            10,
            20,
        )
        .await;
    let seq_2 = wal
        .write(&env.write_ctx, &write_batch_2)
        .await
        .expect("should succeed to write");

    // delete all logs of region 1.
    wal.mark_delete_entries_up_to(WalLocation::new(DEFAULT_SHARD_ID as u64, table_id_1), seq_1)
        .await
        .expect("should succeed to delete");
    let read_req = ReadRequest {
        location: WalLocation::new(DEFAULT_SHARD_ID as u64, table_id_1),
        start: ReadBoundary::Min,
        end: ReadBoundary::Max,
    };
    let iter = wal
        .read_batch(&env.read_ctx, &read_req)
        .await
        .expect("should succeed to read");
    let test_table_data_1 = TestTableData::new(table_id_1, Vec::new(), seq_1);
    env.check_log_entries(vec![test_table_data_1], iter).await;

    check_write_batch(
        env,
        wal.clone(),
        WalLocation::new(DEFAULT_SHARD_ID as u64, table_id_2),
        seq_2,
        &payload_batch2,
    )
    .await;

    wal.close_gracefully().await.unwrap();
}

/// The sequence number should increase monotonically after multiple writes.
async fn sequence_increase_monotonically_multiple_writes<B: WalBuilder>(env: &TestEnv<B>) {
    let table_id = 0;
    let wal = env.build_wal().await;
    let (_, write_batch1) = env
        .build_log_batch(WalLocation::new(DEFAULT_SHARD_ID as u64, table_id), 0, 10)
        .await;
    let seq_1 = wal
        .write(&env.write_ctx, &write_batch1)
        .await
        .expect("should succeed to write");
    let (_, write_batch2) = env
        .build_log_batch(WalLocation::new(DEFAULT_SHARD_ID as u64, table_id), 0, 10)
        .await;
    let seq_2 = wal
        .write(&env.write_ctx, &write_batch2)
        .await
        .expect("should succeed to write");
    let (_, write_batch3) = env
        .build_log_batch(WalLocation::new(DEFAULT_SHARD_ID as u64, table_id), 0, 10)
        .await;

    let seq_3 = wal
        .write(&env.write_ctx, &write_batch3)
        .await
        .expect("should succeed to write");

    assert!(seq_2 > seq_1);
    assert!(seq_3 > seq_2);

    wal.close_gracefully().await.unwrap();
}

/// The sequence number should increase monotonically after write, delete and
/// one more write.
async fn sequence_increase_monotonically_delete_write<B: WalBuilder>(env: &TestEnv<B>) {
    let table_id = 0;
    let wal = env.build_wal().await;
    let (_, write_batch1) = env
        .build_log_batch(WalLocation::new(DEFAULT_SHARD_ID as u64, table_id), 0, 10)
        .await;
    // write
    let seq_1 = wal
        .write(&env.write_ctx, &write_batch1)
        .await
        .expect("should succeed to write");
    // delete
    wal.mark_delete_entries_up_to(WalLocation::new(DEFAULT_SHARD_ID as u64, table_id), seq_1)
        .await
        .expect("should succeed to delete");
    let (_, write_batch2) = env
        .build_log_batch(WalLocation::new(DEFAULT_SHARD_ID as u64, table_id), 0, 10)
        .await;
    // write again
    let seq_2 = wal
        .write(&env.write_ctx, &write_batch2)
        .await
        .expect("should succeed to write");

    let last_seq = wal
        .sequence_num(WalLocation::new(DEFAULT_SHARD_ID as u64, table_id))
        .await
        .unwrap();
    assert_eq!(seq_2, last_seq);

    assert!(seq_2 > seq_1);

    wal.close_gracefully().await.unwrap();
}

/// The sequence number should increase monotonically after write, delete,
/// reopen and write.
async fn sequence_increase_monotonically_delete_reopen_write<B: WalBuilder>(env: &TestEnv<B>) {
    let table_id = 0;
    let wal = env.build_wal().await;
    let (_, write_batch1) = env
        .build_log_batch(WalLocation::new(DEFAULT_SHARD_ID as u64, table_id), 0, 10)
        .await;
    // write
    let seq_1 = wal
        .write(&env.write_ctx, &write_batch1)
        .await
        .expect("should succeed to write");
    // delete
    wal.mark_delete_entries_up_to(WalLocation::new(DEFAULT_SHARD_ID as u64, table_id), seq_1)
        .await
        .expect("should succeed to delete");

    // restart
    wal.close_gracefully().await.unwrap();
    drop(wal);

    let wal = env.build_wal().await;
    // write again
    let (_, write_batch2) = env
        .build_log_batch(WalLocation::new(DEFAULT_SHARD_ID as u64, table_id), 0, 10)
        .await;
    let seq_2 = wal
        .write(&env.write_ctx, &write_batch2)
        .await
        .expect("should succeed to write");

    let last_seq = wal
        .sequence_num(WalLocation::new(DEFAULT_SHARD_ID as u64, table_id))
        .await
        .unwrap();
    assert_eq!(seq_2, last_seq);

    assert!(seq_2 > seq_1);

    wal.close_gracefully().await.unwrap();
}

async fn write_scan<B: WalBuilder>(env: &TestEnv<B>) {
    let table_id_1 = 0;
    let table_id_2 = 1;

    let wal = env.build_wal().await;
    // Write table 0.
    let (payload_batch1, write_batch1) = env
        .build_log_batch(WalLocation::new(DEFAULT_SHARD_ID as u64, table_id_1), 0, 10)
        .await;

    let seq_1 = wal
        .write(&env.write_ctx, &write_batch1)
        .await
        .expect("should succeed to write");

    // Write table 1.
    let (payload_batch2, write_batch2) = env
        .build_log_batch(WalLocation::new(DEFAULT_SHARD_ID as u64, table_id_2), 0, 10)
        .await;

    let seq_2 = wal
        .write(&env.write_ctx, &write_batch2)
        .await
        .expect("should succeed to write");

    // Scan and compare.
    let scan_request = ScanRequest {
        region_id: DEFAULT_SHARD_ID as u64,
    };
    let iter = wal
        .scan(&env.read_ctx, &scan_request)
        .await
        .expect("should succeed to read");

    let test_table_data_1 = TestTableData::new(table_id_1, payload_batch1, seq_1);
    let test_table_data_2 = TestTableData::new(table_id_2, payload_batch2, seq_2);
    env.check_log_entries(vec![test_table_data_1, test_table_data_2], iter)
        .await;
}

#[async_trait]
pub trait WalBuilder: Clone + Send + Sync + 'static {
    type Wal: WalManager + Send + Sync;

    async fn build(&self, data_path: &Path, runtime: Arc<Runtime>) -> Arc<Self::Wal>;
}

#[derive(Clone, Default)]
pub struct RocksWalBuilder;

#[async_trait]
impl WalBuilder for RocksWalBuilder {
    type Wal = RocksImpl;

    async fn build(&self, data_path: &Path, runtime: Arc<Runtime>) -> Arc<Self::Wal> {
        let wal_builder = wal::rocksdb_impl::manager::Builder::new(data_path, runtime);

        Arc::new(
            wal_builder
                .build()
                .expect("should succeed to build rocksimpl wal"),
        )
    }
}

const WAL_NAMESPACE: &str = "wal";

#[derive(Default)]
pub struct MemoryTableWalBuilder {
    table_kv: MemoryImpl,
    ttl: Option<ReadableDuration>,
}

#[async_trait]
impl WalBuilder for MemoryTableWalBuilder {
    type Wal = WalNamespaceImpl<MemoryImpl>;

    async fn build(&self, _data_path: &Path, runtime: Arc<Runtime>) -> Arc<Self::Wal> {
        let config = NamespaceConfig {
            wal_shard_num: 2,
            table_unit_meta_shard_num: 2,
            ttl: self.ttl,
            ..Default::default()
        };

        let wal_runtimes = WalRuntimes {
            read_runtime: runtime.clone(),
            write_runtime: runtime.clone(),
            default_runtime: runtime.clone(),
        };
        let namespace_wal =
            WalNamespaceImpl::open(self.table_kv.clone(), wal_runtimes, WAL_NAMESPACE, config)
                .await
                .unwrap();

        Arc::new(namespace_wal)
    }
}

impl Clone for MemoryTableWalBuilder {
    fn clone(&self) -> Self {
        Self {
            table_kv: MemoryImpl::default(),
            ttl: self.ttl,
        }
    }
}

impl MemoryTableWalBuilder {
    pub fn with_ttl(ttl: &str) -> Self {
        Self {
            table_kv: MemoryImpl::default(),
            ttl: Some(ReadableDuration::from_str(ttl).unwrap()),
        }
    }
}

pub struct KafkaWalBuilder {
    namespace: String,
}

impl KafkaWalBuilder {
    pub fn new() -> Self {
        Self {
            namespace: format!("test-namespace-{}", uuid::Uuid::new_v4()),
        }
    }
}

impl Default for KafkaWalBuilder {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl WalBuilder for KafkaWalBuilder {
    type Wal = MessageQueueImpl<KafkaImpl>;

    async fn build(&self, _data_path: &Path, runtime: Arc<Runtime>) -> Arc<Self::Wal> {
        let mut config = KafkaConfig::default();
        config.client.boost_brokers = Some(vec!["127.0.0.1:9011".to_string()]);
        let kafka_impl = KafkaImpl::new(config).await.unwrap();
        let message_queue_impl = MessageQueueImpl::new(
            self.namespace.clone(),
            kafka_impl,
            runtime.clone(),
            KafkaWalConfig::default(),
        );

        Arc::new(message_queue_impl)
    }
}

impl Clone for KafkaWalBuilder {
    fn clone(&self) -> Self {
        Self {
            namespace: format!("test-namespace-{}", uuid::Uuid::new_v4()),
        }
    }
}

/// The environment for testing wal.
pub struct TestEnv<B> {
    pub dir: TempDir,
    pub runtime: Arc<Runtime>,
    pub write_ctx: WriteContext,
    pub read_ctx: ReadContext,
    /// Builder for a specific wal.
    builder: B,
}

impl<B: WalBuilder> TestEnv<B> {
    pub fn new(num_workers: usize, builder: B) -> Self {
        let runtime = runtime::Builder::default()
            .worker_threads(num_workers)
            .enable_all()
            .build()
            .unwrap();

        Self {
            dir: tempfile::tempdir().unwrap(),
            runtime: Arc::new(runtime),
            write_ctx: WriteContext::default(),
            read_ctx: ReadContext::default(),
            builder,
        }
    }

    pub async fn build_wal(&self) -> WalManagerRef {
        self.builder
            .build(self.dir.path(), self.runtime.clone())
            .await
    }

    pub fn build_payload_batch(&self, start: u32, end: u32) -> Vec<MemoryPayload> {
        (start..end).map(|val| MemoryPayload { val }).collect()
    }

    /// Build the log batch with [MemoryPayload].val range [start, end).
    pub async fn build_log_batch(
        &self,
        location: WalLocation,
        start: u32,
        end: u32,
    ) -> (Vec<MemoryPayload>, LogWriteBatch) {
        let log_entries = start..end;

        let log_batch_encoder = LogBatchEncoder::create(location);
        let log_batch = log_batch_encoder
            .encode_batch(log_entries.map(|v| MemoryPayload { val: v }))
            .expect("should succeed to encode payloads");

        let payload_batch = self.build_payload_batch(start, end);
        (payload_batch, log_batch)
    }

    // pub async fn check_multiple_log_entries

    /// Check whether the log entries from the iterator equals the
    /// `write_batch`.
    pub async fn check_log_entries(
        &self,
        test_table_datas: Vec<TestTableData>,
        mut iter: BatchLogIteratorAdapter,
    ) {
        let mut table_log_entries: HashMap<TableId, VecDeque<_>> =
            HashMap::with_capacity(test_table_datas.len());

        loop {
            let dec = MemoryPayloadDecoder;
            let log_entries = iter
                .next_log_entries(dec, |_| true, VecDeque::new())
                .await
                .expect("should succeed to fetch next log entry");
            if log_entries.is_empty() {
                break;
            }

            for log_entry in log_entries {
                let log_entries = table_log_entries
                    .entry(log_entry.table_id)
                    .or_insert_with(VecDeque::default);
                log_entries.push_back(log_entry);
            }
        }

        for test_table_data in test_table_datas {
            let empty_log_entries = VecDeque::new();
            let log_entries = table_log_entries
                .get(&test_table_data.table_id)
                .unwrap_or(&empty_log_entries);

            assert_eq!(test_table_data.payload_batch.len(), log_entries.len());
            for (idx, (expect_log_write_entry, log_entry)) in test_table_data
                .payload_batch
                .iter()
                .zip(log_entries.iter())
                .rev()
                .enumerate()
            {
                // sequence
                assert_eq!(test_table_data.max_seq - idx as u64, log_entry.sequence);

                // payload
                assert_eq!(expect_log_write_entry, &log_entry.payload);
            }
        }
    }
}

pub struct TestTableData {
    table_id: TableId,
    payload_batch: Vec<MemoryPayload>,
    max_seq: SequenceNumber,
}

impl TestTableData {
    pub fn new(
        table_id: TableId,
        payload_batch: Vec<MemoryPayload>,
        max_seq: SequenceNumber,
    ) -> Self {
        Self {
            table_id,
            payload_batch,
            max_seq,
        }
    }
}
