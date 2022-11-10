// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

use std::{ops::Deref, sync::Arc};

use common_types::{
    table::{Location, TableId, DEFAULT_SHARD_ID},
    SequenceNumber,
};

use crate::{
    manager::{ReadBoundary, ReadRequest, WalManagerRef},
    tests::util::{
        MemoryTableWalBuilder, RocksTestEnv, RocksWalBuilder, TableKvTestEnv, TestEnv, TestPayload,
        WalBuilder,
    },
};

#[test]
fn test_rocksdb_wal() {
    let builder = RocksWalBuilder::default();

    test_all(builder);
}

#[test]
fn test_memory_table_wal_default() {
    let builder = MemoryTableWalBuilder::default();

    test_all(builder);
}

#[test]
fn test_memory_table_wal_with_ttl() {
    let builder = MemoryTableWalBuilder::with_ttl("1d");

    test_all(builder);
}

fn test_all<B: WalBuilder>(builder: B) {
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

    test_sequence_increase_monotonically_delete_reopen_write(builder);
}

fn test_simple_read_write_default_batch<B: WalBuilder>(builder: B) {
    let table_id = 0;
    let env = TestEnv::new(2, builder);
    env.runtime.block_on(simple_read_write(
        &env,
        Location::new(DEFAULT_SHARD_ID, table_id),
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
            Location::new(DEFAULT_SHARD_ID, table_id),
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
    env.runtime.block_on(reopen(&env));
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

async fn check_write_batch_with_read_request<B: WalBuilder>(
    env: &TestEnv<B>,
    wal: WalManagerRef,
    read_req: ReadRequest,
    max_seq: SequenceNumber,
    payload_batch: &[TestPayload],
) {
    let iter = wal
        .read_batch(&env.read_ctx, &read_req)
        .await
        .expect("should succeed to read");
    env.check_log_entries(max_seq, payload_batch, iter).await;
}

async fn check_write_batch<B: WalBuilder>(
    env: &TestEnv<B>,
    wal: WalManagerRef,
    location: Location,
    max_seq: SequenceNumber,
    payload_batch: &[TestPayload],
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
    location: Location,
) {
    let (payload_batch, write_batch) = env.build_log_batch(wal.clone(), location, 0, 10).await;
    let seq = wal
        .write(&env.write_ctx, &write_batch)
        .await
        .expect("should succeed to write");

    check_write_batch(&env, wal, location, seq, &payload_batch).await
}

async fn simple_read_write<B: WalBuilder>(env: &TestEnv<B>, location: Location) {
    let wal = env.build_wal().await;
    // Empty region has 0 sequence num.
    let last_seq = wal.sequence_num(location).await.unwrap();
    assert_eq!(0, last_seq);

    simple_read_write_with_wal(env, wal.clone(), location).await;

    let last_seq = wal.sequence_num(location).await.unwrap();
    assert_eq!(10, last_seq);

    wal.close_gracefully().await.unwrap();
}

/// Test the read with different kinds of boundaries.
async fn read_with_boundary<B: WalBuilder>(env: &TestEnv<B>) {
    let wal = env.build_wal().await;
    let location = Location::new(0, 0);
    let (payload_batch, write_batch) = env.build_log_batch(wal.clone(), location, 0, 10).await;
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
            Location::new(DEFAULT_SHARD_ID, i),
        ));
        let read_write_1 = env.runtime.spawn(simple_read_write_with_wal(
            env.clone(),
            wal.clone(),
            Location::new(DEFAULT_SHARD_ID, i),
        ));
        handles.push(read_write_0);
        handles.push(read_write_1);
    }
    futures::future::join_all(handles)
        .await
        .into_iter()
        .for_each(|res| {
            res.expect("should succeed to join the write");
        });

    wal.close_gracefully().await.unwrap();
}

/// Test whether the written logs can be read after reopen.
async fn reopen<B: WalBuilder>(env: &TestEnv<B>) {
    let table_id = 0;
    let (payload_batch, write_batch, seq) = {
        let wal = env.build_wal().await;
        let (payload_batch, write_batch) = env
            .build_log_batch(
                wal.clone(),
                Location::new(DEFAULT_SHARD_ID, table_id),
                0,
                10,
            )
            .await;
        let seq = wal
            .write(&env.write_ctx, &write_batch)
            .await
            .expect("should succeed to write");

        wal.close_gracefully().await.unwrap();

        (payload_batch, write_batch, seq)
    };

    // reopen the wal
    let wal = env.build_wal().await;

    let last_seq = wal
        .sequence_num(Location::new(DEFAULT_SHARD_ID, table_id))
        .await
        .unwrap();
    assert_eq!(seq, last_seq);

    let read_req = ReadRequest {
        location: Location::new(DEFAULT_SHARD_ID, table_id),
        start: ReadBoundary::Included(seq + 1 - write_batch.entries.len() as u64),
        end: ReadBoundary::Included(seq),
    };
    let iter = wal
        .read_batch(&env.read_ctx, &read_req)
        .await
        .expect("should succeed to read");
    env.check_log_entries(seq, &payload_batch, iter).await;

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
            wal.clone(),
            Location::new(DEFAULT_SHARD_ID, table_id),
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
            wal.clone(),
            Location::new(DEFAULT_SHARD_ID, table_id),
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
        Location::new(DEFAULT_SHARD_ID, table_id),
        seq_1,
        &payload_batch1,
    )
    .await;
    // read the second batch
    check_write_batch(
        env,
        wal.clone(),
        Location::new(DEFAULT_SHARD_ID, table_id),
        seq_2,
        &payload_batch2,
    )
    .await;

    // read the whole batch
    let (seq_3, payload_batch3) = (seq_2, env.build_payload_batch(start_val, end_val));
    check_write_batch(
        env,
        wal.clone(),
        Location::new(DEFAULT_SHARD_ID, table_id),
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
        Location::new(DEFAULT_SHARD_ID, table_id),
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
        .build_log_batch(
            wal.clone(),
            Location::new(DEFAULT_SHARD_ID, table_id),
            0,
            10,
        )
        .await;
    let seq = wal
        .write(&env.write_ctx, &write_batch)
        .await
        .expect("should succeed to write");
    check_write_batch(
        env,
        wal.clone(),
        Location::new(DEFAULT_SHARD_ID, table_id),
        seq,
        &payload_batch,
    )
    .await;

    let last_seq = wal
        .sequence_num(Location::new(DEFAULT_SHARD_ID, table_id))
        .await
        .unwrap();
    assert_eq!(seq, last_seq);

    // delete all logs
    wal.mark_delete_entries_up_to(Location::new(DEFAULT_SHARD_ID, table_id), seq)
        .await
        .expect("should succeed to delete");
    let read_req = ReadRequest {
        location: Location::new(DEFAULT_SHARD_ID, table_id),
        start: ReadBoundary::Min,
        end: ReadBoundary::Max,
    };
    let iter = wal
        .read_batch(&env.read_ctx, &read_req)
        .await
        .expect("should succeed to read");
    env.check_log_entries(seq, &[], iter).await;

    // Sequence num remains unchanged.
    let last_seq = wal
        .sequence_num(Location::new(DEFAULT_SHARD_ID, table_id))
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
        .build_log_batch(
            wal.clone(),
            Location::new(DEFAULT_SHARD_ID, table_id),
            0,
            10,
        )
        .await;
    let seq = wal
        .write(&env.write_ctx, &write_batch)
        .await
        .expect("should succeed to write");
    check_write_batch(
        env,
        wal.clone(),
        Location::new(DEFAULT_SHARD_ID, table_id),
        seq,
        &payload_batch,
    )
    .await;

    // delete all logs
    wal.mark_delete_entries_up_to(Location::new(DEFAULT_SHARD_ID, table_id), seq / 2)
        .await
        .expect("should succeed to delete");
    let read_req = ReadRequest {
        location: Location::new(DEFAULT_SHARD_ID, table_id),
        start: ReadBoundary::Min,
        end: ReadBoundary::Max,
    };
    let iter = wal
        .read_batch(&env.read_ctx, &read_req)
        .await
        .expect("should succeed to read");
    // write_batch.entries.drain(..write_batch.entries.len() / 2);
    payload_batch.drain(..write_batch.entries.len() / 2);
    env.check_log_entries(seq, &payload_batch, iter).await;

    // Sequence num remains unchanged.
    let last_seq = wal
        .sequence_num(Location::new(DEFAULT_SHARD_ID, table_id))
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
        .build_log_batch(
            wal.clone(),
            Location::new(DEFAULT_SHARD_ID, table_id_1),
            0,
            10,
        )
        .await;
    let seq_1 = wal
        .write(&env.write_ctx, &write_batch_1)
        .await
        .expect("should succeed to write");

    let (payload_batch2, write_batch_2) = env
        .build_log_batch(
            wal.clone(),
            Location::new(DEFAULT_SHARD_ID, table_id_2),
            10,
            20,
        )
        .await;
    let seq_2 = wal
        .write(&env.write_ctx, &write_batch_2)
        .await
        .expect("should succeed to write");

    // delete all logs of region 1.
    wal.mark_delete_entries_up_to(Location::new(DEFAULT_SHARD_ID, table_id_1), seq_1)
        .await
        .expect("should succeed to delete");
    let read_req = ReadRequest {
        location: Location::new(DEFAULT_SHARD_ID, table_id_1),
        start: ReadBoundary::Min,
        end: ReadBoundary::Max,
    };
    let iter = wal
        .read_batch(&env.read_ctx, &read_req)
        .await
        .expect("should succeed to read");
    env.check_log_entries(seq_1, &[], iter).await;

    check_write_batch(
        env,
        wal.clone(),
        Location::new(DEFAULT_SHARD_ID, table_id_2),
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
        .build_log_batch(
            wal.clone(),
            Location::new(DEFAULT_SHARD_ID, table_id),
            0,
            10,
        )
        .await;
    let seq_1 = wal
        .write(&env.write_ctx, &write_batch1)
        .await
        .expect("should succeed to write");
    let (_, write_batch2) = env
        .build_log_batch(
            wal.clone(),
            Location::new(DEFAULT_SHARD_ID, table_id),
            0,
            10,
        )
        .await;
    let seq_2 = wal
        .write(&env.write_ctx, &write_batch2)
        .await
        .expect("should succeed to write");
    let (_, write_batch3) = env
        .build_log_batch(
            wal.clone(),
            Location::new(DEFAULT_SHARD_ID, table_id),
            0,
            10,
        )
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
        .build_log_batch(
            wal.clone(),
            Location::new(DEFAULT_SHARD_ID, table_id),
            0,
            10,
        )
        .await;
    // write
    let seq_1 = wal
        .write(&env.write_ctx, &write_batch1)
        .await
        .expect("should succeed to write");
    // delete
    wal.mark_delete_entries_up_to(Location::new(DEFAULT_SHARD_ID, table_id), seq_1)
        .await
        .expect("should succeed to delete");
    let (_, write_batch2) = env
        .build_log_batch(
            wal.clone(),
            Location::new(DEFAULT_SHARD_ID, table_id),
            0,
            10,
        )
        .await;
    // write again
    let seq_2 = wal
        .write(&env.write_ctx, &write_batch2)
        .await
        .expect("should succeed to write");

    let last_seq = wal
        .sequence_num(Location::new(DEFAULT_SHARD_ID, table_id))
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
        .build_log_batch(
            wal.clone(),
            Location::new(DEFAULT_SHARD_ID, table_id),
            0,
            10,
        )
        .await;
    // write
    let seq_1 = wal
        .write(&env.write_ctx, &write_batch1)
        .await
        .expect("should succeed to write");
    // delete
    wal.mark_delete_entries_up_to(Location::new(DEFAULT_SHARD_ID, table_id), seq_1)
        .await
        .expect("should succeed to delete");

    // restart
    wal.close_gracefully().await.unwrap();
    drop(wal);

    let wal = env.build_wal().await;
    // write again
    let (_, write_batch2) = env
        .build_log_batch(
            wal.clone(),
            Location::new(DEFAULT_SHARD_ID, table_id),
            0,
            10,
        )
        .await;
    let seq_2 = wal
        .write(&env.write_ctx, &write_batch2)
        .await
        .expect("should succeed to write");

    let last_seq = wal
        .sequence_num(Location::new(DEFAULT_SHARD_ID, table_id))
        .await
        .unwrap();
    assert_eq!(seq_2, last_seq);

    assert!(seq_2 > seq_1);

    wal.close_gracefully().await.unwrap();
}
