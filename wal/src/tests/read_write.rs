// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

use std::{ops::Deref, sync::Arc};

use common_types::SequenceNumber;

use crate::{
    log_batch::LogWriteBatch,
    manager::{LogReader, LogWriter, ReadBoundary, ReadRequest, RegionId, WalManager},
    tests::util::{
        MemoryTableWalBuilder, RocksTestEnv, RocksWalBuilder, TableKvTestEnv, TestEnv, TestPayload,
        WalBuilder,
    },
};

async fn check_write_batch_with_read_request<B: WalBuilder>(
    env: &TestEnv<B>,
    wal: Arc<B::Wal>,
    read_req: ReadRequest,
    max_seq: SequenceNumber,
    write_batch: &LogWriteBatch<TestPayload>,
) {
    let iter = wal
        .read_batch(&env.read_ctx, &read_req)
        .await
        .expect("should succeed to read");
    env.check_log_entries(max_seq, write_batch, iter).await;
}

async fn check_write_batch<B: WalBuilder>(
    env: &TestEnv<B>,
    wal: Arc<B::Wal>,
    region_id: RegionId,
    max_seq: SequenceNumber,
    write_batch: &LogWriteBatch<TestPayload>,
) {
    let read_req = ReadRequest {
        region_id,
        start: ReadBoundary::Included(max_seq + 1 - write_batch.entries.len() as u64),
        end: ReadBoundary::Included(max_seq),
    };
    check_write_batch_with_read_request(env, wal, read_req, max_seq, write_batch).await
}

async fn simple_read_write_with_wal<B: WalBuilder>(
    env: impl Deref<Target = TestEnv<B>>,
    wal: Arc<B::Wal>,
    region_id: RegionId,
) {
    let write_batch = env.build_log_batch(region_id, 0, 10);
    let seq = wal
        .write(&env.write_ctx, &write_batch)
        .await
        .expect("should succeed to write");

    check_write_batch(&env, wal, region_id, seq, &write_batch).await
}

async fn simple_read_write<B: WalBuilder>(env: &TestEnv<B>, region_id: RegionId) {
    let wal = env.build_wal().await;
    // Empty region has 0 sequence num.
    let last_seq = wal.sequence_num(region_id).await.unwrap();
    assert_eq!(0, last_seq);

    simple_read_write_with_wal(env, wal.clone(), region_id).await;

    let last_seq = wal.sequence_num(region_id).await.unwrap();
    assert_eq!(10, last_seq);

    wal.close_gracefully().await.unwrap();
}

/// Test the read with different kinds of boundaries.
async fn read_with_boundary<B: WalBuilder>(env: &TestEnv<B>) {
    let wal = env.build_wal().await;
    let region_id = 0;
    let write_batch = env.build_log_batch(region_id, 0, 10);
    let end_seq = wal
        .write(&env.write_ctx, &write_batch)
        .await
        .expect("should succeed to write");

    let last_seq = wal.sequence_num(region_id).await.unwrap();
    assert_eq!(end_seq, last_seq);

    let start_seq = end_seq + 1 - write_batch.entries.len() as u64;

    // [min, max]
    {
        let read_req = ReadRequest {
            region_id,
            start: ReadBoundary::Min,
            end: ReadBoundary::Max,
        };
        check_write_batch_with_read_request(env, wal.clone(), read_req, end_seq, &write_batch)
            .await;
    }

    // [0, 10]
    {
        let read_req = ReadRequest {
            region_id,
            start: ReadBoundary::Included(start_seq),
            end: ReadBoundary::Included(end_seq),
        };
        check_write_batch_with_read_request(env, wal.clone(), read_req, end_seq, &write_batch)
            .await;
    }

    // (0, 10]
    {
        let read_req = ReadRequest {
            region_id,
            start: ReadBoundary::Excluded(start_seq),
            end: ReadBoundary::Included(end_seq),
        };
        let write_batch = env.build_log_batch(region_id, 1, 10);
        check_write_batch_with_read_request(env, wal.clone(), read_req, end_seq, &write_batch)
            .await;
    }

    // [0, 10)
    {
        let read_req = ReadRequest {
            region_id,
            start: ReadBoundary::Included(start_seq),
            end: ReadBoundary::Excluded(end_seq),
        };
        let write_batch = env.build_log_batch(region_id, 0, 9);
        check_write_batch_with_read_request(env, wal.clone(), read_req, end_seq - 1, &write_batch)
            .await;
    }

    // (0, 10)
    {
        let read_req = ReadRequest {
            region_id,
            start: ReadBoundary::Excluded(start_seq),
            end: ReadBoundary::Excluded(end_seq),
        };
        let write_batch = env.build_log_batch(region_id, 1, 9);
        check_write_batch_with_read_request(env, wal.clone(), read_req, end_seq - 1, &write_batch)
            .await;
    }

    wal.close_gracefully().await.unwrap();
}

/// Test read and write across multiple regions parallely.
async fn write_multiple_regions_parallelly<B: WalBuilder + 'static>(env: Arc<TestEnv<B>>) {
    let wal = env.build_wal().await;
    let mut handles = Vec::with_capacity(10);
    for i in 0..5 {
        let read_write_0 =
            env.runtime
                .spawn(simple_read_write_with_wal(env.clone(), wal.clone(), i));
        let read_write_1 =
            env.runtime
                .spawn(simple_read_write_with_wal(env.clone(), wal.clone(), i));
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
    let region_id = 0;
    let (write_batch, seq) = {
        let wal = env.build_wal().await;
        let write_batch = env.build_log_batch(region_id, 0, 10);
        let seq = wal
            .write(&env.write_ctx, &write_batch)
            .await
            .expect("should succeed to write");

        wal.close_gracefully().await.unwrap();

        (write_batch, seq)
    };

    // reopen the wal
    let wal = env.build_wal().await;

    let last_seq = wal.sequence_num(region_id).await.unwrap();
    assert_eq!(seq, last_seq);

    let read_req = ReadRequest {
        region_id,
        start: ReadBoundary::Included(seq + 1 - write_batch.entries.len() as u64),
        end: ReadBoundary::Included(seq),
    };
    let iter = wal
        .read_batch(&env.read_ctx, &read_req)
        .await
        .expect("should succeed to read");
    env.check_log_entries(seq, &write_batch, iter).await;

    wal.close_gracefully().await.unwrap();
}

/// A complex test case for read and write:
///  - Write two log batch
///  - Read the first batch and then read the second batch.
///  - Read the whole batch.
///  - Read the part of first batch and second batch.
async fn complex_read_write<B: WalBuilder>(env: &TestEnv<B>) {
    let wal = env.build_wal().await;
    let region_id = 0;

    // write two batches
    let (start_val, mid_val, end_val) = (0, 10, 50);
    let write_batch_1 = env.build_log_batch(region_id, start_val, mid_val);
    let seq_1 = wal
        .write(&env.write_ctx, &write_batch_1)
        .await
        .expect("should succeed to write");
    let write_batch_2 = env.build_log_batch(region_id, mid_val, end_val);
    let seq_2 = wal
        .write(&env.write_ctx, &write_batch_2)
        .await
        .expect("should succeed to write");

    // read the first batch
    check_write_batch(env, wal.clone(), region_id, seq_1, &write_batch_1).await;
    // read the second batch
    check_write_batch(env, wal.clone(), region_id, seq_2, &write_batch_2).await;

    // read the whole batch
    let (seq_3, write_batch_3) = (seq_2, env.build_log_batch(region_id, start_val, end_val));
    check_write_batch(env, wal.clone(), region_id, seq_3, &write_batch_3).await;

    // read the part of batch1 and batch2
    let (seq_4, write_batch_4) = {
        let new_start = (start_val + mid_val) / 2;
        let new_end = (mid_val + end_val) / 2;
        let seq = seq_2 - (end_val - new_end) as u64;
        (seq, env.build_log_batch(region_id, new_start, new_end))
    };
    check_write_batch(env, wal.clone(), region_id, seq_4, &write_batch_4).await;

    wal.close_gracefully().await.unwrap();
}

/// Test whether data can be deleted.
async fn simple_write_delete<B: WalBuilder>(env: &TestEnv<B>) {
    let region_id = 0;
    let wal = env.build_wal().await;
    let mut write_batch = env.build_log_batch(region_id, 0, 10);
    let seq = wal
        .write(&env.write_ctx, &write_batch)
        .await
        .expect("should succeed to write");
    check_write_batch(env, wal.clone(), region_id, seq, &write_batch).await;

    let last_seq = wal.sequence_num(region_id).await.unwrap();
    assert_eq!(seq, last_seq);

    // delete all logs
    wal.mark_delete_entries_up_to(region_id, seq)
        .await
        .expect("should succeed to delete");
    let read_req = ReadRequest {
        region_id,
        start: ReadBoundary::Min,
        end: ReadBoundary::Max,
    };
    let iter = wal
        .read_batch(&env.read_ctx, &read_req)
        .await
        .expect("should succeed to read");
    write_batch.entries.clear();
    env.check_log_entries(seq, &write_batch, iter).await;

    // Sequence num remains unchanged.
    let last_seq = wal.sequence_num(region_id).await.unwrap();
    assert_eq!(seq, last_seq);

    wal.close_gracefully().await.unwrap();
}

/// Delete half of the written data and check the remaining half can be read.
async fn write_delete_half<B: WalBuilder>(env: &TestEnv<B>) {
    let region_id = 0;
    let wal = env.build_wal().await;
    let mut write_batch = env.build_log_batch(region_id, 0, 10);
    let seq = wal
        .write(&env.write_ctx, &write_batch)
        .await
        .expect("should succeed to write");
    check_write_batch(env, wal.clone(), region_id, seq, &write_batch).await;

    // delete all logs
    wal.mark_delete_entries_up_to(region_id, seq / 2)
        .await
        .expect("should succeed to delete");
    let read_req = ReadRequest {
        region_id,
        start: ReadBoundary::Min,
        end: ReadBoundary::Max,
    };
    let iter = wal
        .read_batch(&env.read_ctx, &read_req)
        .await
        .expect("should succeed to read");
    write_batch.entries.drain(..write_batch.entries.len() / 2);
    env.check_log_entries(seq, &write_batch, iter).await;

    // Sequence num remains unchanged.
    let last_seq = wal.sequence_num(region_id).await.unwrap();
    assert_eq!(seq, last_seq);

    wal.close_gracefully().await.unwrap();
}

/// Test delete across multiple regions.
async fn write_delete_multiple_regions<B: WalBuilder>(env: &TestEnv<B>) {
    let (region_id_1, region_id_2) = (1, 2);
    let wal = env.build_wal().await;
    let mut write_batch_1 = env.build_log_batch(region_id_1, 0, 10);
    let seq_1 = wal
        .write(&env.write_ctx, &write_batch_1)
        .await
        .expect("should succeed to write");

    let write_batch_2 = env.build_log_batch(region_id_2, 10, 20);
    let seq_2 = wal
        .write(&env.write_ctx, &write_batch_2)
        .await
        .expect("should succeed to write");

    // delete all logs of region 1.
    wal.mark_delete_entries_up_to(region_id_1, seq_1)
        .await
        .expect("should succeed to delete");
    let read_req = ReadRequest {
        region_id: region_id_1,
        start: ReadBoundary::Min,
        end: ReadBoundary::Max,
    };
    let iter = wal
        .read_batch(&env.read_ctx, &read_req)
        .await
        .expect("should succeed to read");
    write_batch_1.entries.clear();
    env.check_log_entries(seq_1, &write_batch_1, iter).await;

    check_write_batch(env, wal.clone(), region_id_2, seq_2, &write_batch_2).await;

    wal.close_gracefully().await.unwrap();
}

/// The sequence number should increase monotonically after multiple writes.
async fn sequence_increase_monotonically_multiple_writes<B: WalBuilder>(env: &TestEnv<B>) {
    let region_id = 0;
    let wal = env.build_wal().await;
    let write_batch = env.build_log_batch(region_id, 0, 10);
    let seq_1 = wal
        .write(&env.write_ctx, &write_batch)
        .await
        .expect("should succeed to write");
    let seq_2 = wal
        .write(&env.write_ctx, &write_batch)
        .await
        .expect("should succeed to write");
    let seq_3 = wal
        .write(&env.write_ctx, &write_batch)
        .await
        .expect("should succeed to write");

    assert!(seq_2 > seq_1);
    assert!(seq_3 > seq_2);

    wal.close_gracefully().await.unwrap();
}

/// The sequence number should increase monotonically after write, delete and
/// one more write.
async fn sequence_increase_monotonically_delete_write<B: WalBuilder>(env: &TestEnv<B>) {
    let region_id = 0;
    let wal = env.build_wal().await;
    let write_batch = env.build_log_batch(region_id, 0, 10);
    // write
    let seq_1 = wal
        .write(&env.write_ctx, &write_batch)
        .await
        .expect("should succeed to write");
    // delete
    wal.mark_delete_entries_up_to(region_id, seq_1)
        .await
        .expect("should succeed to delete");
    // write again
    let seq_2 = wal
        .write(&env.write_ctx, &write_batch)
        .await
        .expect("should succeed to write");

    let last_seq = wal.sequence_num(region_id).await.unwrap();
    assert_eq!(seq_2, last_seq);

    assert!(seq_2 > seq_1);

    wal.close_gracefully().await.unwrap();
}

/// The sequence number should increase monotonically after write, delete,
/// reopen and write.
async fn sequence_increase_monotonically_delete_reopen_write<B: WalBuilder>(env: &TestEnv<B>) {
    let region_id = 0;
    let wal = env.build_wal().await;
    let write_batch = env.build_log_batch(region_id, 0, 10);
    // write
    let seq_1 = wal
        .write(&env.write_ctx, &write_batch)
        .await
        .expect("should succeed to write");
    // delete
    wal.mark_delete_entries_up_to(region_id, seq_1)
        .await
        .expect("should succeed to delete");

    // restart
    wal.close_gracefully().await.unwrap();
    drop(wal);

    let wal = env.build_wal().await;
    // write again
    let seq_2 = wal
        .write(&env.write_ctx, &write_batch)
        .await
        .expect("should succeed to write");

    let last_seq = wal.sequence_num(region_id).await.unwrap();
    assert_eq!(seq_2, last_seq);

    assert!(seq_2 > seq_1);

    wal.close_gracefully().await.unwrap();
}

#[test]
fn test_simple_read_write_default_batch() {
    let env = RocksTestEnv::new(2, RocksWalBuilder::default());
    env.runtime.block_on(simple_read_write(&env, 0));

    let env = TableKvTestEnv::new(2, MemoryTableWalBuilder::default());
    env.runtime.block_on(simple_read_write(&env, 0));

    let env = TableKvTestEnv::new(2, MemoryTableWalBuilder::with_ttl("1d"));
    env.runtime.block_on(simple_read_write(&env, 0));
}

#[test]
fn test_simple_read_write_different_batch_size() {
    let batch_sizes = [1, 2, 4, 10, 100];

    for batch_size in batch_sizes {
        let mut env = RocksTestEnv::new(2, RocksWalBuilder::default());
        env.read_ctx.batch_size = batch_size;
        env.runtime.block_on(simple_read_write(&env, 0));

        let mut env = TableKvTestEnv::new(2, MemoryTableWalBuilder::default());
        env.read_ctx.batch_size = batch_size;
        env.runtime.block_on(simple_read_write(&env, 0));

        let mut env = TableKvTestEnv::new(2, MemoryTableWalBuilder::with_ttl("1d"));
        env.read_ctx.batch_size = batch_size;
        env.runtime.block_on(simple_read_write(&env, 0));
    }
}

#[test]
fn test_read_with_boundary() {
    let env = RocksTestEnv::new(2, RocksWalBuilder::default());
    env.runtime.block_on(read_with_boundary(&env));

    let env = TableKvTestEnv::new(2, MemoryTableWalBuilder::default());
    env.runtime.block_on(read_with_boundary(&env));

    let env = TableKvTestEnv::new(2, MemoryTableWalBuilder::with_ttl("1d"));
    env.runtime.block_on(read_with_boundary(&env));
}

#[test]
fn test_write_multiple_regions() {
    let env = Arc::new(RocksTestEnv::new(4, RocksWalBuilder::default()));
    env.runtime
        .block_on(write_multiple_regions_parallelly(env.clone()));

    let env = Arc::new(TableKvTestEnv::new(4, MemoryTableWalBuilder::default()));
    env.runtime
        .block_on(write_multiple_regions_parallelly(env.clone()));

    let env = Arc::new(TableKvTestEnv::new(
        4,
        MemoryTableWalBuilder::with_ttl("1d"),
    ));
    env.runtime
        .block_on(write_multiple_regions_parallelly(env.clone()));
}

#[test]
fn test_reopen() {
    let env = RocksTestEnv::new(2, RocksWalBuilder::default());
    env.runtime.block_on(reopen(&env));

    let env = TableKvTestEnv::new(2, MemoryTableWalBuilder::default());
    env.runtime.block_on(reopen(&env));

    let env = TableKvTestEnv::new(2, MemoryTableWalBuilder::with_ttl("1d"));
    env.runtime.block_on(reopen(&env));
}

#[test]
fn test_complex_read_write() {
    let env = RocksTestEnv::new(2, RocksWalBuilder::default());
    env.runtime.block_on(complex_read_write(&env));

    let env = TableKvTestEnv::new(2, MemoryTableWalBuilder::default());
    env.runtime.block_on(complex_read_write(&env));

    let env = TableKvTestEnv::new(2, MemoryTableWalBuilder::with_ttl("1d"));
    env.runtime.block_on(complex_read_write(&env));
}

#[test]
fn test_simple_write_delete() {
    let env = RocksTestEnv::new(2, RocksWalBuilder::default());
    env.runtime.block_on(simple_write_delete(&env));

    let env = TableKvTestEnv::new(2, MemoryTableWalBuilder::default());
    env.runtime.block_on(simple_write_delete(&env));

    let env = TableKvTestEnv::new(2, MemoryTableWalBuilder::with_ttl("1d"));
    env.runtime.block_on(simple_write_delete(&env));
}

#[test]
fn test_write_delete_half() {
    let env = RocksTestEnv::new(2, RocksWalBuilder::default());
    env.runtime.block_on(write_delete_half(&env));

    let env = TableKvTestEnv::new(2, MemoryTableWalBuilder::default());
    env.runtime.block_on(write_delete_half(&env));

    let env = TableKvTestEnv::new(2, MemoryTableWalBuilder::with_ttl("1d"));
    env.runtime.block_on(write_delete_half(&env));
}

#[test]
fn test_write_delete_multiple_regions() {
    let env = RocksTestEnv::new(2, RocksWalBuilder::default());
    env.runtime.block_on(write_delete_multiple_regions(&env));

    let env = TableKvTestEnv::new(2, MemoryTableWalBuilder::default());
    env.runtime.block_on(write_delete_multiple_regions(&env));

    let env = TableKvTestEnv::new(2, MemoryTableWalBuilder::with_ttl("1d"));
    env.runtime.block_on(write_delete_multiple_regions(&env));
}

#[test]
fn test_sequence_increase_monotonically_multiple_writes() {
    let env = RocksTestEnv::new(2, RocksWalBuilder::default());
    env.runtime
        .block_on(sequence_increase_monotonically_multiple_writes(&env));

    let env = TableKvTestEnv::new(2, MemoryTableWalBuilder::default());
    env.runtime
        .block_on(sequence_increase_monotonically_multiple_writes(&env));

    let env = TableKvTestEnv::new(2, MemoryTableWalBuilder::with_ttl("1d"));
    env.runtime
        .block_on(sequence_increase_monotonically_multiple_writes(&env));
}

#[test]
fn test_sequence_increase_monotonically_delete_write() {
    let env = RocksTestEnv::new(2, RocksWalBuilder::default());
    env.runtime
        .block_on(sequence_increase_monotonically_delete_write(&env));

    let env = TableKvTestEnv::new(2, MemoryTableWalBuilder::default());
    env.runtime
        .block_on(sequence_increase_monotonically_delete_write(&env));

    let env = TableKvTestEnv::new(2, MemoryTableWalBuilder::with_ttl("1d"));
    env.runtime
        .block_on(sequence_increase_monotonically_delete_write(&env));
}

#[test]
fn test_sequence_increase_monotonically_delete_reopen_write() {
    let env = RocksTestEnv::new(2, RocksWalBuilder::default());
    env.runtime
        .block_on(sequence_increase_monotonically_delete_reopen_write(&env));

    let env = TableKvTestEnv::new(2, MemoryTableWalBuilder::default());
    env.runtime
        .block_on(sequence_increase_monotonically_delete_reopen_write(&env));

    let env = TableKvTestEnv::new(2, MemoryTableWalBuilder::with_ttl("1d"));
    env.runtime
        .block_on(sequence_increase_monotonically_delete_reopen_write(&env));
}
