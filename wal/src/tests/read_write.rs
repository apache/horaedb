// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

use std::{ops::Deref, sync::Arc};

use common_types::SequenceNumber;

use crate::{
    log_batch::LogWriteBatch,
    manager::{LogReader, LogWriter, ReadBoundary, ReadRequest, RegionId, WalManager},
    tests::util::{RocksTestEnv, TestEnv, TestPayload, WalBuilder},
};

fn check_write_batch_with_read_request<B: WalBuilder>(
    env: &TestEnv<B>,
    wal: Arc<B::Wal>,
    read_req: ReadRequest,
    max_seq: SequenceNumber,
    write_batch: &LogWriteBatch<TestPayload>,
) {
    let iter = wal
        .read(&env.read_ctx, &read_req)
        .expect("should succeed to read");
    env.check_log_entries(max_seq, write_batch, iter);
}

fn check_write_batch<B: WalBuilder>(
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
    check_write_batch_with_read_request(env, wal, read_req, max_seq, write_batch)
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
    check_write_batch(&env, wal, region_id, seq, &write_batch)
}

async fn simple_read_write<B: WalBuilder>(env: &TestEnv<B>, region_id: RegionId) {
    let wal = env.build_wal();
    simple_read_write_with_wal(env, wal.clone(), region_id).await;
}

/// Test the read with different kinds of boundaries.
async fn read_with_boundary<B: WalBuilder>(env: &TestEnv<B>) {
    let wal = env.build_wal();
    let region_id = 0;
    let write_batch = env.build_log_batch(region_id, 0, 10);
    let end_seq = wal
        .write(&env.write_ctx, &write_batch)
        .await
        .expect("should succeed to write");
    let start_seq = end_seq + 1 - write_batch.entries.len() as u64;

    // [min, max]
    {
        let read_req = ReadRequest {
            region_id,
            start: ReadBoundary::Min,
            end: ReadBoundary::Max,
        };
        check_write_batch_with_read_request(env, wal.clone(), read_req, end_seq, &write_batch);
    }

    // [0, 10]
    {
        let read_req = ReadRequest {
            region_id,
            start: ReadBoundary::Included(start_seq),
            end: ReadBoundary::Included(end_seq),
        };
        check_write_batch_with_read_request(env, wal.clone(), read_req, end_seq, &write_batch);
    }

    // (0, 10]
    {
        let read_req = ReadRequest {
            region_id,
            start: ReadBoundary::Excluded(start_seq),
            end: ReadBoundary::Included(end_seq),
        };
        let write_batch = env.build_log_batch(region_id, 1, 10);
        check_write_batch_with_read_request(env, wal.clone(), read_req, end_seq, &write_batch);
    }

    // [0, 10)
    {
        let read_req = ReadRequest {
            region_id,
            start: ReadBoundary::Included(start_seq),
            end: ReadBoundary::Excluded(end_seq),
        };
        let write_batch = env.build_log_batch(region_id, 0, 9);
        check_write_batch_with_read_request(env, wal.clone(), read_req, end_seq - 1, &write_batch);
    }

    // (0, 10)
    {
        let read_req = ReadRequest {
            region_id,
            start: ReadBoundary::Excluded(start_seq),
            end: ReadBoundary::Excluded(end_seq),
        };
        let write_batch = env.build_log_batch(region_id, 1, 9);
        check_write_batch_with_read_request(env, wal.clone(), read_req, end_seq - 1, &write_batch);
    }
}

/// Test read and write across multiple regions parallely.
async fn write_multiple_regions_parallelly<B: WalBuilder + 'static>(env: Arc<TestEnv<B>>) {
    let wal = env.build_wal();
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
}

/// Test whether the written logs can be read after reopen.
async fn reopen<B: WalBuilder>(env: &TestEnv<B>) {
    let region_id = 0;
    let (write_batch, seq) = {
        let wal = env.build_wal();
        let write_batch = env.build_log_batch(region_id, 0, 10);
        let seq = wal
            .write(&env.write_ctx, &write_batch)
            .await
            .expect("should succeed to write");
        (write_batch, seq)
    };

    // reopen the wal
    let wal = env.build_wal();
    let read_req = ReadRequest {
        region_id,
        start: ReadBoundary::Included(seq + 1 - write_batch.entries.len() as u64),
        end: ReadBoundary::Included(seq),
    };
    let iter = wal
        .read(&env.read_ctx, &read_req)
        .expect("should succeed to read");
    env.check_log_entries(seq, &write_batch, iter);
}

/// A complex test case for read and write:
///  - Write two log batch
///  - Read the first batch and then read the second batch.
///  - Read the whole batch.
///  - Read the part of first batch and second batch.
async fn complex_read_write<B: WalBuilder>(env: &TestEnv<B>) {
    let wal = env.build_wal();
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
    check_write_batch(env, wal.clone(), region_id, seq_1, &write_batch_1);
    // read the second batch
    check_write_batch(env, wal.clone(), region_id, seq_2, &write_batch_2);

    // read the whole batch
    let (seq_3, write_batch_3) = (seq_2, env.build_log_batch(region_id, start_val, end_val));
    check_write_batch(env, wal.clone(), region_id, seq_3, &write_batch_3);

    // read the part of batch1 and batch2
    let (seq_4, write_batch_4) = {
        let new_start = (start_val + mid_val) / 2;
        let new_end = (mid_val + end_val) / 2;
        let seq = seq_2 - (end_val - new_end) as u64;
        (seq, env.build_log_batch(region_id, new_start, new_end))
    };
    check_write_batch(env, wal.clone(), region_id, seq_4, &write_batch_4);
}

/// Test whether data can be deleted.
async fn simple_write_delete<B: WalBuilder>(env: &TestEnv<B>) {
    let region_id = 0;
    let wal = env.build_wal();
    let mut write_batch = env.build_log_batch(region_id, 0, 10);
    let seq = wal
        .write(&env.write_ctx, &write_batch)
        .await
        .expect("should succeed to write");
    check_write_batch(env, wal.clone(), region_id, seq, &write_batch);

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
        .read(&env.read_ctx, &read_req)
        .expect("should succeed to read");
    write_batch.entries.clear();
    env.check_log_entries(seq, &write_batch, iter);
}

/// Delete half of the written data and check the remaining half can be read.
async fn write_delete_half<B: WalBuilder>(env: &TestEnv<B>) {
    let region_id = 0;
    let wal = env.build_wal();
    let mut write_batch = env.build_log_batch(region_id, 0, 10);
    let seq = wal
        .write(&env.write_ctx, &write_batch)
        .await
        .expect("should succeed to write");
    check_write_batch(env, wal.clone(), region_id, seq, &write_batch);

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
        .read(&env.read_ctx, &read_req)
        .expect("should succeed to read");
    write_batch.entries.drain(..write_batch.entries.len() / 2);
    env.check_log_entries(seq, &write_batch, iter);
}

/// Test delete across multiple regions.
async fn write_delete_multiple_regions<B: WalBuilder>(env: &TestEnv<B>) {
    let (region_id_1, region_id_2) = (1, 2);
    let wal = env.build_wal();
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
        .read(&env.read_ctx, &read_req)
        .expect("should succeed to read");
    write_batch_1.entries.clear();
    env.check_log_entries(seq_1, &write_batch_1, iter);

    check_write_batch(env, wal.clone(), region_id_2, seq_2, &write_batch_2);
}

/// The sequence number should increase monotonically after multiple writes.
async fn sequence_increase_monotonically_multiple_writes<B: WalBuilder>(env: &TestEnv<B>) {
    let region_id = 0;
    let wal = env.build_wal();
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
}

/// The sequence number should increase monotonically after write, delete and
/// one more write.
async fn sequence_increase_monotonically_delete_write<B: WalBuilder>(env: &TestEnv<B>) {
    let region_id = 0;
    let wal = env.build_wal();
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

    assert!(seq_2 > seq_1);
}

/// The sequence number should increase monotonically after write, delete,
/// reopen and write.
async fn sequence_increase_monotonically_delete_reopen_write<B: WalBuilder>(env: &TestEnv<B>) {
    let region_id = 0;
    let wal = env.build_wal();
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
    drop(wal);
    let wal = env.build_wal();
    // write again
    let seq_2 = wal
        .write(&env.write_ctx, &write_batch)
        .await
        .expect("should succeed to write");

    assert!(seq_2 > seq_1);
}

#[test]
fn test_simple_read_write() {
    let rocks_env = RocksTestEnv::new(2);
    rocks_env.runtime.block_on(simple_read_write(&rocks_env, 0));
}

#[test]
fn test_read_with_boundary() {
    let rocks_env = RocksTestEnv::new(2);
    rocks_env.runtime.block_on(read_with_boundary(&rocks_env));
}

#[test]
fn test_write_multiple_regions() {
    let rocks_env = Arc::new(RocksTestEnv::new(4));
    rocks_env
        .runtime
        .block_on(write_multiple_regions_parallelly(rocks_env.clone()));
}

#[test]
fn test_reopen() {
    let rocks_env = RocksTestEnv::new(2);
    rocks_env.runtime.block_on(reopen(&rocks_env));
}

#[test]
fn test_complex_read_write() {
    let rocks_env = RocksTestEnv::new(2);
    rocks_env.runtime.block_on(complex_read_write(&rocks_env));
}

#[test]
fn test_simple_write_delete() {
    let rocks_env = RocksTestEnv::new(2);
    rocks_env.runtime.block_on(simple_write_delete(&rocks_env));
}

#[test]
fn test_write_delete_half() {
    let rocks_env = RocksTestEnv::new(2);
    rocks_env.runtime.block_on(write_delete_half(&rocks_env));
}
#[test]
fn test_write_delete_multiple_regions() {
    let rocks_env = RocksTestEnv::new(2);
    rocks_env
        .runtime
        .block_on(write_delete_multiple_regions(&rocks_env));
}

#[test]
fn test_sequence_increase_monotonically_multiple_writes() {
    let rocks_env = RocksTestEnv::new(2);
    rocks_env
        .runtime
        .block_on(sequence_increase_monotonically_multiple_writes(&rocks_env));
}

#[test]
fn test_sequence_increase_monotonically_delete_write() {
    let rocks_env = RocksTestEnv::new(2);
    rocks_env
        .runtime
        .block_on(sequence_increase_monotonically_delete_write(&rocks_env));
}

#[test]
fn test_sequence_increase_monotonically_delete_reopen_write() {
    let rocks_env = RocksTestEnv::new(2);
    rocks_env
        .runtime
        .block_on(sequence_increase_monotonically_delete_reopen_write(
            &rocks_env,
        ));
}
