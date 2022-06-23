// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

//! Read write test.

use std::{thread, time};

use common_types::time::Timestamp;
use log::info;
use table_engine::table::ReadOrder;

use crate::{
    setup::{EngineBuilder, MemWalEngineBuilder, RocksEngineBuilder},
    table_options,
    tests::util::{self, TestEnv},
};

#[test]
fn test_multi_table_read_write_rocks() {
    test_multi_table_read_write::<RocksEngineBuilder>();
}

#[test]
fn test_multi_table_read_write_mem_wal() {
    test_multi_table_read_write::<MemWalEngineBuilder>();
}

fn test_multi_table_read_write<T: EngineBuilder>() {
    let env = TestEnv::builder().build();
    let mut test_ctx = env.new_context::<T>();

    env.block_on(async {
        test_ctx.open().await;

        let test_table1 = "test_multi_table_read_write1";
        let test_table2 = "test_multi_table_read_write2";
        let test_table3 = "test_multi_table_read_write3";

        let fixed_schema_table = test_ctx.create_fixed_schema_table(test_table1).await;
        let _ = test_ctx.create_fixed_schema_table(test_table2).await;
        let _ = test_ctx.create_fixed_schema_table(test_table3).await;

        let start_ms = test_ctx.start_ms();
        let rows = [
            // One bucket.
            (
                "key1",
                Timestamp::new(start_ms),
                "tag1-1",
                11.0,
                110.0,
                "tag2-1",
            ),
            (
                "key2",
                Timestamp::new(start_ms),
                "tag1-2",
                12.0,
                110.0,
                "tag2-2",
            ),
            (
                "key2",
                Timestamp::new(start_ms + 1),
                "tag1-3",
                13.0,
                110.0,
                "tag2-3",
            ),
            (
                "key3",
                Timestamp::new(start_ms + 2),
                "tag1-4",
                13.0,
                110.0,
                "tag2-4",
            ),
            (
                "key4",
                Timestamp::new(start_ms + 3),
                "tag1-5",
                13.0,
                110.0,
                "tag2-5",
            ),
            // Next bucket.
            (
                "key5",
                Timestamp::new(
                    start_ms + 1 + 2 * table_options::DEFAULT_SEGMENT_DURATION.as_millis() as i64,
                ),
                "tag-5-3",
                33.0,
                310.0,
                "tag-5-3",
            ),
        ];

        // Write data to table.
        let row_group1 = fixed_schema_table.rows_to_row_group(&rows);
        let row_group2 = fixed_schema_table.rows_to_row_group(&rows);
        let row_group3 = fixed_schema_table.rows_to_row_group(&rows);
        test_ctx.write_to_table(test_table1, row_group1).await;
        test_ctx.write_to_table(test_table2, row_group2).await;
        test_ctx.write_to_table(test_table3, row_group3).await;

        // Read with different opts.
        util::check_read(
            &test_ctx,
            &fixed_schema_table,
            "Test read write table1",
            test_table1,
            &rows,
        )
        .await;

        util::check_read(
            &test_ctx,
            &fixed_schema_table,
            "Test read write table2",
            test_table2,
            &rows,
        )
        .await;

        util::check_read(
            &test_ctx,
            &fixed_schema_table,
            "Test read write table3",
            test_table3,
            &rows,
        )
        .await;

        // Reopen db.
        test_ctx
            .reopen_with_tables(&[test_table1, test_table2, test_table3])
            .await;

        // Read with different opts again.
        util::check_read(
            &test_ctx,
            &fixed_schema_table,
            "Test read write table1 after reopen",
            test_table1,
            &rows,
        )
        .await;
        util::check_read(
            &test_ctx,
            &fixed_schema_table,
            "Test read write table2 after reopen",
            test_table2,
            &rows,
        )
        .await;
        util::check_read(
            &test_ctx,
            &fixed_schema_table,
            "Test read write table3 after reopen",
            test_table3,
            &rows,
        )
        .await;
    });
}

#[test]
fn test_table_write_read_rocks() {
    test_table_write_read::<RocksEngineBuilder>();
}

#[test]
fn test_table_write_read_mem_wal() {
    test_table_write_read::<MemWalEngineBuilder>();
}

fn test_table_write_read<T: EngineBuilder>() {
    let env = TestEnv::builder().build();
    let mut test_ctx = env.new_context::<T>();

    env.block_on(async {
        test_ctx.open().await;

        let test_table1 = "test_table1";
        let fixed_schema_table = test_ctx.create_fixed_schema_table(test_table1).await;

        let start_ms = test_ctx.start_ms();
        let rows = [
            (
                "key1",
                Timestamp::new(start_ms),
                "tag1-1",
                11.0,
                110.0,
                "tag2-1",
            ),
            (
                "key2",
                Timestamp::new(start_ms),
                "tag1-2",
                12.0,
                110.0,
                "tag2-2",
            ),
            (
                "key2",
                Timestamp::new(start_ms + 1),
                "tag1-3",
                13.0,
                110.0,
                "tag2-3",
            ),
        ];
        let row_group = fixed_schema_table.rows_to_row_group(&rows);

        // Write data to table.
        test_ctx.write_to_table(test_table1, row_group).await;

        // Read with different opts.
        util::check_read(
            &test_ctx,
            &fixed_schema_table,
            "Test read write table",
            test_table1,
            &rows,
        )
        .await;

        // Reopen db.
        test_ctx.reopen_with_tables(&[test_table1]).await;

        // Read with different opts again.
        util::check_read(
            &test_ctx,
            &fixed_schema_table,
            "Test read write table after reopen",
            test_table1,
            &rows,
        )
        .await;
    });
}

#[test]
fn test_table_write_get_rocks() {
    test_table_write_get::<RocksEngineBuilder>();
}

#[test]
fn test_table_write_get_mem_wal() {
    test_table_write_get::<MemWalEngineBuilder>();
}

fn test_table_write_get<T: EngineBuilder>() {
    let env = TestEnv::builder().build();
    let mut test_ctx = env.new_context::<T>();

    env.block_on(async {
        test_ctx.open().await;

        let test_table1 = "test_table1";
        let fixed_schema_table = test_ctx.create_fixed_schema_table(test_table1).await;

        let start_ms = test_ctx.start_ms();
        let rows = [
            (
                "key1",
                Timestamp::new(start_ms),
                "tag1-1",
                11.0,
                110.0,
                "tag2-1",
            ),
            (
                "key2",
                Timestamp::new(start_ms),
                "tag1-2",
                12.0,
                110.0,
                "tag2-2",
            ),
            (
                "key2",
                Timestamp::new(start_ms + 1),
                "tag1-3",
                13.0,
                110.0,
                "tag2-3",
            ),
        ];
        let row_group = fixed_schema_table.rows_to_row_group(&rows);

        // Write data to table.
        test_ctx.write_to_table(test_table1, row_group).await;

        util::check_get(
            &test_ctx,
            &fixed_schema_table,
            "Try to get row",
            test_table1,
            &rows,
        )
        .await;

        // Reopen db.
        test_ctx.reopen_with_tables(&[test_table1]).await;

        util::check_get(
            &test_ctx,
            &fixed_schema_table,
            "Try to get row after reopen",
            test_table1,
            &rows,
        )
        .await;
    });
}

#[test]
fn test_table_write_get_override_rocks() {
    test_table_write_get_override::<RocksEngineBuilder>();
}

#[test]
fn test_table_write_get_override_mem_wal() {
    test_table_write_get_override::<MemWalEngineBuilder>();
}

fn test_table_write_get_override<T: EngineBuilder>() {
    test_table_write_get_override_case::<T>(FlushPoint::NoFlush);

    test_table_write_get_override_case::<T>(FlushPoint::AfterFirstWrite);

    test_table_write_get_override_case::<T>(FlushPoint::AfterOverwrite);

    test_table_write_get_override_case::<T>(FlushPoint::FirstAndOverwrite);
}

#[derive(Debug)]
enum FlushPoint {
    NoFlush,
    AfterFirstWrite,
    AfterOverwrite,
    FirstAndOverwrite,
}

fn test_table_write_get_override_case<T: EngineBuilder>(flush_point: FlushPoint) {
    let env = TestEnv::builder().build();
    let mut test_ctx = env.new_context::<T>();

    env.block_on(async {
        info!(
            "test_table_write_get_override_case, flush_point:{:?}",
            flush_point
        );

        test_ctx.open().await;

        let test_table1 = "test_table1";
        let fixed_schema_table = test_ctx.create_fixed_schema_table(test_table1).await;

        let start_ms = test_ctx.start_ms();
        {
            let rows = [
                (
                    "key1",
                    Timestamp::new(start_ms),
                    "tag1-1",
                    11.0,
                    110.0,
                    "tag2-1",
                ),
                (
                    "key2",
                    Timestamp::new(start_ms),
                    "tag1-2",
                    12.0,
                    110.0,
                    "tag2-2",
                ),
                (
                    "key3",
                    Timestamp::new(start_ms + 10),
                    "tag1-3",
                    13.0,
                    110.0,
                    "tag2-3",
                ),
                (
                    "key2",
                    Timestamp::new(start_ms + 1),
                    "tag1-3",
                    13.0,
                    110.0,
                    "tag2-3",
                ),
            ];
            let row_group = fixed_schema_table.rows_to_row_group(&rows);

            // Write data to table.
            test_ctx.write_to_table(test_table1, row_group).await;
        }

        if let FlushPoint::AfterFirstWrite | FlushPoint::FirstAndOverwrite = flush_point {
            test_ctx.flush_table(test_table1).await;
        }

        // Override some rows
        {
            let rows = [
                (
                    "key2",
                    Timestamp::new(start_ms),
                    "tag1-2-copy",
                    112.0,
                    210.0,
                    "tag2-2-copy",
                ),
                (
                    "key2",
                    Timestamp::new(start_ms + 1),
                    "tag1-3-copy",
                    113.0,
                    210.0,
                    "tag2-3-copy",
                ),
            ];
            let row_group = fixed_schema_table.rows_to_row_group(&rows);

            test_ctx.write_to_table(test_table1, row_group).await;
        }

        if let FlushPoint::AfterOverwrite | FlushPoint::FirstAndOverwrite = flush_point {
            test_ctx.flush_table(test_table1).await;
        }

        let expect_rows = [
            (
                "key1",
                Timestamp::new(start_ms),
                "tag1-1",
                11.0,
                110.0,
                "tag2-1",
            ),
            (
                "key2",
                Timestamp::new(start_ms),
                "tag1-2-copy",
                112.0,
                210.0,
                "tag2-2-copy",
            ),
            (
                "key2",
                Timestamp::new(start_ms + 1),
                "tag1-3-copy",
                113.0,
                210.0,
                "tag2-3-copy",
            ),
            (
                "key3",
                Timestamp::new(start_ms + 10),
                "tag1-3",
                13.0,
                110.0,
                "tag2-3",
            ),
        ];

        util::check_get(
            &test_ctx,
            &fixed_schema_table,
            "Try to get row",
            test_table1,
            &expect_rows,
        )
        .await;

        // Reopen db.
        test_ctx.reopen_with_tables(&[test_table1]).await;

        util::check_get(
            &test_ctx,
            &fixed_schema_table,
            "Try to get row after reopen",
            test_table1,
            &expect_rows,
        )
        .await;
    });
}

#[test]
fn test_db_write_buffer_size_rocks() {
    // Use different table name to avoid metrics collision.
    test_db_write_buffer_size::<RocksEngineBuilder>("test_db_write_buffer_size_rocks");
}

#[test]
fn test_db_write_buffer_size_mem_wal() {
    // Use different table name to avoid metrics collision.
    test_db_write_buffer_size::<MemWalEngineBuilder>("test_db_write_buffer_size_mem_wal");
}

fn test_db_write_buffer_size<T: EngineBuilder>(table_name: &str) {
    let mut env = TestEnv::builder().build();
    env.config.db_write_buffer_size = 1;
    test_write_buffer_size_overflow::<T>(table_name, env);
}

#[test]
fn test_space_write_buffer_size_rocks() {
    // Use different table name to avoid metrics collision.
    test_space_write_buffer_size::<RocksEngineBuilder>("test_space_write_buffer_size_rocks");
}

#[test]
fn test_space_write_buffer_size_mem_wal() {
    // Use different table name to avoid metrics collision.
    test_space_write_buffer_size::<MemWalEngineBuilder>("test_space_write_buffer_size_mem_wal");
}

fn test_space_write_buffer_size<T: EngineBuilder>(table_name: &str) {
    let mut env = TestEnv::builder().build();
    env.config.space_write_buffer_size = 1;
    test_write_buffer_size_overflow::<T>(table_name, env);
}

fn test_write_buffer_size_overflow<T: EngineBuilder>(test_table_name: &str, env: TestEnv) {
    let mut test_ctx = env.new_context::<T>();

    env.block_on(async {
        test_ctx.open().await;

        let fixed_schema_table = test_ctx.create_fixed_schema_table(test_table_name).await;

        let table = test_ctx.table(test_table_name);
        // Note that table with same name shares same global prometheus metrics.
        let old_stats = table.stats();

        let start_ms = test_ctx.start_ms();
        let rows1 = [
            (
                "key1",
                Timestamp::new(start_ms),
                "tag1-1",
                11.0,
                110.0,
                "tag2-1",
            ),
            (
                "key2",
                Timestamp::new(start_ms),
                "tag1-2",
                12.0,
                110.0,
                "tag2-2",
            ),
            (
                "key2",
                Timestamp::new(start_ms + 1),
                "tag1-3",
                13.0,
                110.0,
                "tag2-3",
            ),
        ];
        let row_group = fixed_schema_table.rows_to_row_group(&rows1);
        // Write rows1 to table.
        test_ctx.write_to_table(test_table_name, row_group).await;

        let stats = table.stats();
        assert_eq!(old_stats.num_read, stats.num_read);
        assert_eq!(old_stats.num_write + 1, stats.num_write);
        assert_eq!(old_stats.num_flush, stats.num_flush);

        let rows2 = [
            (
                "key4",
                Timestamp::new(start_ms + 2),
                "tag1-4",
                11.0,
                110.0,
                "tag2-4",
            ),
            (
                "key5",
                Timestamp::new(start_ms + 3),
                "tag1-5",
                12.0,
                110.0,
                "tag2-5",
            ),
        ];

        let row_group = fixed_schema_table.rows_to_row_group(&rows2);
        // Write rows2 to table.
        test_ctx.write_to_table(test_table_name, row_group).await;

        let mut rows = Vec::new();
        rows.extend_from_slice(&rows1);
        rows.extend_from_slice(&rows2);

        // TODO(boyan) a better way to wait  table flushing finishes.
        thread::sleep(time::Duration::from_millis(500));

        // Read with different opts.
        util::check_read(
            &test_ctx,
            &fixed_schema_table,
            "Test read write table",
            test_table_name,
            &rows,
        )
        .await;

        let stats = table.stats();
        assert_eq!(old_stats.num_read + 5, stats.num_read);
        assert_eq!(old_stats.num_write + 2, stats.num_write);
        // Flush when reaches (db/space) write_buffer size limitation.
        assert_eq!(old_stats.num_flush + 1, stats.num_flush);

        drop(table);
        // Reopen db.
        test_ctx.reopen_with_tables(&[test_table_name]).await;

        // Read with different opts again.
        util::check_read(
            &test_ctx,
            &fixed_schema_table,
            "Test read write table after reopen",
            test_table_name,
            &rows,
        )
        .await;
    });
}

#[test]
fn test_table_write_read_reverse_rocks() {
    test_table_write_read_reverse::<RocksEngineBuilder>();
}

#[test]
fn test_table_write_read_reverse_mem_wal() {
    test_table_write_read_reverse::<MemWalEngineBuilder>();
}

fn test_table_write_read_reverse<T: EngineBuilder>() {
    let env = TestEnv::builder().build();
    let mut test_ctx = env.new_context::<T>();

    env.block_on(async {
        test_ctx.open().await;

        let test_table = "test_table";
        let fixed_schema_table = test_ctx.create_fixed_schema_table(test_table).await;

        let start_ms = test_ctx.start_ms();
        let rows = [
            (
                "key1",
                Timestamp::new(start_ms),
                "tag1-1",
                11.0,
                110.0,
                "tag2-1",
            ),
            // update the first row
            (
                "key1",
                Timestamp::new(start_ms),
                "tag1-2",
                12.0,
                110.0,
                "tag2-2",
            ),
            (
                "key1",
                Timestamp::new(start_ms + 1),
                "tag1-2",
                12.0,
                110.0,
                "tag2-2",
            ),
            (
                "key2",
                Timestamp::new(start_ms),
                "tag1-3",
                13.0,
                110.0,
                "tag2-3",
            ),
            (
                "key2",
                Timestamp::new(start_ms + 1),
                "tag1-3",
                13.0,
                110.0,
                "tag2-3",
            ),
        ];
        let expect_reversed_rows = vec![rows[4], rows[3], rows[2], rows[1]];
        let row_group = fixed_schema_table.rows_to_row_group(&rows);

        // Write data to table.
        test_ctx.write_to_table(test_table, row_group).await;

        // Read reverse
        util::check_read_with_order(
            &test_ctx,
            &fixed_schema_table,
            "Test read write table",
            test_table,
            &expect_reversed_rows,
            ReadOrder::Desc,
        )
        .await;
    });
}

#[test]
fn test_table_write_read_reverse_after_flush_rocks() {
    test_table_write_read_reverse_after_flush::<RocksEngineBuilder>();
}

#[test]
fn test_table_write_read_reverse_after_flush_mem_wal() {
    test_table_write_read_reverse_after_flush::<MemWalEngineBuilder>();
}

fn test_table_write_read_reverse_after_flush<T: EngineBuilder>() {
    let env = TestEnv::builder().build();
    let mut test_ctx = env.new_context::<T>();

    env.block_on(async {
        test_ctx.open().await;

        let test_table = "test_table";
        let fixed_schema_table = test_ctx.create_fixed_schema_table(test_table).await;

        let start_ms = test_ctx.start_ms();
        let rows1 = [
            (
                "key1",
                Timestamp::new(start_ms),
                "tag1-1",
                11.0,
                110.0,
                "tag2-1",
            ),
            (
                "key2",
                Timestamp::new(start_ms),
                "tag1-3",
                13.0,
                110.0,
                "tag2-3",
            ),
            (
                "key2",
                Timestamp::new(start_ms + 1),
                "tag1-3",
                13.0,
                110.0,
                "tag2-3",
            ),
        ];

        let rows2 = vec![
            // update the first row
            (
                "key1",
                Timestamp::new(start_ms),
                "tag1-2",
                12.0,
                110.0,
                "tag2-2",
            ),
            (
                "key1",
                Timestamp::new(start_ms + 1),
                "tag1-2",
                12.0,
                110.0,
                "tag2-2",
            ),
        ];

        let expect_reversed_rows = vec![rows1[2], rows1[1], rows2[1], rows2[0]];
        let row_group1 = fixed_schema_table.rows_to_row_group(&rows1);
        // Write data to table and flush
        test_ctx.write_to_table(test_table, row_group1).await;
        test_ctx.flush_table(test_table).await;

        let row_group2 = fixed_schema_table.rows_to_row_group(&rows2);
        // Write data to table and not flush
        test_ctx.write_to_table(test_table, row_group2).await;

        // Read reverse
        util::check_read_with_order(
            &test_ctx,
            &fixed_schema_table,
            "Test read write table",
            test_table,
            &expect_reversed_rows,
            ReadOrder::Desc,
        )
        .await;
    });
}
