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

//! Read write test.

use std::{thread, time};

use common_types::time::Timestamp;
use logger::info;
use wal::manager::WalsOpener;

use crate::{
    table_options,
    tests::util::{self, memory_ctxs, rocksdb_ctxs, EngineBuildContext, TestContext, TestEnv},
};

#[test]
fn test_multi_table_read_write_rocks() {
    let rocksdb_ctxs = rocksdb_ctxs();
    for ctx in rocksdb_ctxs {
        test_multi_table_read_write(ctx);
    }
}

#[test]
fn test_multi_table_read_write_mem_wal() {
    let memory_ctxs = memory_ctxs();
    for ctx in memory_ctxs {
        test_multi_table_read_write(ctx);
    }
}

fn test_multi_table_read_write<T: EngineBuildContext>(engine_context: T) {
    let env = TestEnv::builder().build();
    let mut test_ctx = env.new_context(engine_context);

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
    let rocksdb_ctxs = rocksdb_ctxs();
    for ctx in rocksdb_ctxs {
        test_table_write_read(ctx);
    }
}

#[test]
fn test_table_write_read_mem_wal() {
    let memory_ctxs = memory_ctxs();
    for ctx in memory_ctxs {
        test_table_write_read(ctx);
    }
}

fn test_table_write_read<T: EngineBuildContext>(engine_context: T) {
    let env = TestEnv::builder().build();
    let mut test_ctx = env.new_context(engine_context);

    env.block_on(async {
        test_ctx.open().await;

        let test_table1 = "test_table1";
        let fixed_schema_table = test_ctx.create_fixed_schema_table(test_table1).await;

        let start_ms = test_ctx.start_ms();
        let rows: [(&str, Timestamp, &str, f64, f64, &str); 3] = [
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
    let rocksdb_ctxs = rocksdb_ctxs();
    for ctx in rocksdb_ctxs {
        test_table_write_get(ctx);
    }
}

#[test]
fn test_table_write_get_mem_wal() {
    let memory_ctxs = memory_ctxs();
    for ctx in memory_ctxs {
        test_table_write_get(ctx);
    }
}

fn test_table_write_get<T: EngineBuildContext>(engine_context: T) {
    let env = TestEnv::builder().build();
    let mut test_ctx = env.new_context(engine_context);

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
    let rocksdb_ctxs = rocksdb_ctxs();
    for ctx in rocksdb_ctxs {
        test_table_write_get_override(ctx);
    }
}

#[test]
fn test_table_write_get_override_mem_wal() {
    let memory_ctxs = memory_ctxs();
    for ctx in memory_ctxs {
        test_table_write_get_override(ctx);
    }
}

fn test_table_write_get_override<T: EngineBuildContext>(engine_context: T) {
    test_table_write_get_override_case::<T>(FlushPoint::NoFlush, engine_context.clone());

    test_table_write_get_override_case::<T>(FlushPoint::AfterFirstWrite, engine_context.clone());

    test_table_write_get_override_case::<T>(FlushPoint::AfterOverwrite, engine_context.clone());

    test_table_write_get_override_case::<T>(FlushPoint::FirstAndOverwrite, engine_context);
}

#[derive(Debug)]
enum FlushPoint {
    NoFlush,
    AfterFirstWrite,
    AfterOverwrite,
    FirstAndOverwrite,
}

fn test_table_write_get_override_case<T: EngineBuildContext>(
    flush_point: FlushPoint,
    engine_context: T,
) {
    let env = TestEnv::builder().build();
    let mut test_ctx = env.new_context(engine_context);

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
    let rocksdb_ctxs = rocksdb_ctxs();
    for ctx in rocksdb_ctxs {
        // Use different table name to avoid metrics collision.
        test_db_write_buffer_size("test_db_write_buffer_size_rocks", ctx);
    }
}

#[test]
fn test_db_write_buffer_size_mem_wal() {
    let memory_ctxs = memory_ctxs();
    for ctx in memory_ctxs {
        // Use different table name to avoid metrics collision.
        test_db_write_buffer_size("test_db_write_buffer_size_mem_wal", ctx);
    }
}

fn test_db_write_buffer_size<T: EngineBuildContext>(table_name: &str, engine_context: T) {
    let env = TestEnv::builder().build();
    let mut test_ctx = env.new_context(engine_context);
    test_ctx.config_mut().db_write_buffer_size = 1;
    test_write_buffer_size_overflow(table_name, env, test_ctx);
}

#[test]
fn test_space_write_buffer_size_rocks() {
    let rocksdb_ctxs = rocksdb_ctxs();
    for ctx in rocksdb_ctxs {
        // Use different table name to avoid metrics collision.
        test_space_write_buffer_size("test_space_write_buffer_size_rocks", ctx);
    }
}

#[test]
fn test_space_write_buffer_size_mem_wal() {
    let memory_ctxs = memory_ctxs();
    for ctx in memory_ctxs {
        // Use different table name to avoid metrics collision.
        test_space_write_buffer_size("test_space_write_buffer_size_mem_wal", ctx);
    }
}

fn test_space_write_buffer_size<T: EngineBuildContext>(table_name: &str, engine_context: T) {
    let env = TestEnv::builder().build();
    let mut test_ctx = env.new_context(engine_context);
    test_ctx.config_mut().space_write_buffer_size = 1;
    test_write_buffer_size_overflow(table_name, env, test_ctx);
}

fn test_write_buffer_size_overflow<T: WalsOpener>(
    test_table_name: &str,
    env: TestEnv,
    mut test_ctx: TestContext<T>,
) {
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

        // Read with different opts.
        util::check_read(
            &test_ctx,
            &fixed_schema_table,
            "Test read write table",
            test_table_name,
            &rows,
        )
        .await;

        // TODO(lee) a better way to wait table flushing finishes.
        thread::sleep(time::Duration::from_millis(500));

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
