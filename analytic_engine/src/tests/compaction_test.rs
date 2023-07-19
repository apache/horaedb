// Copyright 2022-2023 CeresDB Project Authors. Licensed under Apache-2.0.

//! Compaction integration tests.

use common_types::time::Timestamp;
use table_engine::table::FlushRequest;

use crate::{
    compaction::SizeTieredCompactionOptions,
    tests::util::{
        self, EngineBuildContext, MemoryEngineBuildContext, RocksDBEngineBuildContext, TestEnv,
    },
};

#[test]
fn test_table_compact_current_segment_rocks() {
    let rocksdb_ctx = RocksDBEngineBuildContext::default();
    test_table_compact_current_segment(rocksdb_ctx);
}

#[test]
fn test_table_compact_current_segment_mem_wal() {
    let memory_ctx = MemoryEngineBuildContext::default();
    test_table_compact_current_segment(memory_ctx);
}

fn test_table_compact_current_segment<T: EngineBuildContext>(engine_context: T) {
    let env = TestEnv::builder().build();
    let mut test_ctx = env.new_context(engine_context);

    env.block_on(async {
        test_ctx.open().await;

        let compact_test_table1 = "compact_test_table1";
        let fixed_schema_table = test_ctx
            .create_fixed_schema_table(compact_test_table1)
            .await;
        let default_opts = SizeTieredCompactionOptions::default();

        let mut expect_rows = Vec::new();

        let start_ms = test_ctx.start_ms();
        // Write max_threshold*2 sst to ensure level0->level1, level1->level1 compaction
        // will be triggered.
        for offset in 0..default_opts.max_threshold as i64 * 2 {
            let rows = [
                (
                    "key1",
                    Timestamp::new(start_ms + offset),
                    "tag1-1",
                    11.0,
                    110.0,
                    "tag2-1",
                ),
                (
                    "key2",
                    Timestamp::new(start_ms + offset),
                    "tag1-2",
                    12.0,
                    110.0,
                    "tag2-2",
                ),
            ];
            expect_rows.extend_from_slice(&rows);
            let row_group = fixed_schema_table.rows_to_row_group(&rows);

            test_ctx
                .write_to_table(compact_test_table1, row_group)
                .await;

            // Flush table and generate sst.
            test_ctx
                .flush_table_with_request(compact_test_table1, FlushRequest { sync: true })
                .await;
        }

        expect_rows.sort_unstable_by_key(|row_tuple| (row_tuple.0, row_tuple.1));

        util::check_read(
            &test_ctx,
            &fixed_schema_table,
            "Test read after flush",
            compact_test_table1,
            &expect_rows,
        )
        .await;

        test_util::init_log_for_test();

        // Trigger a compaction.
        test_ctx.compact_table(compact_test_table1).await;

        // Check read after compaction.
        util::check_read(
            &test_ctx,
            &fixed_schema_table,
            "Test read after compaction",
            compact_test_table1,
            &expect_rows,
        )
        .await;
    });
}
