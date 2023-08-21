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

//! Drop table tests

use std::collections::HashMap;

use common_types::{column_schema, datum::DatumKind, time::Timestamp};
use table_engine::table::AlterSchemaRequest;

use crate::tests::{
    table::FixedSchemaTable,
    util::{
        self, memory_ctxs, rocksdb_ctxs, EngineBuildContext, MemoryEngineBuildContext,
        RocksDBEngineBuildContext, TestEnv,
    },
};

#[test]
fn test_drop_table_once_rocks() {
    let rocksdb_ctx = RocksDBEngineBuildContext::default();
    test_drop_table_once(rocksdb_ctx);
}

#[test]
fn test_drop_table_once_mem_wal() {
    let memory_ctx = MemoryEngineBuildContext::default();
    test_drop_table_once(memory_ctx);
}

fn test_drop_table_once<T: EngineBuildContext>(engine_context: T) {
    let env = TestEnv::builder().build();
    let mut test_ctx = env.new_context(engine_context);

    env.block_on(async {
        test_ctx.open().await;

        let drop_test_table1 = "drop_test_table1";
        let table_id = test_ctx
            .create_fixed_schema_table(drop_test_table1)
            .await
            .table_id();

        assert!(test_ctx.drop_table(drop_test_table1).await);

        let table_opt = test_ctx
            .try_open_table(table_id, drop_test_table1)
            .await
            .unwrap();
        assert!(table_opt.is_none());

        test_ctx.reopen().await;

        let table_opt = test_ctx
            .try_open_table(table_id, drop_test_table1)
            .await
            .unwrap();
        assert!(table_opt.is_none());
    });
}

#[test]
fn test_drop_table_again_rocks() {
    let rocksdb_ctx = RocksDBEngineBuildContext::default();
    test_drop_table_again(rocksdb_ctx);
}

#[test]
fn test_drop_table_again_mem_wal() {
    let memory_ctx = MemoryEngineBuildContext::default();
    test_drop_table_again(memory_ctx);
}

fn test_drop_table_again<T: EngineBuildContext>(engine_context: T) {
    let env = TestEnv::builder().build();
    let mut test_ctx = env.new_context(engine_context);

    env.block_on(async {
        test_ctx.open().await;

        let drop_test_table1 = "drop_test_table1";
        let table_id = test_ctx
            .create_fixed_schema_table(drop_test_table1)
            .await
            .table_id();

        assert!(test_ctx.drop_table(drop_test_table1).await);

        assert!(!test_ctx.drop_table(drop_test_table1).await);

        let table_opt = test_ctx
            .try_open_table(table_id, drop_test_table1)
            .await
            .unwrap();
        assert!(table_opt.is_none());
    });
}

#[test]
fn test_drop_create_table_mixed_rocks() {
    let rocksdb_ctx = RocksDBEngineBuildContext::default();
    test_drop_create_table_mixed(rocksdb_ctx);
}

#[test]
fn test_drop_create_table_mixed_mem_wal() {
    let memory_ctx = MemoryEngineBuildContext::default();
    test_drop_create_table_mixed(memory_ctx);
}

fn test_drop_create_table_mixed<T: EngineBuildContext>(engine_context: T) {
    let env = TestEnv::builder().build();
    let mut test_ctx = env.new_context(engine_context);

    env.block_on(async {
        test_ctx.open().await;

        let drop_test_table1 = "drop_test_table1";
        let table1_id = test_ctx
            .create_fixed_schema_table(drop_test_table1)
            .await
            .table_id();

        assert!(test_ctx.drop_table(drop_test_table1).await);

        // Create another table after dropped.
        let test_table2 = "test_table2";
        let table2_id = test_ctx
            .create_fixed_schema_table(test_table2)
            .await
            .table_id();

        let table_opt = test_ctx
            .try_open_table(table1_id, drop_test_table1)
            .await
            .unwrap();
        assert!(table_opt.is_none());

        test_ctx.reopen().await;

        let table_opt = test_ctx
            .try_open_table(table1_id, drop_test_table1)
            .await
            .unwrap();
        assert!(table_opt.is_none());
        // Table 2 is still exists.
        assert!(test_ctx
            .try_open_table(table2_id, test_table2)
            .await
            .unwrap()
            .is_some());
    });
}

fn test_drop_create_same_table_case<T: EngineBuildContext>(flush: bool, engine_context: T) {
    let env = TestEnv::builder().build();
    let mut test_ctx = env.new_context(engine_context);

    env.block_on(async {
        test_ctx.open().await;

        let drop_test_table1 = "drop_test_table1";
        let fixed_schema_table = test_ctx.create_fixed_schema_table(drop_test_table1).await;

        // Write data to table1.
        let start_ms = test_ctx.start_ms();
        let rows = [(
            "key1",
            Timestamp::new(start_ms),
            "tag1-1",
            11.0,
            110.0,
            "tag2-1",
        )];
        let row_group = fixed_schema_table.rows_to_row_group(&rows);
        test_ctx.write_to_table(drop_test_table1, row_group).await;

        if flush {
            test_ctx.flush_table(drop_test_table1).await;
        }

        assert!(test_ctx.drop_table(drop_test_table1).await);

        // Create same table again.
        let drop_test_table1 = "drop_test_table1";
        test_ctx.create_fixed_schema_table(drop_test_table1).await;

        // No data exists.
        util::check_read(
            &test_ctx,
            &fixed_schema_table,
            "Test read table",
            drop_test_table1,
            &[],
        )
        .await;

        test_ctx.reopen_with_tables(&[drop_test_table1]).await;

        // No data exists.
        util::check_read(
            &test_ctx,
            &fixed_schema_table,
            "Test read table after reopen",
            drop_test_table1,
            &[],
        )
        .await;
    });
}

#[test]
fn test_drop_create_same_table_rocks() {
    let rocksdb_ctxs = rocksdb_ctxs();
    for ctx in rocksdb_ctxs {
        test_drop_create_same_table(ctx);
    }
}

#[test]
fn test_drop_create_same_table_mem_wal() {
    let memory_ctxs = memory_ctxs();
    for ctx in memory_ctxs {
        test_drop_create_same_table(ctx);
    }
}

fn test_drop_create_same_table<T: EngineBuildContext>(engine_context: T) {
    test_drop_create_same_table_case::<T>(false, engine_context.clone());

    test_drop_create_same_table_case::<T>(true, engine_context);
}

#[test]
fn test_alter_schema_drop_create_rocks() {
    let rocksdb_ctxs = rocksdb_ctxs();
    for ctx in rocksdb_ctxs {
        test_alter_schema_drop_create(ctx);
    }
}

#[test]
fn test_alter_schema_drop_create_mem_wal() {
    let memory_ctxs = memory_ctxs();
    for ctx in memory_ctxs {
        test_alter_schema_drop_create(ctx);
    }
}

fn test_alter_schema_drop_create<T: EngineBuildContext>(engine_context: T) {
    let env = TestEnv::builder().build();
    let mut test_ctx = env.new_context(engine_context);

    env.block_on(async {
        test_ctx.open().await;

        let drop_test_table1 = "drop_test_table1";
        test_ctx.create_fixed_schema_table(drop_test_table1).await;

        // Alter schema.
        let old_schema = test_ctx.table(drop_test_table1).schema();
        let schema_builder = FixedSchemaTable::default_schema_builder()
            .add_normal_column(
                column_schema::Builder::new("add_double".to_string(), DatumKind::Double)
                    .is_nullable(true)
                    .build()
                    .unwrap(),
            )
            .unwrap();
        let new_schema = schema_builder
            .version(old_schema.version() + 1)
            .build()
            .unwrap();
        let request = AlterSchemaRequest {
            schema: new_schema.clone(),
            pre_schema_version: old_schema.version(),
        };
        let affected = test_ctx
            .try_alter_schema(drop_test_table1, request)
            .await
            .unwrap();
        assert_eq!(0, affected);

        // Drop table.
        assert!(test_ctx.drop_table(drop_test_table1).await);

        // Create same table again.
        let drop_test_table1 = "drop_test_table1";
        test_ctx.create_fixed_schema_table(drop_test_table1).await;

        test_ctx.reopen_with_tables(&[drop_test_table1]).await;
    });
}

#[test]
fn test_alter_options_drop_create_rocks() {
    let rocksdb_ctxs = rocksdb_ctxs();
    for ctx in rocksdb_ctxs {
        test_alter_options_drop_create(ctx);
    }
}

#[test]
fn test_alter_options_drop_create_mem_wal() {
    let memory_ctxs = memory_ctxs();
    for ctx in memory_ctxs {
        test_alter_options_drop_create(ctx);
    }
}

fn test_alter_options_drop_create<T: EngineBuildContext>(engine_context: T) {
    let env = TestEnv::builder().build();
    let mut test_ctx = env.new_context(engine_context);

    env.block_on(async {
        test_ctx.open().await;

        let drop_test_table1 = "drop_test_table1";
        test_ctx.create_fixed_schema_table(drop_test_table1).await;

        // Alter options.
        let mut new_opts = HashMap::new();
        new_opts.insert("arena_block_size".to_string(), "10240".to_string());

        let affected = test_ctx
            .try_alter_options(drop_test_table1, new_opts)
            .await
            .unwrap();
        assert_eq!(0, affected);

        // Drop table.
        assert!(test_ctx.drop_table(drop_test_table1).await);

        // Create same table again.
        let drop_test_table1 = "drop_test_table1";
        test_ctx.create_fixed_schema_table(drop_test_table1).await;

        test_ctx.reopen_with_tables(&[drop_test_table1]).await;
    });
}
