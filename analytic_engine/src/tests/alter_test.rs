// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

//! Alter test

use std::collections::{BTreeMap, HashMap};

use common_types::{
    column_schema,
    datum::DatumKind,
    row::{RowGroup, RowGroupBuilder},
    schema::{self, Schema},
    time::Timestamp,
};
use log::info;
use table_engine::table::AlterSchemaRequest;

use crate::{
    setup::{EngineBuilder, MemWalEngineBuilder, RocksEngineBuilder},
    table_options::TableOptions,
    tests::{
        row_util,
        table::{self, FixedSchemaTable},
        util::{Null, TestContext, TestEnv},
    },
};

#[test]
fn test_alter_table_add_column_rocks() {
    test_alter_table_add_column::<RocksEngineBuilder>();
}

#[ignore = "Enable this test when manifest use another snapshot implementation"]
#[test]
fn test_alter_table_add_column_mem_wal() {
    test_alter_table_add_column::<MemWalEngineBuilder>();
}

fn test_alter_table_add_column<T: EngineBuilder>() {
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
        ];

        // Write data to table.
        let row_group = fixed_schema_table.rows_to_row_group(&rows);
        test_ctx.write_to_table(test_table1, row_group).await;

        alter_schema_same_schema_version_case(&test_ctx, test_table1).await;

        alter_schema_old_pre_version_case(&test_ctx, test_table1).await;

        alter_schema_add_column_case(&mut test_ctx, test_table1, start_ms, false).await;

        // Prepare another table for alter.
        let test_table2 = "test_table2";
        test_ctx.create_fixed_schema_table(test_table2).await;
        let row_group = fixed_schema_table.rows_to_row_group(&rows);
        test_ctx.write_to_table(test_table2, row_group).await;

        alter_schema_add_column_case(&mut test_ctx, test_table2, start_ms, true).await;
    });
}

// Add two columns:
// - add_string
// - add_double
fn add_columns(schema_builder: schema::Builder) -> schema::Builder {
    schema_builder
        .add_normal_column(
            column_schema::Builder::new("add_string".to_string(), DatumKind::String)
                .is_nullable(true)
                .build()
                .expect("should succeed build column schema"),
        )
        .unwrap()
        .add_normal_column(
            column_schema::Builder::new("add_double".to_string(), DatumKind::Double)
                .is_nullable(true)
                .build()
                .expect("should succeed build column schema"),
        )
        .unwrap()
}

async fn alter_schema_same_schema_version_case<T: EngineBuilder>(
    test_ctx: &TestContext<T>,
    table_name: &str,
) {
    info!("test alter_schema_same_schema_version_case");

    let mut schema_builder = FixedSchemaTable::default_schema_builder();
    schema_builder = add_columns(schema_builder);
    let new_schema = schema_builder.build().unwrap();

    let table = test_ctx.table(table_name);
    let old_schema = table.schema();

    let request = AlterSchemaRequest {
        schema: new_schema,
        pre_schema_version: old_schema.version(),
    };

    let res = test_ctx.try_alter_schema(table_name, request).await;
    assert!(res.is_err());
}

async fn alter_schema_old_pre_version_case<T: EngineBuilder>(
    test_ctx: &TestContext<T>,
    table_name: &str,
) {
    info!("test alter_schema_old_pre_version_case");

    let mut schema_builder = FixedSchemaTable::default_schema_builder();
    schema_builder = add_columns(schema_builder);

    let table = test_ctx.table(table_name);
    let old_schema = table.schema();

    let new_schema = schema_builder
        .version(old_schema.version() + 1)
        .build()
        .unwrap();

    let request = AlterSchemaRequest {
        schema: new_schema,
        pre_schema_version: old_schema.version() - 1,
    };

    let res = test_ctx.try_alter_schema(table_name, request).await;
    assert!(res.is_err());
}

async fn alter_schema_add_column_case<T: EngineBuilder>(
    test_ctx: &mut TestContext<T>,
    table_name: &str,
    start_ms: i64,
    flush: bool,
) {
    info!(
        "test alter_schema_add_column_case, table_name:{}",
        table_name
    );

    let mut schema_builder = FixedSchemaTable::default_schema_builder();
    schema_builder = add_columns(schema_builder);

    let old_schema = test_ctx.table(table_name).schema();

    let new_schema = schema_builder
        .version(old_schema.version() + 1)
        .build()
        .unwrap();

    let request = AlterSchemaRequest {
        schema: new_schema.clone(),
        pre_schema_version: old_schema.version(),
    };

    let affected = test_ctx
        .try_alter_schema(table_name, request)
        .await
        .unwrap();
    assert_eq!(1, affected);

    let rows = [
        (
            "key1",
            Timestamp::new(start_ms + 10),
            "tag1-1",
            11.0,
            110.0,
            "tag2-1",
            "add1-1",
            210.0,
        ),
        (
            "key2",
            Timestamp::new(start_ms + 10),
            "tag1-2",
            12.0,
            110.0,
            "tag2-2",
            "add1-2",
            220.0,
        ),
    ];
    let rows_vec = row_util::new_rows_8(&rows);
    let row_group = RowGroupBuilder::with_rows(new_schema.clone(), rows_vec)
        .unwrap()
        .build();

    // Write data with new schema.
    test_ctx.write_to_table(table_name, row_group).await;

    if flush {
        test_ctx.flush_table(table_name).await;
    }

    let new_schema_rows = [
        // We need to check null datum, so tuples have different types and we need to
        // convert it into row first.
        row_util::new_row_8((
            "key1",
            Timestamp::new(start_ms),
            "tag1-1",
            11.0,
            110.0,
            "tag2-1",
            Null,
            Null,
        )),
        row_util::new_row_8((
            "key1",
            Timestamp::new(start_ms + 10),
            "tag1-1",
            11.0,
            110.0,
            "tag2-1",
            "add1-1",
            210.0,
        )),
        row_util::new_row_8((
            "key2",
            Timestamp::new(start_ms),
            "tag1-2",
            12.0,
            110.0,
            "tag2-2",
            Null,
            Null,
        )),
        row_util::new_row_8((
            "key2",
            Timestamp::new(start_ms + 10),
            "tag1-2",
            12.0,
            110.0,
            "tag2-2",
            "add1-2",
            220.0,
        )),
    ];
    let new_schema_row_group =
        RowGroupBuilder::with_rows(new_schema.clone(), new_schema_rows.to_vec())
            .unwrap()
            .build();

    // Read data using new schema.
    check_read_row_group(
        test_ctx,
        "Test read new schema after add columns",
        table_name,
        &new_schema,
        &new_schema_row_group,
    )
    .await;

    let old_schema_rows = [
        (
            "key1",
            Timestamp::new(start_ms),
            "tag1-1",
            11.0,
            110.0,
            "tag2-1",
        ),
        (
            "key1",
            Timestamp::new(start_ms + 10),
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
            Timestamp::new(start_ms + 10),
            "tag1-2",
            12.0,
            110.0,
            "tag2-2",
        ),
    ];
    let old_schema_rows_vec = row_util::new_rows_6(&old_schema_rows);
    let old_schema_row_group = RowGroupBuilder::with_rows(old_schema.clone(), old_schema_rows_vec)
        .unwrap()
        .build();

    // Read data using old schema.
    check_read_row_group(
        test_ctx,
        "Test read old schema after add columns",
        table_name,
        &old_schema,
        &old_schema_row_group,
    )
    .await;

    // Reopen db.
    test_ctx.reopen_with_tables(&[table_name]).await;

    // Read again after reopen.
    check_read_row_group(
        test_ctx,
        "Test read after reopen",
        table_name,
        &new_schema,
        &new_schema_row_group,
    )
    .await;
}

async fn check_read_row_group<T: EngineBuilder>(
    test_ctx: &TestContext<T>,
    msg: &str,
    table_name: &str,
    schema: &Schema,
    row_group: &RowGroup,
) {
    for read_opts in table::read_opts_list() {
        info!("{}, opts:{:?}", msg, read_opts);

        let record_batches = test_ctx
            .read_table(
                table_name,
                table::new_read_all_request(schema.clone(), read_opts),
            )
            .await;

        table::assert_batch_eq_to_row_group(&record_batches, row_group);
    }
}

#[test]
fn test_alter_table_options_rocks() {
    test_alter_table_options::<RocksEngineBuilder>();
}

#[ignore = "Enable this test when manifest use another snapshot implementation"]
#[test]
fn test_alter_table_options_mem_wal() {
    test_alter_table_options::<MemWalEngineBuilder>();
}

fn test_alter_table_options<T: EngineBuilder>() {
    let env = TestEnv::builder().build();
    let mut test_ctx = env.new_context::<T>();

    env.block_on(async {
        test_ctx.open().await;

        let test_table1 = "test_table1";
        test_ctx.create_fixed_schema_table(test_table1).await;

        let opts = test_ctx.table(test_table1).options();

        let default_opts_map = default_options();

        assert_options_eq(&default_opts_map, &opts);

        alter_immutable_option_case(&test_ctx, test_table1, "segment_duration", "20d").await;

        alter_immutable_option_case(&test_ctx, test_table1, "bucket_duration", "20d").await;

        alter_immutable_option_case(&test_ctx, test_table1, "update_mode", "Append").await;

        alter_mutable_option_case(&mut test_ctx, test_table1, "enable_ttl", "false").await;
        alter_mutable_option_case(&mut test_ctx, test_table1, "enable_ttl", "true").await;

        alter_mutable_option_case(&mut test_ctx, test_table1, "arena_block_size", "10240").await;

        alter_mutable_option_case(&mut test_ctx, test_table1, "write_buffer_size", "1024000").await;

        alter_mutable_option_case(
            &mut test_ctx,
            test_table1,
            "num_rows_per_row_group",
            "10000",
        )
        .await;
    });
}

async fn alter_immutable_option_case<T: EngineBuilder>(
    test_ctx: &TestContext<T>,
    table_name: &str,
    opt_key: &str,
    opt_value: &str,
) {
    let old_opts = test_ctx.table(table_name).options();

    let mut new_opts = HashMap::new();
    new_opts.insert(opt_key.to_string(), opt_value.to_string());

    let affected = test_ctx
        .try_alter_options(table_name, new_opts)
        .await
        .unwrap();
    assert_eq!(1, affected);

    let opts_after_alter = test_ctx.table(table_name).options();
    assert_options_eq(&old_opts, &opts_after_alter);
}

async fn alter_mutable_option_case<T: EngineBuilder>(
    test_ctx: &mut TestContext<T>,
    table_name: &str,
    opt_key: &str,
    opt_value: &str,
) {
    let mut expect_opts = test_ctx.table(table_name).options();
    expect_opts.insert(opt_key.to_string(), opt_value.to_string());

    let mut new_opts = HashMap::new();
    new_opts.insert(opt_key.to_string(), opt_value.to_string());

    let affected = test_ctx
        .try_alter_options(table_name, new_opts)
        .await
        .unwrap();
    assert_eq!(1, affected);

    let opts_after_alter = test_ctx.table(table_name).options();
    assert_options_eq(&expect_opts, &opts_after_alter);

    // Reopen table.
    test_ctx.reopen_with_tables(&[table_name]).await;

    let opts_after_alter = test_ctx.table(table_name).options();
    assert_options_eq(&expect_opts, &opts_after_alter);
}

fn assert_options_eq(left: &HashMap<String, String>, right: &HashMap<String, String>) {
    let sorted_left: BTreeMap<_, _> = left.iter().collect();
    let sorted_right: BTreeMap<_, _> = right.iter().collect();

    assert_eq!(sorted_left, sorted_right);
}

fn default_options() -> HashMap<String, String> {
    let table_opts = TableOptions::default();

    table_opts.to_raw_map()
}
