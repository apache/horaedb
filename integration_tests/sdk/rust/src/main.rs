// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

use std::{
    sync::Arc,
    time::{self, SystemTime},
};

use ceresdb_client::{
    db_client::{Builder, DbClient, Mode},
    model::{
        sql_query::{Request as SqlQueryRequest, Response as SqlQueryResponse},
        value::Value,
        write::{point::PointBuilder, Request as WriteRequest},
    },
    RpcContext,
};

type TestDatas = (Vec<Vec<Value>>, Vec<Vec<Value>>);
const ENDPOINT: &str = "127.0.0.1:8831";

#[tokio::main]
async fn main() {
    println!("Begin test, endpoint:{ENDPOINT}");

    let client = Builder::new(ENDPOINT.to_string(), Mode::Direct).build();
    let rpc_ctx = RpcContext::default().database("public".to_string());
    let now = current_timestamp_ms();

    let test_datas = generate_test_datas(now);

    test_auto_create_table(&client, &rpc_ctx, now, &test_datas).await;
    test_add_column(&client, &rpc_ctx, now, &test_datas).await;

    print!("Test done")
}

async fn test_auto_create_table(
    client: &Arc<dyn DbClient>,
    rpc_ctx: &RpcContext,
    timestamp: i64,
    test_datas: &TestDatas,
) {
    println!("Test auto create table");

    drop_table_if_exists(client, rpc_ctx, timestamp).await;

    write(client, rpc_ctx, timestamp, &test_datas.0, false).await;

    let mut query_data = Vec::new();
    for data in &test_datas.0 {
        let one_query_data = data.clone().into_iter().take(4).collect();
        query_data.push(one_query_data);
    }
    sql_query(client, rpc_ctx, timestamp, &query_data).await;
}

async fn test_add_column(
    client: &Arc<dyn DbClient>,
    rpc_ctx: &RpcContext,
    timestamp: i64,
    test_datas: &TestDatas,
) {
    println!("Test add column");

    write(client, rpc_ctx, timestamp, &test_datas.1, true).await;

    let mut query_data = test_datas.0.clone();
    query_data.extend(test_datas.1.clone());
    sql_query(client, rpc_ctx, timestamp, &query_data).await;
}

async fn drop_table_if_exists(client: &Arc<dyn DbClient>, rpc_ctx: &RpcContext, timestamp: i64) {
    let test_table = format!("test_table_{timestamp}");
    let query_req = SqlQueryRequest {
        tables: vec![test_table.clone()],
        sql: format!("DROP TABLE IF EXISTS {test_table}"),
    };
    let _ = client.sql_query(rpc_ctx, &query_req).await.unwrap();
}

async fn sql_query(
    client: &Arc<dyn DbClient>,
    rpc_ctx: &RpcContext,
    timestamp: i64,
    test_data: &Vec<Vec<Value>>,
) {
    let test_table = format!("test_table_{timestamp}");
    let query_req = SqlQueryRequest {
        tables: vec![test_table.clone()],
        sql: format!("SELECT * from {test_table}"),
    };
    let resp = client.sql_query(rpc_ctx, &query_req).await.unwrap();
    let raw_rows = extract_raw_rows_from_sql_query(&resp);

    let expected = format_rows(test_data);
    let actual = format_rows(&raw_rows);
    assert_eq!(expected, actual);
}

async fn write(
    client: &Arc<dyn DbClient>,
    rpc_ctx: &RpcContext,
    timestamp: i64,
    test_data: &Vec<Vec<Value>>,
    new_column: bool,
) {
    let mut write_req = WriteRequest::default();
    let test_table = format!("test_table_{timestamp}");
    let mut test_points = Vec::with_capacity(test_data.len());
    for test_row in test_data {
        let point = {
            let timestamp_val = match &test_row[0] {
                Value::Timestamp(val) => *val,
                _ => unreachable!(),
            };
            let builder = PointBuilder::new(test_table.clone())
                .timestamp(timestamp_val)
                .field("old-field0".to_string(), test_row[1].clone())
                .field("old-field1".to_string(), test_row[2].clone())
                .tag("old-tagk1".to_string(), test_row[3].clone());

            if new_column {
                builder
                    .tag("new-tag".to_string(), test_row[4].clone())
                    .field("new-field".to_string(), test_row[5].clone())
                    .build()
                    .unwrap()
            } else {
                builder.build().unwrap()
            }
        };
        test_points.push(point);
    }
    write_req.add_points(test_points);

    let resp = client.write(rpc_ctx, &write_req).await.unwrap();
    assert_eq!(resp.success, 2);
    assert_eq!(resp.failed, 0);
}

fn generate_test_datas(timestamp: i64) -> (Vec<Vec<Value>>, Vec<Vec<Value>>) {
    let col0 = vec![
        Value::Timestamp(timestamp),
        Value::String("old-tagv0".to_string()),
        Value::Int64(123),
        Value::UInt64(1222223333334),
        Value::String("".to_string()),
        Value::UInt64(0),
    ];
    let col1 = vec![
        Value::Timestamp(timestamp),
        Value::String("old-tagv1".to_string()),
        Value::Int64(124),
        Value::UInt64(1222223333335),
        Value::String("".to_string()),
        Value::UInt64(0),
    ];

    let new_col0 = vec![
        Value::Timestamp(timestamp),
        Value::String("old-tagv0".to_string()),
        Value::Int64(123),
        Value::UInt64(1222223333334),
        Value::String("new-tagv0".to_string()),
        Value::UInt64(666666),
    ];
    let new_col1 = vec![
        Value::Timestamp(timestamp),
        Value::String("old-tagv1".to_string()),
        Value::Int64(124),
        Value::UInt64(1222223333335),
        Value::String("new-tagv1".to_string()),
        Value::UInt64(88888888),
    ];

    (vec![col0, col1], vec![new_col0, new_col1])
}

fn current_timestamp_ms() -> i64 {
    SystemTime::now()
        .duration_since(time::UNIX_EPOCH)
        .map(|duration| duration.as_millis() as i64)
        .unwrap_or(0)
}

fn extract_raw_rows_from_sql_query(resp: &SqlQueryResponse) -> Vec<Vec<Value>> {
    let mut raw_rows = Vec::with_capacity(resp.rows.len());
    for row in &resp.rows {
        let mut column_iter = row.columns().iter();
        // In the automatically created table schema, `tsid` column will be added by
        // CeresDB, we just ignore it.
        column_iter.next();
        let col_vals = column_iter.map(|col| col.value().clone()).collect();
        raw_rows.push(col_vals);
    }

    raw_rows
}

fn format_rows(rows: &Vec<Vec<Value>>) -> Vec<String> {
    let mut formatted_rows = Vec::new();
    for row in rows {
        let mut row_str = row
            .iter()
            .map(|col| format!("{col:?}"))
            .collect::<Vec<_>>();
        row_str.sort();
        formatted_rows.push(format!("{row_str:?}"));
    }

    formatted_rows.sort();
    formatted_rows
}
