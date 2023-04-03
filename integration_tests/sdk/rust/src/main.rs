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

const ENDPOINT: &str = "127.0.0.1:8831";

struct TestDatas {
    col_names: Vec<String>,
    rows: Vec<Vec<Value>>,
}

impl TestDatas {
    fn pick_rows_for_write(&self, new_column: bool) -> Vec<Vec<Value>> {
        if !new_column {
            self.rows.iter().take(2).cloned().collect::<Vec<_>>()
        } else {
            vec![self.rows[2].clone(), self.rows[3].clone()]
        }
    }

    fn pick_rows_for_query_check(&self, new_column: bool) -> Vec<Vec<(String, Value)>> {
        let mut expected_rows = Vec::new();
        if !new_column {
            let rows = self
                .rows
                .iter()
                .take(2)
                .map(|row| row.iter().take(4).cloned().collect::<Vec<_>>());

            for row in rows {
                let col_names = self.col_names.iter().take(4).cloned();
                let row = col_names.zip(row.into_iter()).collect::<Vec<_>>();
                expected_rows.push(row);
            }
        } else {
            let rows = self.rows.iter().cloned();

            for row in rows {
                let col_names = self.col_names.iter().cloned();
                let row = col_names.zip(row.into_iter()).collect::<Vec<_>>();
                expected_rows.push(row);
            }
        };

        expected_rows
    }
}

#[tokio::main]
async fn main() {
    println!("Begin test, endpoint:{ENDPOINT}");

    let client = Builder::new(ENDPOINT.to_string(), Mode::Direct).build();
    let rpc_ctx = RpcContext::default().database("public".to_string());
    let now = current_timestamp_ms();

    let test_datas = generate_test_datas(now);

    test_auto_create_table(&client, &rpc_ctx, now, &test_datas).await;
    test_add_column(&client, &rpc_ctx, now, &test_datas).await;

    drop_table_if_exists(&client, &rpc_ctx, now).await;
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

    write(client, rpc_ctx, timestamp, test_datas, false).await;
    sql_query(client, rpc_ctx, timestamp, test_datas, false).await;
}

async fn test_add_column(
    client: &Arc<dyn DbClient>,
    rpc_ctx: &RpcContext,
    timestamp: i64,
    test_datas: &TestDatas,
) {
    println!("Test add column");

    write(client, rpc_ctx, timestamp, test_datas, true).await;
    sql_query(client, rpc_ctx, timestamp, test_datas, true).await;
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
    test_data: &TestDatas,
    new_column: bool,
) {
    let all_columns = test_data.col_names.clone();
    let selections = if !new_column {
        format!(
            "`{}`,`{}`,`{}`,`{}`",
            all_columns[0], all_columns[1], all_columns[2], all_columns[3]
        )
    } else {
        format!(
            "`{}`,`{}`,`{}`,`{}`,`{}`,`{}`",
            all_columns[0],
            all_columns[1],
            all_columns[2],
            all_columns[3],
            all_columns[4],
            all_columns[5]
        )
    };

    let test_table = format!("test_table_{timestamp}");
    let query_req = SqlQueryRequest {
        tables: vec![test_table.clone()],
        sql: format!("SELECT {selections} from {test_table}"),
    };
    let resp = client.sql_query(rpc_ctx, &query_req).await.unwrap();
    assert_eq!(resp.affected_rows, 0);

    let resp_rows = extract_rows_from_sql_query(&resp);
    let expected_rows = test_data.pick_rows_for_query_check(new_column);
    let expected = format_rows(&expected_rows);
    let actual = format_rows(&resp_rows);
    assert_eq!(expected, actual);
}

async fn write(
    client: &Arc<dyn DbClient>,
    rpc_ctx: &RpcContext,
    timestamp: i64,
    test_data: &TestDatas,
    new_column: bool,
) {
    let test_table = format!("test_table_{timestamp}");
    let mut write_req = WriteRequest::default();
    let mut points = Vec::new();

    let rows = test_data.pick_rows_for_write(new_column);
    for row in rows {
        let point = {
            let builder = PointBuilder::new(test_table.clone())
                .timestamp(timestamp)
                .tag(test_data.col_names[1].clone(), row[1].clone())
                .field(test_data.col_names[2].clone(), row[2].clone())
                .field(test_data.col_names[3].clone(), row[3].clone());

            if new_column {
                builder
                    .tag(test_data.col_names[4].clone(), row[4].clone())
                    .field(test_data.col_names[5].clone(), row[5].clone())
                    .build()
                    .unwrap()
            } else {
                builder.build().unwrap()
            }
        };
        points.push(point);
    }
    write_req.add_points(points);

    let resp = client.write(rpc_ctx, &write_req).await.unwrap();
    assert_eq!(resp.success, 2);
    assert_eq!(resp.failed, 0);
}

fn generate_test_datas(timestamp: i64) -> TestDatas {
    let col_names = vec![
        "timestamp".to_string(),
        "old-tag".to_string(),
        "old-field0".to_string(),
        "old-field1".to_string(),
        "new-tag".to_string(),
        "new-field".to_string(),
    ];

    let rows = vec![
        vec![
            Value::Timestamp(timestamp),
            Value::String("old-tagv0".to_string()),
            Value::Int64(123),
            Value::UInt64(1222223333334),
            Value::String("".to_string()),
            Value::UInt64(0),
        ],
        vec![
            Value::Timestamp(timestamp),
            Value::String("old-tagv1".to_string()),
            Value::Int64(124),
            Value::UInt64(1222223333335),
            Value::String("".to_string()),
            Value::UInt64(0),
        ],
        vec![
            Value::Timestamp(timestamp),
            Value::String("old-tagv0".to_string()),
            Value::Int64(123),
            Value::UInt64(1222223333334),
            Value::String("new-tagv0".to_string()),
            Value::UInt64(666666),
        ],
        vec![
            Value::Timestamp(timestamp),
            Value::String("old-tagv1".to_string()),
            Value::Int64(124),
            Value::UInt64(1222223333335),
            Value::String("new-tagv1".to_string()),
            Value::UInt64(88888888),
        ],
    ];

    TestDatas { col_names, rows }
}

fn current_timestamp_ms() -> i64 {
    SystemTime::now()
        .duration_since(time::UNIX_EPOCH)
        .map(|duration| duration.as_millis() as i64)
        .unwrap_or(0)
}

fn extract_rows_from_sql_query(resp: &SqlQueryResponse) -> Vec<Vec<(String, Value)>> {
    let mut rows = Vec::with_capacity(resp.rows.len());
    for row in &resp.rows {
        let col_vals = row
            .columns()
            .iter()
            .map(|col| (col.name().to_string(), col.value().clone()))
            .collect();
        rows.push(col_vals);
    }

    rows
}

fn format_rows(rows: &[Vec<(String, Value)>]) -> Vec<String> {
    let mut sorted_row_strs = rows
        .iter()
        .map(|row| {
            let mut sorted_row = row.clone();
            sorted_row.sort_by(|col1, col2| col1.0.cmp(&col2.0));
            let sorted_row = sorted_row.into_iter().map(|col| col.1).collect::<Vec<_>>();

            format!("{sorted_row:?}")
        })
        .collect::<Vec<_>>();
    sorted_row_strs.sort();

    sorted_row_strs
}
