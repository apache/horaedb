// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

use bytes::Bytes;
use sqlparser::ast::{BinaryOperator, Expr, Value};

use crate::{
    column_schema,
    datum::{Datum, DatumKind},
    projected_schema::ProjectedSchema,
    record_batch::{RecordBatchWithKey, RecordBatchWithKeyBuilder},
    row::{
        contiguous::{ContiguousRowReader, ContiguousRowWriter, ProjectedContiguousRow},
        Row,
    },
    schema,
    schema::{IndexInWriterSchema, Schema},
    string::StringBytes,
    time::Timestamp,
};

fn base_schema_builder() -> schema::Builder {
    schema::Builder::new()
        .auto_increment_column_id(true)
        .add_key_column(
            column_schema::Builder::new("key1".to_string(), DatumKind::Varbinary)
                .build()
                .expect("should succeed build column schema"),
        )
        .unwrap()
        .add_key_column(
            column_schema::Builder::new("key2".to_string(), DatumKind::Timestamp)
                .build()
                .expect("should succeed build column schema"),
        )
        .unwrap()
        .add_normal_column(
            column_schema::Builder::new("field1".to_string(), DatumKind::Double)
                .is_nullable(true)
                .build()
                .expect("should succeed build column schema"),
        )
        .unwrap()
        .add_normal_column(
            column_schema::Builder::new("field2".to_string(), DatumKind::String)
                .is_nullable(true)
                .build()
                .expect("should succeed build column schema"),
        )
        .unwrap()
}

fn default_value_schema_builder() -> schema::Builder {
    schema::Builder::new()
        .auto_increment_column_id(true)
        .add_key_column(
            column_schema::Builder::new("key1".to_string(), DatumKind::Varbinary)
                .build()
                .expect("should succeed build column schema"),
        )
        .unwrap()
        .add_key_column(
            column_schema::Builder::new("key2".to_string(), DatumKind::Timestamp)
                .build()
                .expect("should succeed build column schema"),
        )
        .unwrap()
        .add_normal_column(
            // The data type of column and its default value will not be the same in most time.
            // So we need check if the type coercion is legal and do type coercion when legal.
            // In he following, the data type of column is `Int64`, and the type of default value
            // expr is `Int64`. So we use this column to cover the test, which has the same type.
            column_schema::Builder::new("field1".to_string(), DatumKind::Int64)
                .default_value(Some(Expr::Value(Value::Number("10".to_string(), false))))
                .build()
                .expect("should succeed build column schema"),
        )
        .unwrap()
        .add_normal_column(
            // The data type of column is `UInt32`, and the type of default value expr is `Int64`.
            // So we use this column to cover the test, which has different type.
            column_schema::Builder::new("field2".to_string(), DatumKind::UInt32)
                .default_value(Some(Expr::Value(Value::Number("20".to_string(), false))))
                .build()
                .expect("should succeed build column schema"),
        )
        .unwrap()
        .add_normal_column(
            column_schema::Builder::new("field3".to_string(), DatumKind::UInt32)
                .default_value(Some(Expr::BinaryOp {
                    left: Box::new(Expr::Value(Value::Number("1".to_string(), false))),
                    op: BinaryOperator::Plus,
                    right: Box::new(Expr::Value(Value::Number("2".to_string(), false))),
                }))
                .build()
                .expect("should succeed build column schema"),
        )
        .unwrap()
}

/// Build a schema for testing:
/// (key1(varbinary), key2(timestamp), field1(double), field2(string))
pub fn build_schema() -> Schema {
    base_schema_builder().build().unwrap()
}

/// Build a schema for testing:
/// (key1(varbinary), key2(timestamp), field1(int64, default 10),
/// field2(uint32, default 20)), field3(uint32, default 1 + 2)
pub fn build_default_value_schema() -> Schema {
    default_value_schema_builder().build().unwrap()
}

pub fn build_projected_schema() -> ProjectedSchema {
    let schema = build_schema();
    assert!(schema.num_columns() > 1);
    let projection: Vec<usize> = (0..schema.num_columns() - 1).collect();
    ProjectedSchema::new(schema, Some(projection)).unwrap()
}

pub fn build_row(key1: &[u8], key2: i64, field1: f64, field2: &str) -> Row {
    let datums = vec![
        Datum::Varbinary(Bytes::copy_from_slice(key1)),
        Datum::Timestamp(Timestamp::new(key2)),
        Datum::Double(field1),
        Datum::String(StringBytes::from(field2)),
    ];

    Row::from_datums(datums)
}

pub fn build_row_opt(key1: &[u8], key2: i64, field1: Option<f64>, field2: Option<&str>) -> Row {
    let datums = vec![
        Datum::Varbinary(Bytes::copy_from_slice(key1)),
        Datum::Timestamp(Timestamp::new(key2)),
        field1.map(Datum::Double).unwrap_or(Datum::Null),
        field2
            .map(|v| Datum::String(StringBytes::from(v)))
            .unwrap_or(Datum::Null),
    ];

    Row::from_datums(datums)
}

pub fn build_rows() -> Vec<Row> {
    vec![
        build_row(b"binary key", 1000000, 10.0, "string value"),
        build_row(b"binary key1", 1000001, 11.0, "string value 1"),
        build_row_opt(b"binary key2", 1000002, None, Some("string value 2")),
        build_row_opt(b"binary key3", 1000003, Some(13.0), None),
        build_row_opt(b"binary key4", 1000004, None, None),
    ]
}

pub fn build_record_batch_with_key_by_rows(rows: Vec<Row>) -> RecordBatchWithKey {
    let schema = build_schema();
    assert!(schema.num_columns() > 1);
    let projection: Vec<usize> = (0..schema.num_columns() - 1).collect();
    let projected_schema = ProjectedSchema::new(schema.clone(), Some(projection)).unwrap();
    let row_projected_schema = projected_schema.try_project_with_key(&schema).unwrap();

    let mut builder =
        RecordBatchWithKeyBuilder::with_capacity(projected_schema.to_record_schema_with_key(), 2);
    let index_in_writer = IndexInWriterSchema::for_same_schema(schema.num_columns());

    let mut buf = Vec::new();
    for row in rows {
        let mut writer = ContiguousRowWriter::new(&mut buf, &schema, &index_in_writer);

        writer.write_row(&row).unwrap();

        let source_row = ContiguousRowReader::with_schema(&buf, &schema);
        let projected_row = ProjectedContiguousRow::new(source_row, &row_projected_schema);
        builder
            .append_projected_contiguous_row(&projected_row)
            .unwrap();
    }
    builder.build().unwrap()
}

pub fn check_record_batch_with_key_with_rows(
    record_batch_with_key: &RecordBatchWithKey,
    row_num: usize,
    column_num: usize,
    rows: Vec<Row>,
) -> bool {
    for (i, row) in rows.iter().enumerate().take(row_num) {
        for j in 0..column_num {
            let datum = &row[j];
            let datum2 = record_batch_with_key.column(j).datum(i);

            if *datum != datum2 {
                return false;
            }
        }
    }
    true
}
