// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Request serialization bench.

use bytes_ext::Bytes;
use common_types::{
    column_schema,
    datum::{Datum, DatumKind},
    row::{Row, RowGroup},
    schema,
    schema::Schema,
    string::StringBytes,
    time::Timestamp,
};
use fb_util::remote_engine_generated::fbprotocol::WriteRequest as FBWriteRequest;
use horaedbproto::remote_engine::WriteRequest as PbWriteRequest;
use prost::Message;
use table_engine::{
    remote::model::{TableIdentifier, WriteRequest},
    table::WriteRequest as TableWriteRequest,
};

pub struct RequestSerializationBench {}

impl Default for RequestSerializationBench {
    fn default() -> Self {
        Self::new()
    }
}

impl RequestSerializationBench {
    pub fn new() -> Self {
        RequestSerializationBench {}
    }

    pub fn run_bench(&self) {
        // build table schema

        let schema = build_schema();
        let rows = build_rows();

        let row_group: RowGroup = RowGroup::new_unchecked(schema.clone(), rows.clone());
        let table_id = TableIdentifier {
            catalog: "catalog".to_string(),
            schema: "schema".to_string(),
            table: "table".to_string(),
        };

        let table_request = WriteRequest {
            table: table_id.clone(),
            write_request: TableWriteRequest {
                row_group: row_group.clone(),
            },
        };
        let table_request2 = WriteRequest {
            table: table_id,
            write_request: TableWriteRequest { row_group },
        };

        let s1 = std::time::Instant::now();
        let req1 = table_request.convert_into_pb();
        let mut buf = Vec::new();
        req1.unwrap().encode(&mut buf).unwrap();
        let s2 = std::time::Instant::now();

        PbWriteRequest::decode(&*buf).expect("Message decode failed");
        let s3 = std::time::Instant::now();

        let req2 = table_request2.convert_into_fb();
        let s4 = std::time::Instant::now();

        req2.unwrap().deserialize::<FBWriteRequest>().unwrap();
        let s5 = std::time::Instant::now();

        println!(
            "pb encode: {:?}, pb decode: {:?}, fb encode: {:?}, fb decode: {:?}",
            s2 - s1,
            s3 - s2,
            s4 - s3,
            s5 - s4
        );
    }
}

/// Build a schema for testing, which contains 6 columns:
/// - key1(varbinary)
/// - key2(timestamp)
/// - field1(double)
/// - field2(string)
/// - field3(Time)
/// - field4(Date)
pub fn build_schema() -> Schema {
    base_schema_builder().build().unwrap()
}

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
        .add_normal_column(
            column_schema::Builder::new("field3".to_string(), DatumKind::Date)
                .is_nullable(true)
                .build()
                .expect("should succeed build column schema"),
        )
        .unwrap()
        .add_normal_column(
            column_schema::Builder::new("field4".to_string(), DatumKind::Time)
                .is_nullable(true)
                .build()
                .expect("should succeed build column schema"),
        )
        .unwrap()
        .primary_key_indexes(vec![0, 1])
}

pub fn build_rows() -> Vec<Row> {
    let mut v = Vec::new();
    for _ in 0..1000 {
        let r = build_row(b"binary key", 1000000, 10.0, "string value", 0, 0);
        v.push(r);
    }
    v
}

pub fn build_row(
    key1: &[u8],
    key2: i64,
    field1: f64,
    field2: &str,
    field3: i32,
    field4: i64,
) -> Row {
    let datums = vec![
        Datum::Varbinary(Bytes::copy_from_slice(key1)),
        Datum::Timestamp(Timestamp::new(key2)),
        Datum::Double(field1),
        Datum::String(StringBytes::from(field2)),
        Datum::Date(field3),
        Datum::Time(field4),
    ];

    Row::from_datums(datums)
}

pub fn build_row_opt(
    key1: &[u8],
    key2: i64,
    field1: Option<f64>,
    field2: Option<&str>,
    field3: Option<i32>,
    field4: Option<i64>,
) -> Row {
    let datums = vec![
        Datum::Varbinary(Bytes::copy_from_slice(key1)),
        Datum::Timestamp(Timestamp::new(key2)),
        field1.map(Datum::Double).unwrap_or(Datum::Null),
        field2
            .map(|v| Datum::String(StringBytes::from(v)))
            .unwrap_or(Datum::Null),
        field3.map(Datum::Date).unwrap_or(Datum::Null),
        field4.map(Datum::Time).unwrap_or(Datum::Null),
    ];

    Row::from_datums(datums)
}
