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

//! Utils to create table.

use std::collections::HashMap;

use common_types::{
    column_schema,
    datum::{Datum, DatumKind},
    row::{Row, RowGroup},
    schema::{self, Schema},
    table::DEFAULT_SHARD_ID,
    time::Timestamp,
};
use table_engine::{
    self,
    engine::{CreateTableParams, CreateTableRequest, TableState},
    table::{SchemaId, TableId, TableSeq},
};
use time_ext::ReadableDuration;

use crate::util::start_ms;

pub fn new_row_6<C0, C1, C2, C3, C4, C5>(data: (C0, C1, C2, C3, C4, C5)) -> Row
where
    C0: Into<Datum>,
    C1: Into<Datum>,
    C2: Into<Datum>,
    C3: Into<Datum>,
    C4: Into<Datum>,
    C5: Into<Datum>,
{
    let cols = vec![
        data.0.into(),
        data.1.into(),
        data.2.into(),
        data.3.into(),
        data.4.into(),
        data.5.into(),
    ];

    Row::from_datums(cols)
}

pub type WriteRequestTuple = (String, Timestamp, String, f64, f64, String);
pub type RowTuple<'a> = (&'a str, Timestamp, &'a str, f64, f64, &'a str);

pub fn new_table_id(schema_id: u16, table_seq: u32) -> TableId {
    TableId::with_seq(SchemaId::from(schema_id), TableSeq::from(table_seq)).unwrap()
}

pub struct RowTupleGenerator {}

pub struct FixedSchemaTable {
    create_request: CreateTableRequest,
    write_requests: Vec<WriteRequestTuple>,
}

impl FixedSchemaTable {
    pub fn builder() -> Builder {
        Builder::default()
    }

    fn default_schema() -> Schema {
        Self::default_schema_builder().build().unwrap()
    }

    pub fn default_schema_builder() -> schema::Builder {
        create_schema_builder(
            // Key columns
            &[("key", DatumKind::String), ("ts", DatumKind::Timestamp)],
            // Normal columns
            &[
                ("string_tag", DatumKind::String),
                ("double_field1", DatumKind::Double),
                ("double_field2", DatumKind::Double),
                ("string_field2", DatumKind::String),
            ],
        )
    }

    #[inline]
    pub fn table_id(&self) -> TableId {
        self.create_request.table_id
    }

    #[inline]
    pub fn create_request(&self) -> &CreateTableRequest {
        &self.create_request
    }

    fn new_row(data: RowTuple) -> Row {
        new_row_6(data)
    }

    pub fn rows_to_row_group(&self, data: &[RowTuple]) -> RowGroup {
        let rows = data
            .iter()
            .copied()
            .map(FixedSchemaTable::new_row)
            .collect();

        self.new_row_group(rows)
    }

    fn new_row_group(&self, rows: Vec<Row>) -> RowGroup {
        RowGroup::try_new(self.create_request.params.table_schema.clone(), rows).unwrap()
    }

    pub fn prepare_write_requests(&mut self, batch_size: usize) {
        let start_ms = start_ms();
        self.write_requests.clear();
        (0..batch_size).for_each(|idx| {
            self.write_requests.push((
                format!("key_{idx}"),
                Timestamp::new(start_ms + idx as i64),
                format!("tag1_{idx}"),
                11.0,
                110.0,
                format!("tag2_{idx}"),
            ))
        });
    }

    pub fn row_tuples(&self) -> Vec<RowTuple> {
        self.write_requests
            .iter()
            .map(|x| (x.0.as_str(), x.1, x.2.as_str(), x.3, x.4, x.5.as_str()))
            .collect()
    }
}

#[must_use]
pub struct Builder {
    create_request: CreateTableRequest,
}

impl Builder {
    pub fn schema_id(mut self, schema_id: SchemaId) -> Self {
        self.create_request.schema_id = schema_id;
        self
    }

    pub fn table_name(mut self, table_name: String) -> Self {
        self.create_request.params.table_name = table_name;
        self
    }

    pub fn table_id(mut self, table_id: TableId) -> Self {
        self.create_request.table_id = table_id;
        self
    }

    pub fn enable_ttl(mut self, enable_ttl: bool) -> Self {
        self.create_request.params.table_options.insert(
            common_types::OPTION_KEY_ENABLE_TTL.to_string(),
            enable_ttl.to_string(),
        );
        self
    }

    pub fn ttl(mut self, duration: ReadableDuration) -> Self {
        self.create_request
            .params
            .table_options
            .insert(common_types::TTL.to_string(), duration.to_string());
        self
    }

    pub fn build_fixed(self) -> FixedSchemaTable {
        FixedSchemaTable {
            create_request: self.create_request,
            write_requests: Vec::new(),
        }
    }
}

impl Default for Builder {
    fn default() -> Self {
        let params = CreateTableParams {
            catalog_name: "horaedb".to_string(),
            schema_name: "public".to_string(),
            table_name: "test_table".to_string(),
            table_schema: FixedSchemaTable::default_schema(),
            partition_info: None,
            engine: table_engine::ANALYTIC_ENGINE_TYPE.to_string(),
            table_options: HashMap::new(),
        };

        Self {
            create_request: CreateTableRequest {
                params,
                schema_id: SchemaId::from_u32(2),
                table_id: new_table_id(2, 1),
                state: TableState::Stable,
                shard_id: DEFAULT_SHARD_ID,
            },
        }
    }
}

// Format of input slice: &[ ( column name, column type ) ]
pub fn create_schema_builder(
    key_tuples: &[(&str, DatumKind)],
    normal_tuples: &[(&str, DatumKind)],
) -> schema::Builder {
    assert!(!key_tuples.is_empty());

    let mut schema_builder = schema::Builder::with_capacity(key_tuples.len() + normal_tuples.len())
        .auto_increment_column_id(true)
        .primary_key_indexes((0..key_tuples.len()).collect());

    for tuple in key_tuples {
        // Key column is not nullable.
        let column_schema = column_schema::Builder::new(tuple.0.to_string(), tuple.1)
            .is_nullable(false)
            .build()
            .expect("Should succeed to build key column schema");
        schema_builder = schema_builder.add_key_column(column_schema).unwrap();
    }

    for tuple in normal_tuples {
        let column_schema = column_schema::Builder::new(tuple.0.to_string(), tuple.1)
            .is_nullable(true)
            .build()
            .expect("Should succeed to build normal column schema");
        schema_builder = schema_builder.add_normal_column(column_schema).unwrap();
    }

    schema_builder
}
