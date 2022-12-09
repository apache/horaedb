// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

//! Utils to create table.

use std::{collections::HashMap, sync::Arc};

use common_types::{
    column_schema,
    datum::{Datum, DatumKind},
    projected_schema::ProjectedSchema,
    record_batch::RecordBatch,
    request_id::RequestId,
    row::{Row, RowGroup, RowGroupBuilder},
    schema::{self, Schema},
    table::{DEFAULT_CLUSTER_VERSION, DEFAULT_SHARD_ID},
    time::Timestamp,
};
use common_util::config::ReadableDuration;
use table_engine::{
    self,
    engine::{CreateTableRequest, TableState},
    predicate::Predicate,
    table::{GetRequest, ReadOptions, ReadOrder, ReadRequest, SchemaId, TableId, TableSeq},
};

use crate::{table_options, tests::row_util};

pub fn new_table_id(schema_id: u16, table_seq: u32) -> TableId {
    TableId::with_seq(SchemaId::from(schema_id), TableSeq::from(table_seq)).unwrap()
}

pub type RowTuple<'a> = (&'a str, Timestamp, &'a str, f64, f64, &'a str);
pub type RowTupleOpt<'a> = (
    &'a str,
    Timestamp,
    Option<&'a str>,
    Option<f64>,
    Option<f64>,
    Option<&'a str>,
);
pub type KeyTuple<'a> = (&'a str, Timestamp);

pub struct FixedSchemaTable {
    create_request: CreateTableRequest,
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

    #[inline]
    pub fn segment_duration_ms(&self) -> i64 {
        table_options::DEFAULT_SEGMENT_DURATION.as_millis() as i64
    }

    // Format of data: (key string, timestamp, string_tag, double_field1,
    // double_field2, string_field2)
    fn new_row(data: RowTuple) -> Row {
        row_util::new_row_6(data)
    }

    pub fn rows_to_row_group(&self, data: &[RowTuple]) -> RowGroup {
        let rows = data
            .iter()
            .copied()
            .map(FixedSchemaTable::new_row)
            .collect();

        self.new_row_group(rows)
    }

    pub fn rows_opt_to_row_group(&self, data: &[RowTupleOpt]) -> RowGroup {
        let rows = data
            .iter()
            .copied()
            .map(FixedSchemaTable::new_row_opt)
            .collect();

        self.new_row_group(rows)
    }

    fn new_row_group(&self, rows: Vec<Row>) -> RowGroup {
        RowGroupBuilder::with_rows(self.create_request.table_schema.clone(), rows)
            .unwrap()
            .build()
    }

    fn new_row_opt(data: RowTupleOpt) -> Row {
        row_util::new_row_6(data)
    }

    pub fn new_read_all_request(&self, opts: ReadOptions, read_order: ReadOrder) -> ReadRequest {
        new_read_all_request_with_order(self.create_request.table_schema.clone(), opts, read_order)
    }

    pub fn new_get_request(&self, key: KeyTuple) -> GetRequest {
        let primary_key = vec![key.0.into(), key.1.into()];

        GetRequest {
            request_id: RequestId::next_id(),
            projected_schema: ProjectedSchema::no_projection(
                self.create_request.table_schema.clone(),
            ),
            primary_key,
        }
    }

    pub fn new_get_request_from_row(&self, data: RowTuple) -> GetRequest {
        self.new_get_request((data.0, data.1))
    }

    pub fn assert_batch_eq_to_rows(&self, record_batches: &[RecordBatch], rows: &[RowTuple]) {
        let row_group = self.rows_to_row_group(rows);
        assert_batch_eq_to_row_group(record_batches, &row_group);
    }

    pub fn assert_row_eq(&self, data: RowTuple, row: Row) {
        row_util::assert_row_eq_6(data, row);
    }
}

pub fn read_opts_list() -> Vec<ReadOptions> {
    vec![
        ReadOptions::default(),
        ReadOptions {
            batch_size: 1,
            read_parallelism: 1,
            row_group_num_per_sst_reader: usize::MAX,
        },
        ReadOptions {
            batch_size: 1,
            read_parallelism: 4,
            row_group_num_per_sst_reader: usize::MAX,
        },
        ReadOptions {
            batch_size: 100,
            read_parallelism: 1,
            row_group_num_per_sst_reader: usize::MAX,
        },
        ReadOptions {
            batch_size: 100,
            read_parallelism: 4,
            row_group_num_per_sst_reader: usize::MAX,
        },
    ]
}

pub fn new_read_all_request_with_order(
    schema: Schema,
    opts: ReadOptions,
    order: ReadOrder,
) -> ReadRequest {
    ReadRequest {
        request_id: RequestId::next_id(),
        opts,
        projected_schema: ProjectedSchema::no_projection(schema),
        predicate: Arc::new(Predicate::empty()),
        order,
    }
}

pub fn new_read_all_request(schema: Schema, opts: ReadOptions) -> ReadRequest {
    new_read_all_request_with_order(schema, opts, ReadOrder::None)
}

pub fn assert_batch_eq_to_row_group(record_batches: &[RecordBatch], row_group: &RowGroup) {
    if record_batches.is_empty() {
        assert!(row_group.is_empty());
    }

    for record_batch in record_batches {
        assert_eq!(
            record_batch.schema().columns(),
            row_group.schema().columns()
        );
    }

    let mut cursor = RecordBatchesCursor::new(record_batches);

    for row in row_group.iter() {
        for (column_idx, datum) in row.iter().enumerate() {
            assert_eq!(
                &cursor.datum(column_idx),
                datum,
                "record_batches:{:?}, row_group:{:?}",
                record_batches,
                row_group
            );
        }
        cursor.step();
    }
}

struct RecordBatchesCursor<'a> {
    record_batches: &'a [RecordBatch],
    batch_idx: usize,
    row_idx_in_batch: usize,
}

impl<'a> RecordBatchesCursor<'a> {
    fn new(record_batches: &[RecordBatch]) -> RecordBatchesCursor {
        RecordBatchesCursor {
            record_batches,
            batch_idx: 0,
            row_idx_in_batch: 0,
        }
    }

    fn step(&mut self) {
        if self.batch_idx >= self.record_batches.len() {
            return;
        }

        self.row_idx_in_batch += 1;
        if self.row_idx_in_batch >= self.record_batches[self.batch_idx].num_rows() {
            self.batch_idx += 1;
            self.row_idx_in_batch = 0;
        }
    }

    fn datum(&self, column_idx: usize) -> Datum {
        let record_batch = &self.record_batches[self.batch_idx];
        let column_in_batch = record_batch.column(column_idx);
        column_in_batch.datum(self.row_idx_in_batch)
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
        self.create_request.table_name = table_name;
        self
    }

    pub fn table_id(mut self, table_id: TableId) -> Self {
        self.create_request.table_id = table_id;
        self
    }

    pub fn enable_ttl(mut self, enable_ttl: bool) -> Self {
        self.create_request.options.insert(
            table_engine::OPTION_KEY_ENABLE_TTL.to_string(),
            enable_ttl.to_string(),
        );
        self
    }

    pub fn ttl(mut self, duration: ReadableDuration) -> Self {
        self.create_request
            .options
            .insert(table_options::TTL.to_string(), duration.to_string());
        self
    }

    pub fn build_fixed(self) -> FixedSchemaTable {
        FixedSchemaTable {
            create_request: self.create_request,
        }
    }
}

impl Default for Builder {
    fn default() -> Self {
        Self {
            create_request: CreateTableRequest {
                catalog_name: "ceresdb".to_string(),
                schema_name: "public".to_string(),
                schema_id: SchemaId::from_u32(2),
                table_id: new_table_id(2, 1),
                table_name: "test_table".to_string(),
                table_schema: FixedSchemaTable::default_schema(),
                partition_info: None,
                engine: table_engine::ANALYTIC_ENGINE_TYPE.to_string(),
                options: HashMap::new(),
                state: TableState::Stable,
                shard_id: DEFAULT_SHARD_ID,
                cluster_version: DEFAULT_CLUSTER_VERSION,
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
        .auto_increment_column_id(true);

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
