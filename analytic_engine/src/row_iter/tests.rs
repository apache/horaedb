// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

use async_trait::async_trait;
use common_types::{
    projected_schema::ProjectedSchema,
    record_batch::{RecordBatchWithKey, RecordBatchWithKeyBuilder},
    row::{
        contiguous::{ContiguousRowReader, ContiguousRowWriter, ProjectedContiguousRow},
        Row,
    },
    schema::{IndexInWriterSchema, RecordSchemaWithKey, Schema},
};
use common_util::define_result;
use snafu::Snafu;

use crate::row_iter::RecordBatchWithKeyIterator;

#[derive(Debug, Snafu)]
pub enum Error {}

define_result!(Error);

pub struct VectorIterator {
    schema: RecordSchemaWithKey,
    items: Vec<Option<RecordBatchWithKey>>,
    idx: usize,
}

impl VectorIterator {
    pub fn new(schema: RecordSchemaWithKey, items: Vec<RecordBatchWithKey>) -> Self {
        Self {
            schema,
            items: items.into_iter().map(Some).collect(),
            idx: 0,
        }
    }
}

#[async_trait]
impl RecordBatchWithKeyIterator for VectorIterator {
    type Error = Error;

    fn schema(&self) -> &RecordSchemaWithKey {
        &self.schema
    }

    async fn next_batch(&mut self) -> Result<Option<RecordBatchWithKey>> {
        if self.idx == self.items.len() {
            return Ok(None);
        }

        let ret = Ok(self.items[self.idx].take());
        self.idx += 1;

        ret
    }
}

pub fn build_record_batch_with_key(schema: Schema, rows: Vec<Row>) -> RecordBatchWithKey {
    assert!(schema.num_columns() > 1);
    let projection: Vec<usize> = (0..schema.num_columns()).collect();
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

pub async fn check_iterator<T: RecordBatchWithKeyIterator>(iter: &mut T, expected_rows: Vec<Row>) {
    let mut visited_rows = 0;
    while let Some(batch) = iter.next_batch().await.unwrap() {
        for row_idx in 0..batch.num_rows() {
            assert_eq!(batch.clone_row_at(row_idx), expected_rows[visited_rows]);
            visited_rows += 1;
        }
    }

    assert_eq!(visited_rows, expected_rows.len());
}
