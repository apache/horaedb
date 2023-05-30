// Copyright 2022-2023 CeresDB Project Authors. Licensed under Apache-2.0.

use std::sync::Arc;

use arrow::{
    array::{BooleanArray, StringArray},
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use async_trait::async_trait;
use query_engine::executor::RecordBatchVec;
use query_frontend::plan::DescribeTablePlan;
use snafu::{ResultExt, Snafu};
use table_engine::table::TableRef;

use crate::interpreter::{
    Describe, Interpreter, InterpreterPtr, Output, Result as InterpreterResult,
};

#[derive(Debug, Snafu)]
pub enum Error {}

define_result!(Error);

pub struct DescribeInterpreter {
    plan: DescribeTablePlan,
}

impl DescribeInterpreter {
    pub fn create(plan: DescribeTablePlan) -> InterpreterPtr {
        Box::new(Self { plan })
    }

    async fn execute_describe(self: Box<Self>) -> Result<Output> {
        let DescribeTablePlan { table } = self.plan;

        Self::table_ref_to_record_batch(table).map(Output::Records)
    }

    fn table_ref_to_record_batch(table_ref: TableRef) -> Result<RecordBatchVec> {
        let table_schema = table_ref.schema();
        let num_columns = table_schema.num_columns();

        let mut names = Vec::with_capacity(num_columns);
        let mut types = Vec::with_capacity(num_columns);
        let mut is_primary_keys = Vec::with_capacity(num_columns);
        let mut is_nullables = Vec::with_capacity(num_columns);
        let mut is_tags = Vec::with_capacity(num_columns);
        for (idx, col) in table_schema.columns().iter().enumerate() {
            names.push(col.name.to_string());
            types.push(col.data_type.to_string());
            is_primary_keys.push(table_schema.is_primary_key_index(&idx));
            is_nullables.push(col.is_nullable);
            is_tags.push(col.is_tag);
        }

        let schema = Schema::new(vec![
            Field::new("name", DataType::Utf8, false),
            Field::new("type", DataType::Utf8, false),
            Field::new("is_primary", DataType::Boolean, false),
            Field::new("is_nullable", DataType::Boolean, false),
            Field::new("is_tag", DataType::Boolean, false),
        ]);

        let arrow_record_batch = RecordBatch::try_new(
            Arc::new(schema),
            vec![
                Arc::new(StringArray::from(names)),
                Arc::new(StringArray::from(types)),
                Arc::new(BooleanArray::from(is_primary_keys)),
                Arc::new(BooleanArray::from(is_nullables)),
                Arc::new(BooleanArray::from(is_tags)),
            ],
        )
        .unwrap();

        let record_batch = arrow_record_batch.try_into().unwrap();

        Ok(vec![record_batch])
    }
}

#[async_trait]
impl Interpreter for DescribeInterpreter {
    async fn execute(self: Box<Self>) -> InterpreterResult<Output> {
        self.execute_describe().await.context(Describe)
    }
}
