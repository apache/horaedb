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

use std::{convert::TryInto, sync::Arc};

use arrow::{
    array::UInt8Array,
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use async_trait::async_trait;
use macros::define_result;
use query_frontend::plan::ExistsTablePlan;
use snafu::{ResultExt, Snafu};

use crate::{
    interpreter::{Exists, Interpreter, InterpreterPtr, Output, Result as InterpreterResult},
    RecordBatchVec,
};

#[derive(Debug, Snafu)]
pub enum Error {}

define_result!(Error);

pub struct ExistsInterpreter {
    plan: ExistsTablePlan,
}

impl ExistsInterpreter {
    pub fn create(plan: ExistsTablePlan) -> InterpreterPtr {
        Box::new(Self { plan })
    }

    async fn execute_exists(self: Box<Self>) -> Result<Output> {
        let ExistsTablePlan { exists } = self.plan;

        exists_table_result(exists).map(Output::Records)
    }
}

fn exists_table_result(exists: bool) -> Result<RecordBatchVec> {
    let schema = Schema::new(vec![Field::new("result", DataType::UInt8, false)]);

    let arrow_record_batch = RecordBatch::try_new(
        Arc::new(schema),
        vec![Arc::new(UInt8Array::from_value(
            if exists { 1u8 } else { 0u8 },
            1,
        ))],
    )
    .unwrap();

    let record_batch = arrow_record_batch.try_into().unwrap();

    Ok(vec![record_batch])
}

#[async_trait]
impl Interpreter for ExistsInterpreter {
    async fn execute(self: Box<Self>) -> InterpreterResult<Output> {
        self.execute_exists().await.context(Exists)
    }
}
