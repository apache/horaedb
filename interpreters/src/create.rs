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

//! Interpreter for create statements

use async_trait::async_trait;
use macros::define_result;
use query_frontend::plan::CreateTablePlan;
use snafu::{ResultExt, Snafu};
use table_engine::engine::TableEngineRef;

use crate::{
    context::Context,
    interpreter::{Create, Interpreter, InterpreterPtr, Output, Result as InterpreterResult},
    table_manipulator::{self, TableManipulatorRef},
};

#[derive(Debug, Snafu)]
#[snafu(visibility(pub(crate)))]
pub enum Error {
    #[snafu(display("Failed to create table by table manipulator, err:{}", source))]
    ManipulateTable { source: table_manipulator::Error },
}

define_result!(Error);

/// Create interpreter
pub struct CreateInterpreter {
    ctx: Context,
    plan: CreateTablePlan,
    table_engine: TableEngineRef,
    table_manipulator: TableManipulatorRef,
}

impl CreateInterpreter {
    pub fn create(
        ctx: Context,
        plan: CreateTablePlan,
        table_engine: TableEngineRef,
        table_manipulator: TableManipulatorRef,
    ) -> InterpreterPtr {
        Box::new(Self {
            ctx,
            plan,
            table_engine,
            table_manipulator,
        })
    }
}

impl CreateInterpreter {
    async fn execute_create(self: Box<Self>) -> Result<Output> {
        self.table_manipulator
            .create_table(self.ctx, self.plan, self.table_engine)
            .await
            .context(ManipulateTable)
    }
}

// TODO(yingwen): Wrap a method that returns self::Result, simplify some code to
// converting self::Error to super::Error
#[async_trait]
impl Interpreter for CreateInterpreter {
    async fn execute(self: Box<Self>) -> InterpreterResult<Output> {
        self.execute_create().await.context(Create)
    }
}
