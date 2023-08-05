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

//! Interpreter for select statement

use async_trait::async_trait;
use log::debug;
use macros::define_result;
use query_engine::executor::{Executor, Query};
use query_frontend::plan::QueryPlan;
use snafu::{ResultExt, Snafu};

use crate::{
    context::Context,
    interpreter::{Interpreter, InterpreterPtr, Output, Result as InterpreterResult, Select},
};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Failed to create query context, err:{}", source))]
    CreateQueryContext { source: crate::context::Error },

    #[snafu(display("Failed to execute logical plan, err:{}", source))]
    ExecutePlan {
        source: query_engine::executor::Error,
    },
}

define_result!(Error);

/// Select interpreter
pub struct SelectInterpreter<T> {
    ctx: Context,
    plan: QueryPlan,
    executor: T,
}

impl<T: Executor + 'static> SelectInterpreter<T> {
    pub fn create(ctx: Context, plan: QueryPlan, executor: T) -> InterpreterPtr {
        Box::new(Self {
            ctx,
            plan,
            executor,
        })
    }
}

#[async_trait]
impl<T: Executor> Interpreter for SelectInterpreter<T> {
    async fn execute(self: Box<Self>) -> InterpreterResult<Output> {
        let request_id = self.ctx.request_id();
        debug!(
            "Interpreter execute select begin, request_id:{}, plan:{:?}",
            request_id, self.plan
        );

        let query_ctx = self
            .ctx
            .new_query_context()
            .context(CreateQueryContext)
            .context(Select)?;
        let query = Query::new(self.plan);
        let record_batches = self
            .executor
            .execute_logical_plan(query_ctx, query)
            .await
            .context(ExecutePlan)
            .context(Select)?;

        debug!(
            "Interpreter execute select finish, request_id:{}",
            request_id
        );

        Ok(Output::Records(record_batches))
    }
}
