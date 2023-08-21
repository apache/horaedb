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
use futures::TryStreamExt;
use generic_error::{BoxError, GenericError};
use log::debug;
use macros::define_result;
use query_engine::{executor::ExecutorRef, physical_planner::PhysicalPlannerRef};
use query_frontend::plan::QueryPlan;
use snafu::{ResultExt, Snafu};
use table_engine::stream::SendableRecordBatchStream;

use crate::{
    context::Context,
    interpreter::{Interpreter, InterpreterPtr, Output, Result as InterpreterResult, Select},
    RecordBatchVec,
};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Failed to create query context, err:{}", source))]
    CreateQueryContext { source: crate::context::Error },

    #[snafu(display("Failed to execute physical plan, msg:{}, err:{}", msg, source))]
    ExecutePlan { msg: String, source: GenericError },
}

define_result!(Error);

/// Select interpreter
pub struct SelectInterpreter {
    ctx: Context,
    plan: QueryPlan,
    executor: ExecutorRef,
    physical_planner: PhysicalPlannerRef,
}

impl SelectInterpreter {
    pub fn create(
        ctx: Context,
        plan: QueryPlan,
        executor: ExecutorRef,
        physical_planner: PhysicalPlannerRef,
    ) -> InterpreterPtr {
        Box::new(Self {
            ctx,
            plan,
            executor,
            physical_planner,
        })
    }
}

#[async_trait]
impl Interpreter for SelectInterpreter {
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

        // Create physical plan.
        let physical_plan = self
            .physical_planner
            .plan(&query_ctx, self.plan)
            .await
            .box_err()
            .context(ExecutePlan {
                msg: "failed to build physical plan",
            })
            .context(Select)?;

        let record_batch_stream = self
            .executor
            .execute(&query_ctx, physical_plan)
            .await
            .box_err()
            .context(ExecutePlan {
                msg: "failed to execute physical plan",
            })
            .context(Select)?;

        debug!(
            "Interpreter execute select finish, request_id:{}",
            request_id
        );

        let record_batches = collect(record_batch_stream).await?;

        Ok(Output::Records(record_batches))
    }
}

async fn collect(stream: SendableRecordBatchStream) -> InterpreterResult<RecordBatchVec> {
    stream
        .try_collect()
        .await
        .box_err()
        .context(ExecutePlan {
            msg: "failed to collect execution results",
        })
        .context(Select)
}
