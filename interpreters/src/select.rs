// Copyright 2022-2023 CeresDB Project Authors. Licensed under Apache-2.0.

//! Interpreter for select statement

use async_trait::async_trait;
use log::debug;
use macros::define_result;
use query_engine::{executor::Executor, physical_planner::PhysicalPlanner};
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
    ExecutePlan { source: query_engine::error::Error },
}

define_result!(Error);

/// Select interpreter
pub struct SelectInterpreter<T, P> {
    ctx: Context,
    plan: QueryPlan,
    executor: T,
    physical_planner: P,
}

impl<T: Executor + 'static, P: PhysicalPlanner> SelectInterpreter<T, P> {
    pub fn create(
        ctx: Context,
        plan: QueryPlan,
        executor: T,
        physical_planner: P,
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
impl<T: Executor, P: PhysicalPlanner> Interpreter for SelectInterpreter<T, P> {
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
            .context(ExecutePlan)
            .context(Select)?;

        let record_batches = self
            .executor
            .execute(&query_ctx, physical_plan)
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
