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

use std::{sync::Arc, time::Instant};

use async_trait::async_trait;
use generic_error::BoxError;
use log::info;
use snafu::ResultExt;
use table_engine::stream::SendableRecordBatchStream;
use time_ext::InstantExt;

use crate::{
    context::Context,
    datafusion_impl::{
        task_context::{DatafusionTaskExecContext, Preprocessor},
        DfContextBuilder,
    },
    error::*,
    executor::Executor,
    physical_planner::{PhysicalPlanPtr, TaskExecContext},
};

#[derive(Debug, Clone)]
pub struct DatafusionExecutorImpl {
    /// Datafuison session context builder
    df_ctx_builder: Arc<DfContextBuilder>,

    /// Preprocessor for processing some physical plan before executing
    preprocessor: Arc<Preprocessor>,
}

impl DatafusionExecutorImpl {
    pub fn new(df_ctx_builder: Arc<DfContextBuilder>, preprocessor: Arc<Preprocessor>) -> Self {
        Self {
            df_ctx_builder,
            preprocessor,
        }
    }

    fn task_exec_context(&self, ctx: &Context) -> TaskExecContext {
        let session_ctx = self.df_ctx_builder.build(ctx);
        let task_ctx = session_ctx.task_ctx();

        let df_ctx = DatafusionTaskExecContext {
            request_id: ctx.request_id,
            task_ctx,
            preprocessor: self.preprocessor.clone(),
        };

        TaskExecContext::default().with_datafusion_context(df_ctx)
    }
}

#[async_trait]
impl Executor for DatafusionExecutorImpl {
    async fn execute(
        &self,
        ctx: &Context,
        physical_plan: PhysicalPlanPtr,
    ) -> Result<SendableRecordBatchStream> {
        info!(
            "DatafusionExecutorImpl begin to execute plan, request_id:{}, physical_plan: {:?}",
            ctx.request_id, physical_plan
        );

        let begin_instant = Instant::now();

        // TODO: build the `TaskContext` directly rather than through `SessionContext`.
        let task_ctx = self.task_exec_context(ctx);
        let stream = physical_plan
            .execute(&task_ctx)
            .await
            .box_err()
            .with_context(|| ExecutorWithCause {
                msg: Some("failed to execute physical plan".to_string()),
            })?;

        info!(
            "DatafusionExecutorImpl finish to execute plan, request_id:{}, cost:{}ms, plan_and_metrics: {}",
            ctx.request_id,
            begin_instant.saturating_elapsed().as_millis(),
            physical_plan.metrics_to_string()
        );

        Ok(stream)
    }
}
