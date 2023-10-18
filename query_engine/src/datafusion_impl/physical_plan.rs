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

//! Datafusion physical execution plan

use std::{
    any::Any,
    fmt::{self, Debug, Formatter},
    sync::{Arc, RwLock},
};

use async_trait::async_trait;
use datafusion::physical_plan::{
    coalesce_partitions::CoalescePartitionsExec, display::DisplayableExecutionPlan, displayable,
    ExecutionPlan,
};
use generic_error::BoxError;
use logger::info;
use snafu::{OptionExt, ResultExt};
use table_engine::stream::{FromDfStream, SendableRecordBatchStream};

use crate::{
    datafusion_impl::task_context::{DatafusionTaskExecContext, Preprocessor},
    error::*,
    physical_planner::{PhysicalPlan, TaskExecContext},
};

pub enum TypedPlan {
    Normal(Arc<dyn ExecutionPlan>),
    Partitioned(Arc<dyn ExecutionPlan>),
    Remote(Vec<u8>),
}

impl fmt::Debug for TypedPlan {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Normal(plan) => f.debug_tuple("Normal").field(plan).finish(),
            Self::Partitioned(plan) => f.debug_tuple("Partitioned").field(plan).finish(),
            Self::Remote(_) => f.debug_tuple("Remote").finish(),
        }
    }
}

/// Datafusion physical plan adapter
/// Some covert works for the `original_plan` will be done before executing,
/// and convert result will be kept in `executed_plan` for inspecting the
/// execution status in later.
pub struct DataFusionPhysicalPlanAdapter {
    /// Original plan
    original_plan: TypedPlan,

    /// Executed plan from `original_plan` converting
    executed_plan: RwLock<Option<Arc<dyn ExecutionPlan>>>,
}

impl DataFusionPhysicalPlanAdapter {
    pub fn new(typed_plan: TypedPlan) -> Self {
        Self {
            original_plan: typed_plan,
            executed_plan: RwLock::new(None),
        }
    }
}

impl Debug for DataFusionPhysicalPlanAdapter {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DataFusionPhysicalPlan")
            .field("typed_plan", &self.original_plan)
            .finish()
    }
}

#[async_trait]
impl PhysicalPlan for DataFusionPhysicalPlanAdapter {
    fn as_any(&self) -> &dyn Any {
        self
    }

    async fn execute(&self, task_ctx: &TaskExecContext) -> Result<SendableRecordBatchStream> {
        // Get datafusion task context.
        let df_task_ctx =
            task_ctx
                .as_datafusion_task_ctx()
                .with_context(|| PhysicalPlanNoCause {
                    msg: Some("datafusion task ctx not found".to_string()),
                })?;

        // Maybe need preprocess for getting executable plan.
        let executable = df_task_ctx
            .preprocessor
            .process(&self.original_plan, &df_task_ctx.ctx)
            .await?;

        // Coalesce the multiple outputs plan.
        let partition_count = executable.output_partitioning().partition_count();
        let executable = if partition_count <= 1 {
            executable
        } else {
            Arc::new(CoalescePartitionsExec::new(executable))
        };

        info!(
            "DatafusionExecutorImpl get the executable plan, request_id:{}, physical_plan:{}",
            df_task_ctx.ctx.request_id,
            displayable(executable.as_ref()).indent(true).to_string()
        );

        // Kept the executed plan.
        {
            let executed = &mut *self.executed_plan.write().unwrap();
            *executed = Some(executable.clone());
        }

        // Execute the plan.
        // Ensure to be `Some` here.
        let df_stream = executable
            .execute(0, df_task_ctx.task_ctx.clone())
            .box_err()
            .context(PhysicalPlanWithCause {
                msg: Some(format!("partition_count:{partition_count}")),
            })?;

        let stream = FromDfStream::new(df_stream)
            .box_err()
            .context(PhysicalPlanWithCause { msg: None })?;

        Ok(Box::pin(stream))
    }

    fn metrics_to_string(&self) -> String {
        let executed_opt = { self.executed_plan.read().unwrap().clone() };

        match executed_opt {
            Some(plan) => DisplayableExecutionPlan::with_metrics(plan.as_ref())
                .indent(true)
                .to_string(),
            None => "plan is not executed yet".to_string(),
        }
    }
}
