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
    fmt::{Debug, Formatter},
    sync::Arc,
};

use async_trait::async_trait;
use datafusion::{
    execution::context::TaskContext,
    physical_plan::{
        coalesce_partitions::CoalescePartitionsExec, display::DisplayableExecutionPlan,
        ExecutionPlan,
    },
    prelude::SessionContext,
};
use generic_error::BoxError;
use snafu::ResultExt;
use table_engine::stream::{FromDfStream, SendableRecordBatchStream};

use crate::{error::*, physical_planner::PhysicalPlan};

/// Datafusion physical plan adapter
///
/// Because we need to
pub struct DataFusionPhysicalPlanImpl {
    ctx: SessionContext,
    plan: Arc<dyn ExecutionPlan>,
}

impl DataFusionPhysicalPlanImpl {
    pub fn with_plan(ctx: SessionContext, plan: Arc<dyn ExecutionPlan>) -> Self {
        Self { ctx, plan }
    }

    pub fn as_df_physical_plan(&self) -> Arc<dyn ExecutionPlan> {
        self.plan.clone()
    }
}

impl Debug for DataFusionPhysicalPlanImpl {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DataFusionPhysicalPlan")
            .field("plan", &self.plan)
            .finish()
    }
}

#[async_trait]
impl PhysicalPlan for DataFusionPhysicalPlanImpl {
    fn execute(&self) -> Result<SendableRecordBatchStream> {
        let task_context = Arc::new(TaskContext::from(&self.ctx));
        let partition_count = self.plan.output_partitioning().partition_count();
        let df_stream = if partition_count <= 1 {
            self.plan
                .execute(0, task_context)
                .box_err()
                .context(PhysicalPlanWithCause {
                    msg: Some(format!("partition_count:{partition_count}")),
                })?
        } else {
            // merge into a single partition
            let plan = CoalescePartitionsExec::new(self.plan.clone());
            // MergeExec must produce a single partition
            assert_eq!(1, plan.output_partitioning().partition_count());
            plan.execute(0, task_context)
                .box_err()
                .context(PhysicalPlanWithCause {
                    msg: Some(format!("partition_count:{partition_count}")),
                })?
        };

        let stream = FromDfStream::new(df_stream)
            .box_err()
            .context(PhysicalPlanWithCause { msg: None })?;

        Ok(Box::pin(stream))
    }

    fn metrics_to_string(&self) -> String {
        DisplayableExecutionPlan::with_metrics(&*self.plan)
            .indent(true)
            .to_string()
    }
}
