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

//! Physical execution plan

use std::{
    fmt::{Debug, Formatter},
    sync::Arc,
};

use async_trait::async_trait;
use datafusion::{
    error::DataFusionError,
    execution::context::TaskContext,
    physical_plan::{
        coalesce_partitions::CoalescePartitionsExec, display::DisplayableExecutionPlan,
        ExecutionPlan,
    },
    prelude::SessionContext,
};
use macros::define_result;
use snafu::{Backtrace, ResultExt, Snafu};
use table_engine::stream::{FromDfStream, SendableRecordBatchStream};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display(
        "DataFusion Failed to execute plan, err:{}.\nBacktrace:\n{}",
        source,
        backtrace
    ))]
    DataFusionExec {
        partition_count: usize,
        source: DataFusionError,
        backtrace: Backtrace,
    },

    #[snafu(display("Failed to convert datafusion stream, err:{}", source))]
    ConvertStream { source: table_engine::stream::Error },
}

define_result!(Error);

pub trait PhysicalPlan: std::fmt::Debug {
    /// execute this plan and returns the result
    fn execute(&self) -> Result<SendableRecordBatchStream>;

    /// Convert internal metrics to string.
    fn metrics_to_string(&self) -> String;
}

pub type PhysicalPlanPtr = Box<dyn PhysicalPlan + Send + Sync>;

pub struct DataFusionPhysicalPlan {
    ctx: SessionContext,
    plan: Arc<dyn ExecutionPlan>,
}

impl DataFusionPhysicalPlan {
    pub fn with_plan(ctx: SessionContext, plan: Arc<dyn ExecutionPlan>) -> Self {
        Self { ctx, plan }
    }
}

impl Debug for DataFusionPhysicalPlan {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DataFusionPhysicalPlan")
            .field("plan", &self.plan)
            .finish()
    }
}

#[async_trait]
impl PhysicalPlan for DataFusionPhysicalPlan {
    fn execute(&self) -> Result<SendableRecordBatchStream> {
        let task_context = Arc::new(TaskContext::from(&self.ctx));
        let partition_count = self.plan.output_partitioning().partition_count();
        let df_stream = if partition_count <= 1 {
            self.plan
                .execute(0, task_context)
                .context(DataFusionExec { partition_count })?
        } else {
            // merge into a single partition
            let plan = CoalescePartitionsExec::new(self.plan.clone());
            // MergeExec must produce a single partition
            assert_eq!(1, plan.output_partitioning().partition_count());
            plan.execute(0, task_context)
                .context(DataFusionExec { partition_count })?
        };

        let stream = FromDfStream::new(df_stream).context(ConvertStream)?;

        Ok(Box::pin(stream))
    }

    fn metrics_to_string(&self) -> String {
        // TODO: set to verbose mode for more details now, maybe we can add a flag to
        // control it.
        DisplayableExecutionPlan::with_metrics(&*self.plan)
            .indent(true)
            .to_string()
    }
}
