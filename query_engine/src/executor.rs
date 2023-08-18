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

//! Query executor

use std::{fmt, sync::Arc, time::Instant};

use async_trait::async_trait;
use generic_error::BoxError;
use log::{debug, info};
use snafu::ResultExt;
use table_engine::stream::SendableRecordBatchStream;
use time_ext::InstantExt;

use crate::{context::Context, error::*, physical_planner::PhysicalPlanPtr};

/// Query executor
///
/// Executes the logical plan
#[async_trait]
pub trait Executor: fmt::Debug + Send + Sync + 'static {
    // TODO(yingwen): Maybe return a stream
    /// Execute the query, returning the query results as RecordBatchVec
    ///
    /// REQUIRE: The meta data of tables in query should be found from
    /// ContextRef
    async fn execute(
        &self,
        ctx: &Context,
        physical_plan: PhysicalPlanPtr,
    ) -> Result<SendableRecordBatchStream>;
}

pub type ExecutorRef = Arc<dyn Executor>;

#[derive(Debug, Clone, Default)]
pub struct ExecutorImpl;

#[async_trait]
impl Executor for ExecutorImpl {
    async fn execute(
        &self,
        ctx: &Context,
        physical_plan: PhysicalPlanPtr,
    ) -> Result<SendableRecordBatchStream> {
        let begin_instant = Instant::now();

        debug!(
            "Executor physical optimization finished, request_id:{}, physical_plan: {:?}",
            ctx.request_id, physical_plan
        );

        let stream = physical_plan
            .execute()
            .box_err()
            .with_context(|| ExecutorWithCause {
                msg: Some("failed to execute physical plan".to_string()),
            })?;

        info!(
            "Executor executed plan, request_id:{}, cost:{}ms, plan_and_metrics: {}",
            ctx.request_id,
            begin_instant.saturating_elapsed().as_millis(),
            physical_plan.metrics_to_string()
        );

        Ok(stream)
    }
}
