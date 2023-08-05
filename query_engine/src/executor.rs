// Copyright 2022-2023 CeresDB Project Authors. Licensed under Apache-2.0.

//! Query executor

use std::time::Instant;

use async_trait::async_trait;
use common_types::record_batch::RecordBatch;
use futures::TryStreamExt;
use generic_error::BoxError;
use log::{debug, info};
use snafu::ResultExt;
use table_engine::stream::SendableRecordBatchStream;
use time_ext::InstantExt;

use crate::{context::Context, error::*, physical_planner::PhysicalPlanPtr};

// Use a type alias so that we are able to replace the implementation
pub type RecordBatchVec = Vec<RecordBatch>;

/// Query executor
///
/// Executes the logical plan
#[async_trait]
pub trait Executor: Clone + Send + Sync {
    // TODO(yingwen): Maybe return a stream
    /// Execute the query, returning the query results as RecordBatchVec
    ///
    /// REQUIRE: The meta data of tables in query should be found from
    /// ContextRef
    // TODO: I am not sure that whether we should should pass the `Context` as
    // parameter rather than place it into `Executor`.
    async fn execute(
        &self,
        ctx: &Context,
        physical_plan: PhysicalPlanPtr,
    ) -> Result<RecordBatchVec>;
}

#[derive(Clone, Default)]
pub struct ExecutorImpl;

#[async_trait]
impl Executor for ExecutorImpl {
    async fn execute(
        &self,
        ctx: &Context,
        physical_plan: PhysicalPlanPtr,
    ) -> Result<RecordBatchVec> {
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

        // Collect all records in the pool, as the stream may perform some costly
        // calculation
        let record_batches = collect(stream).await?;

        info!(
            "Executor executed plan, request_id:{}, cost:{}ms, plan_and_metrics: {}",
            ctx.request_id,
            begin_instant.saturating_elapsed().as_millis(),
            physical_plan.metrics_to_string()
        );

        Ok(record_batches)
    }
}

async fn collect(stream: SendableRecordBatchStream) -> Result<RecordBatchVec> {
    stream
        .try_collect()
        .await
        .box_err()
        .with_context(|| ExecutorWithCause {
            msg: Some("failed to collect query results".to_string()),
        })
}
