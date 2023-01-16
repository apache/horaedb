// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

//! Query executor

use std::{sync::Arc, time::Instant};

use async_trait::async_trait;
use common_types::record_batch::RecordBatch;
use common_util::time::InstantExt;
use datafusion::prelude::SessionContext;
use futures::TryStreamExt;
use log::{debug, info};
use snafu::{Backtrace, ResultExt, Snafu};
use sql::{plan::QueryPlan, provider::CatalogProviderAdapter};
use table_engine::stream::SendableRecordBatchStream;

use crate::{
    config::Config,
    context::{Context, ContextRef},
    logical_optimizer::{LogicalOptimizer, LogicalOptimizerImpl},
    physical_optimizer::{PhysicalOptimizer, PhysicalOptimizerImpl},
    physical_plan::PhysicalPlanPtr,
};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Failed to do logical optimization, err:{}", source))]
    LogicalOptimize {
        source: crate::logical_optimizer::Error,
    },

    #[snafu(display("Failed to do physical optimization, err:{}", source))]
    PhysicalOptimize {
        source: crate::physical_optimizer::Error,
    },

    #[snafu(display("Failed to execute physical plan, err:{}", source))]
    ExecutePhysical { source: crate::physical_plan::Error },

    #[snafu(display("Failed to collect record batch stream, err:{}", source,))]
    Collect { source: table_engine::stream::Error },

    #[snafu(display("Timeout when execute, err:{}.\nBacktrace:\n{}", source, backtrace))]
    Timeout {
        source: tokio::time::error::Elapsed,
        backtrace: Backtrace,
    },
}

define_result!(Error);

// Use a type alias so that we are able to replace the implementation
pub type RecordBatchVec = Vec<RecordBatch>;

/// Query to execute
///
/// Contains the query plan and other infos
#[derive(Debug)]
pub struct Query {
    /// The query plan
    plan: QueryPlan,
}

impl Query {
    pub fn new(plan: QueryPlan) -> Self {
        Self { plan }
    }
}

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
    async fn execute_logical_plan(&self, ctx: ContextRef, query: Query) -> Result<RecordBatchVec>;
}

#[derive(Clone, Default)]
pub struct ExecutorImpl {
    config: Config,
}

impl ExecutorImpl {
    pub fn new(config: Config) -> Self {
        Self { config }
    }
}

#[async_trait]
impl Executor for ExecutorImpl {
    async fn execute_logical_plan(&self, ctx: ContextRef, query: Query) -> Result<RecordBatchVec> {
        let plan = query.plan;

        // Register catalogs to datafusion execution context.
        let catalogs =
            CatalogProviderAdapter::new_adapters(plan.tables.clone(), self.config.read_parallelism);
        let df_ctx = ctx.build_df_session_ctx(&self.config, ctx.request_id, ctx.deadline);
        for (name, catalog) in catalogs {
            df_ctx.register_catalog(&name, Arc::new(catalog));
        }
        let begin_instant = Instant::now();

        let physical_plan = optimize_plan(&ctx, df_ctx, plan).await?;

        debug!(
            "Executor physical optimization finished, request_id:{}, physical_plan: {:?}",
            ctx.request_id, physical_plan
        );

        let stream = physical_plan.execute().context(ExecutePhysical)?;

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

async fn optimize_plan(
    ctx: &Context,
    df_ctx: SessionContext,
    plan: QueryPlan,
) -> Result<PhysicalPlanPtr> {
    let mut logical_optimizer = LogicalOptimizerImpl::with_context(df_ctx.clone());
    let plan = logical_optimizer.optimize(plan).context(LogicalOptimize)?;

    debug!(
        "Executor logical optimization finished, request_id:{}, plan: {:#?}",
        ctx.request_id, plan
    );

    let mut physical_optimizer = PhysicalOptimizerImpl::with_context(df_ctx);
    physical_optimizer
        .optimize(plan)
        .await
        .context(PhysicalOptimize)
}

async fn collect(stream: SendableRecordBatchStream) -> Result<RecordBatchVec> {
    stream.try_collect().await.context(Collect)
}
