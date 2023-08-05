// Copyright 2023 CeresDB Project Authors. Licensed under Apache-2.0.

use async_trait::async_trait;
use query_frontend::plan::QueryPlan;
use table_engine::stream::SendableRecordBatchStream;

use crate::{context::Context, error::*};

/// Physical query planner that converts a logical plan to a
/// physical plan suitable for execution.
/// During the convert process, it may do following things:
///   + Optimize the logical plan.
///   + Create the initial physical plan from the optimized logical.
///   + Optimize and get the final physical plan.
#[async_trait]
pub trait PhysicalPlanner: Clone + Send + Sync + 'static {
    /// Create a physical plan from a logical plan
    async fn plan(&self, logical_plan: QueryPlan, ctx: &Context) -> Result<PhysicalPlanPtr>;
}

pub trait PhysicalPlan: std::fmt::Debug {
    /// execute this plan and returns the result
    fn execute(&self) -> Result<SendableRecordBatchStream>;

    /// Convert internal metrics to string.
    fn metrics_to_string(&self) -> String;
}

pub type PhysicalPlanPtr = Box<dyn PhysicalPlan + Send + Sync>;
