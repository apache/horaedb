// Copyright 2022-2023 CeresDB Project Authors. Licensed under Apache-2.0.

//! Logical optimizer

#[cfg(test)]
pub mod tests;
pub mod type_conversion;

use datafusion::prelude::SessionContext;
use generic_error::BoxError;
use query_frontend::plan::QueryPlan;
use snafu::ResultExt;

use crate::error::*;

/// LogicalOptimizer transform the QueryPlan into a potentially more efficient
/// plan
pub trait LogicalOptimizer {
    // TODO(yingwen): Maybe support other plans
    fn optimize(&mut self, plan: QueryPlan) -> Result<QueryPlan>;
}

pub struct LogicalOptimizerImpl {
    ctx: SessionContext,
}

impl LogicalOptimizerImpl {
    pub fn with_context(ctx: SessionContext) -> Self {
        Self { ctx }
    }
}

impl LogicalOptimizer for LogicalOptimizerImpl {
    fn optimize(&mut self, plan: QueryPlan) -> Result<QueryPlan> {
        // TODO(yingwen): Avoid clone the plan multiple times during optimization
        let QueryPlan {
            mut df_plan,
            tables,
        } = plan;
        df_plan = self
            .ctx
            .state()
            .optimize(&df_plan)
            .box_err()
            .context(LogicalOptimizerWithCause { msg: None })?;

        Ok(QueryPlan { df_plan, tables })
    }
}
