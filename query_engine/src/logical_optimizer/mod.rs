// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

//! Logical optimizer

pub mod order_by_primary_key;
#[cfg(test)]
pub mod tests;
pub mod type_conversion;

use arrow_deps::datafusion::error::DataFusionError;
use snafu::{Backtrace, ResultExt, Snafu};
use sql::plan::QueryPlan;

use crate::context::ContextRef;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display(
        "DataFusion Failed to optimize logical plan, err:{}.\nBacktrace:\n{}",
        source,
        backtrace
    ))]
    // TODO(yingwen): Should we carry plan in this context?
    DataFusionOptimize {
        source: DataFusionError,
        backtrace: Backtrace,
    },
}

define_result!(Error);

/// LogicalOptimizer transform the QueryPlan into a potentially more efficient
/// plan
pub trait LogicalOptimizer {
    // TODO(yingwen): Maybe support other plans
    fn optimize(&mut self, plan: QueryPlan) -> Result<QueryPlan>;
}

pub struct LogicalOptimizerImpl {
    ctx: ContextRef,
}

impl LogicalOptimizerImpl {
    pub fn with_context(ctx: ContextRef) -> Self {
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
        let exec_ctx = self.ctx.df_exec_ctx();
        df_plan = exec_ctx.optimize(&df_plan).context(DataFusionOptimize)?;

        Ok(QueryPlan { df_plan, tables })
    }
}
