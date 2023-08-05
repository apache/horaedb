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

//! Logical optimizer

#[cfg(test)]
pub mod tests;
pub mod type_conversion;

use datafusion::{error::DataFusionError, prelude::SessionContext};
use macros::define_result;
use query_frontend::plan::QueryPlan;
use snafu::{Backtrace, ResultExt, Snafu};

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
            .context(DataFusionOptimize)?;

        Ok(QueryPlan { df_plan, tables })
    }
}
