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

//! The query planner adapter provides some planner extensions of datafusion.

use std::sync::Arc;

use datafusion::{
    execution::context::{QueryPlanner, SessionState},
    logical_expr::logical_plan::LogicalPlan,
    physical_plan::ExecutionPlan,
    physical_planner::{DefaultPhysicalPlanner, ExtensionPlanner, PhysicalPlanner},
};

pub mod prom_align;
use async_trait::async_trait;

/// The adapter for extending the default datafusion planner.
pub struct QueryPlannerAdapter;

#[async_trait]
impl QueryPlanner for QueryPlannerAdapter {
    async fn create_physical_plan(
        &self,
        logical_plan: &LogicalPlan,
        session_state: &SessionState,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        let extension_planners: Vec<Arc<dyn ExtensionPlanner + Send + Sync>> = vec![
            Arc::new(prom_align::PromAlignPlanner),
            Arc::new(influxql_query::exec::context::IOxExtensionPlanner {}),
        ];

        let physical_planner = DefaultPhysicalPlanner::with_extension_planners(extension_planners);
        physical_planner
            .create_physical_plan(logical_plan, session_state)
            .await
    }
}
