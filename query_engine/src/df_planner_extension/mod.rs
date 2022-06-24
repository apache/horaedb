// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

//! The query planner adapter provides some planner extensions of datafusion.

use std::sync::Arc;

use arrow_deps::datafusion::{
    execution::context::{QueryPlanner, SessionState},
    logical_plan::LogicalPlan,
    physical_plan::{
        planner::{DefaultPhysicalPlanner, ExtensionPlanner},
        ExecutionPlan, PhysicalPlanner,
    },
};

pub mod prom_align;
pub mod table_scan_by_primary_key;
use async_trait::async_trait;

/// The adapter for extending the default datafusion planner.
pub struct QueryPlannerAdapter;

#[async_trait]
impl QueryPlanner for QueryPlannerAdapter {
    async fn create_physical_plan(
        &self,
        logical_plan: &LogicalPlan,
        session_state: &SessionState,
    ) -> arrow_deps::datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        let extension_planners: Vec<Arc<dyn ExtensionPlanner + Send + Sync>> = vec![
            Arc::new(table_scan_by_primary_key::Planner),
            Arc::new(prom_align::PromAlignPlanner),
        ];

        let physical_planner = DefaultPhysicalPlanner::with_extension_planners(extension_planners);
        physical_planner
            .create_physical_plan(logical_plan, session_state)
            .await
    }
}
