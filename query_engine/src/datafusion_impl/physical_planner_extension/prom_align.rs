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

use std::sync::Arc;

use async_trait::async_trait;
use datafusion::{
    error::DataFusionError,
    execution::context::SessionState,
    logical_expr::logical_plan::{LogicalPlan, UserDefinedLogicalNode},
    physical_plan::ExecutionPlan,
    physical_planner::{ExtensionPlanner, PhysicalPlanner},
};
use query_frontend::promql::PromAlignNode;

use crate::datafusion_impl::physical_plan_extension::prom_align::PromAlignExec;

pub struct PromAlignPlanner;

#[async_trait]
impl ExtensionPlanner for PromAlignPlanner {
    async fn plan_extension(
        &self,
        _planner: &dyn PhysicalPlanner,
        node: &dyn UserDefinedLogicalNode,
        logical_inputs: &[&LogicalPlan],
        physical_inputs: &[Arc<dyn ExecutionPlan>],
        _session_state: &SessionState,
    ) -> datafusion::error::Result<Option<Arc<dyn ExecutionPlan>>> {
        Ok(
            if let Some(node) = node.as_any().downcast_ref::<PromAlignNode>() {
                assert_eq!(logical_inputs.len(), 1, "Inconsistent number of inputs");
                assert_eq!(physical_inputs.len(), 1, "Inconsistent number of inputs");
                Some(Arc::new(
                    PromAlignExec::try_new(
                        physical_inputs[0].clone(),
                        node.column_name.clone(),
                        node.func,
                        node.align_param,
                        node.read_parallelism,
                    )
                    // DataFusionError is lost when wrapped, use string instead.
                    .map_err(|e| DataFusionError::Plan(e.to_string()))?,
                ))
            } else {
                None
            },
        )
    }
}
