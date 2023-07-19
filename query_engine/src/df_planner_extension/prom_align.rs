// Copyright 2022-2023 CeresDB Project Authors. Licensed under Apache-2.0.

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
use snafu::Snafu;

use crate::df_execution_extension::prom_align::{Error as ExecError, PromAlignExec};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Build execution failed. err:{:?}", source))]
    ExecutionError { source: ExecError },
}

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
