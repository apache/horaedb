// Copyright 2022-2023 CeresDB Project Authors. Licensed under Apache-2.0.

use std::{
    any::Any,
    fmt::{Debug, Formatter},
    hash::{Hash, Hasher},
    sync::Arc,
};

use async_trait::async_trait;
use datafusion::{
    common::DFSchemaRef,
    error::DataFusionError,
    execution::context::SessionState,
    logical_expr::{
        expr_rewriter,
        logical_plan::{LogicalPlan, TableScan, UserDefinedLogicalNode},
        Expr,
    },
    physical_plan::ExecutionPlan,
    physical_planner::{ExtensionPlanner, PhysicalPlanner},
};
use table_engine::{provider::TableProviderAdapter, table::ReadOrder};

/// The extension planner creates physical plan for the
/// [`TableScanByPrimaryKey`] which is a logical plan node.
pub struct Planner;

#[async_trait]
impl ExtensionPlanner for Planner {
    async fn plan_extension(
        &self,
        _planner: &dyn PhysicalPlanner,
        node: &dyn UserDefinedLogicalNode,
        _logical_inputs: &[&LogicalPlan],
        _physical_inputs: &[Arc<dyn ExecutionPlan>],
        session_state: &SessionState,
    ) -> datafusion::error::Result<Option<Arc<dyn ExecutionPlan>>> {
        let maybe_node = node.as_any().downcast_ref::<TableScanByPrimaryKey>();
        if let Some(node) = maybe_node {
            let plan = node.build_scan_table_exec_plan(session_state).await?;
            Ok(Some(plan))
        } else {
            Ok(None)
        }
    }
}

/// TableScanInPrimaryKeyOrder is a [`UserDefinedLogicalNode`] of datafusion
/// which normally is generated during logical plan optimization.
///
/// It differs from the default [`TableScan`] in its corresponding
/// [`ExecutionPlan`] is a special [`ScanTable`] which can controls the scan
/// order.
#[derive(Clone, Hash, PartialEq)]
pub struct TableScanByPrimaryKey {
    asc: bool,
    scan_plan: Arc<LogicalPlan>,
}

impl Debug for TableScanByPrimaryKey {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        self.fmt_for_explain(f)
    }
}

impl TableScanByPrimaryKey {
    /// Build the node from a [TableScan] node
    ///
    /// Note it panics if the plan node is not a LogicalPlan::TableScan.
    pub fn new_from_scan_plan(asc: bool, scan_plan: Arc<LogicalPlan>) -> Self {
        // TODO(xikai): should ensure the scan_plan is a real TableScan.
        Self { asc, scan_plan }
    }

    /// Build the scan table [ExecutionPlan].
    async fn build_scan_table_exec_plan(
        &self,
        session_state: &SessionState,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        match self.scan_plan.as_ref() {
            LogicalPlan::TableScan(TableScan {
                source,
                projection,
                filters,
                fetch,
                ..
            }) => {
                let table_provider =
                    if let Some(v) = source.as_any().downcast_ref::<TableProviderAdapter>() {
                        v
                    } else {
                        return Err(DataFusionError::Internal(format!(
                            "expect table provider adapter, given plan:{:?}",
                            self.scan_plan,
                        )));
                    };

                // Remove all qualifiers from the scan as the provider
                // doesn't know (nor should care) how the relation was
                // referred to in the query
                let filters = expr_rewriter::unnormalize_cols(filters.iter().cloned());

                // TODO: `scan_table` contains some IO (read metadata) which should not happen
                // in plan stage. It should be push down to execute stage.
                table_provider
                    .scan_table(
                        session_state,
                        projection.as_ref(),
                        &filters,
                        *fetch,
                        ReadOrder::from_is_asc(Some(self.asc)),
                    )
                    .await
            }
            _ => Err(DataFusionError::Internal(format!(
                "expect scan plan, given plan:{:?}",
                self.scan_plan
            ))),
        }
    }
}

impl UserDefinedLogicalNode for TableScanByPrimaryKey {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![]
    }

    fn schema(&self) -> &DFSchemaRef {
        self.scan_plan.schema()
    }

    fn expressions(&self) -> Vec<Expr> {
        vec![]
    }

    fn fmt_for_explain(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(
            f,
            "ScanTableInPrimaryKeyOrder, asc:{}, table_scan:{:?}",
            self.asc, self.scan_plan
        )
    }

    fn from_template(
        &self,
        _exprs: &[Expr],
        _inputs: &[LogicalPlan],
    ) -> Arc<dyn UserDefinedLogicalNode> {
        Arc::new(Self {
            asc: self.asc,
            scan_plan: self.scan_plan.clone(),
        })
    }

    fn name(&self) -> &str {
        "ScanTableInPrimaryKeyOrder"
    }

    fn dyn_hash(&self, state: &mut dyn Hasher) {
        let mut s = state;
        self.hash(&mut s);
    }

    fn dyn_eq(&self, other: &dyn UserDefinedLogicalNode) -> bool {
        match other.as_any().downcast_ref::<Self>() {
            Some(o) => self == o,
            None => false,
        }
    }
}
