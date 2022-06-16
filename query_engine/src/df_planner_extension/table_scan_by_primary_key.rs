// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

use std::{
    any::Any,
    fmt::{Debug, Formatter},
    sync::Arc,
};

use arrow_deps::datafusion::{
    error::DataFusionError,
    execution::context::SessionState,
    logical_plan::{self, DFSchemaRef, Expr, LogicalPlan, TableScan, UserDefinedLogicalNode},
    physical_plan::{planner::ExtensionPlanner, ExecutionPlan, PhysicalPlanner},
};
use table_engine::{provider::TableProviderAdapter, table::ReadOrder};

/// The extension planner creates physical plan for the
/// [`TableScanByPrimaryKey`] which is a logical plan node.
pub struct Planner;

impl ExtensionPlanner for Planner {
    fn plan_extension(
        &self,
        _planner: &dyn PhysicalPlanner,
        node: &dyn UserDefinedLogicalNode,
        _logical_inputs: &[&LogicalPlan],
        _physical_inputs: &[Arc<dyn ExecutionPlan>],
        _session_state: &SessionState,
    ) -> arrow_deps::datafusion::error::Result<Option<Arc<dyn ExecutionPlan>>> {
        node.as_any()
            .downcast_ref::<TableScanByPrimaryKey>()
            .map(|order_by_node| order_by_node.build_scan_table_exec_plan())
            .transpose()
    }
}

/// TableScanInPrimaryKeyOrder is a [`UserDefinedLogicalNode`] of datafusion
/// which normally is generated during logical plan optimization.
///
/// It differs from the default [`TableScan`] in its corresponding
/// [`ExecutionPlan`] is a special [`ScanTable`] which can controls the scan
/// order.
#[derive(Clone)]
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
    fn build_scan_table_exec_plan(
        &self,
    ) -> arrow_deps::datafusion::error::Result<Arc<dyn ExecutionPlan>> {
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
                let filters = logical_plan::unnormalize_cols(filters.iter().cloned());

                futures::executor::block_on(table_provider.scan_table(
                    projection,
                    &filters,
                    *fetch,
                    ReadOrder::from_is_asc(Some(self.asc)),
                ))
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
    ) -> Arc<dyn UserDefinedLogicalNode + Send + Sync> {
        Arc::new(Self {
            asc: self.asc,
            scan_plan: self.scan_plan.clone(),
        })
    }
}
