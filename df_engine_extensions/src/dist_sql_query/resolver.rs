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

use async_recursion::async_recursion;
use catalog::manager::ManagerRef as CatalogManagerRef;
use datafusion::{
    error::{DataFusionError, Result as DfResult},
    physical_plan::ExecutionPlan,
};
use table_engine::{remote::model::TableIdentifier, table::TableRef};

use crate::{
    dist_sql_query::{
        physical_plan::{
            ResolvedPartitionedScan, SubTablePlanContext, UnresolvedPartitionedScan,
            UnresolvedSubTableScan,
        },
        ExecutableScanBuilderRef, RemotePhysicalPlanExecutorRef, TableScanContext,
    },
    metrics::PUSH_DOWN_PLAN_COUNTER,
};

/// Resolver which makes datafuison dist query related plan executable.
///
/// The reason we define a `Resolver` rather than `physical optimization rule`
/// is: As I see, physical optimization rule is responsible for optimizing a bad
/// plan to good one, rather than making a inexecutable plan executable.
/// So we define `Resolver` to make it, it may be somthing similar to task
/// generator responsible for generating task for executor to run based on
/// physical plan.
pub struct Resolver {
    remote_executor: RemotePhysicalPlanExecutorRef,
    catalog_manager: CatalogManagerRef,
    scan_builder: ExecutableScanBuilderRef,
}

impl Resolver {
    pub fn new(
        remote_executor: RemotePhysicalPlanExecutorRef,
        catalog_manager: CatalogManagerRef,
        scan_builder: ExecutableScanBuilderRef,
    ) -> Self {
        Self {
            remote_executor,
            catalog_manager,
            scan_builder,
        }
    }

    /// Resolve partitioned scan, including:
    ///   - Convert `UnresolvedPartitionedScan`(inexecutable) to
    ///     `ResolvedPartitionedScan`(executable).
    ///   - Push nodes(e.g. filter, projection, partial aggregation,...) to
    ///     `ResolvedPartitionedScan`.
    ///
    /// Example for the process:
    ///   - Initial plan:
    ///
    /// ```plaintext
    ///     Final Aggregation
    ///         Partial Aggregation
    ///             Filter
    ///                 UnresolvedPartitionedScan
    /// ```
    ///
    ///    - After converting partitioned scan from unresolved to resolved:
    ///
    /// ```plaintext
    ///     Final Aggregation
    ///         Partial Aggregation
    ///             Filter
    ///                 ResolvedPartitionedScan
    ///                     UnresolvedSubTableScan (send to remote node)
    /// ```
    ///
    ///    - After pushing down nodes:
    ///
    /// ```plaintext
    ///     Final Aggregation
    ///         ResolvedPartitionedScan
    ///             Partial Aggregation (send to remote node)
    ///                 Filter (send to remote node)
    ///                      UnresolvedSubTableScan (send to remote node)
    /// ```
    pub fn resolve_partitioned_scan(
        &self,
        plan: Arc<dyn ExecutionPlan>,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        let resolved_plan = self.resolve_partitioned_scan_internal(plan)?;
        PUSH_DOWN_PLAN_COUNTER
            .with_label_values(&["remote_scan"])
            .inc();

        if let Some(plan) = resolved_plan
            .as_any()
            .downcast_ref::<ResolvedPartitionedScan>()
        {
            Ok(plan.pushdown_finished())
        } else {
            Ok(resolved_plan)
        }
    }

    pub fn resolve_partitioned_scan_internal(
        &self,
        plan: Arc<dyn ExecutionPlan>,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        // Leave node, let's resolve it and return.
        if let Some(unresolved) = plan.as_any().downcast_ref::<UnresolvedPartitionedScan>() {
            let metrics_collector = unresolved.metrics_collector.clone();
            let sub_tables = unresolved.sub_tables.clone();
            let remote_plans = sub_tables
                .into_iter()
                .map(|table| {
                    let plan = Arc::new(UnresolvedSubTableScan {
                        table: table.clone(),
                        table_scan_ctx: unresolved.table_scan_ctx.clone(),
                    });
                    let sub_metrics_collect = metrics_collector.span(table.table.clone());

                    SubTablePlanContext::new(table, plan, sub_metrics_collect)
                })
                .collect::<Vec<_>>();

            return Ok(Arc::new(ResolvedPartitionedScan::new(
                self.remote_executor.clone(),
                remote_plans,
                metrics_collector,
            )));
        }

        let children = plan.children().clone();
        // Occur some node isn't table scan but without children? It should return, too.
        if children.is_empty() {
            return Ok(plan);
        }

        // Resolve children if exist.
        let mut new_children = Vec::with_capacity(children.len());
        for child in children {
            let child = self.resolve_partitioned_scan_internal(child)?;

            new_children.push(child);
        }

        Self::maybe_push_down_to_remote_plans(new_children, plan)
    }

    fn maybe_push_down_to_remote_plans(
        mut new_children: Vec<Arc<dyn ExecutionPlan>>,
        current_node: Arc<dyn ExecutionPlan>,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        // No children, just return.
        if new_children.is_empty() {
            return Ok(current_node);
        }

        // When this node has multiple children, it can't be pushed down to remote.
        if new_children.len() > 1 {
            new_children.iter_mut().for_each(|child| {
                if let Some(plan) = child.as_any().downcast_ref::<ResolvedPartitionedScan>() {
                    *child = plan.pushdown_finished();
                }
            });
            return current_node.with_new_children(new_children);
        }

        // Has ensured that this node has just child and it is just
        // `ResolvedPartitionedScan`, try to push down it to remote plans in
        // `ResolvedPartitionedScan`.
        let child = new_children.first().unwrap();
        let partitioned_scan =
            if let Some(plan) = child.as_any().downcast_ref::<ResolvedPartitionedScan>() {
                plan
            } else {
                return current_node.with_new_children(new_children);
            };

        partitioned_scan.try_to_push_down_more(current_node.clone())
    }

    #[async_recursion]
    pub async fn resolve_sub_scan(
        &self,
        plan: Arc<dyn ExecutionPlan>,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        // Leave node, let's resolve it and return.
        let build_scan_opt =
            if let Some(unresolved) = plan.as_any().downcast_ref::<UnresolvedSubTableScan>() {
                let table = self.find_table(&unresolved.table)?;
                let table_scan_ctx = unresolved.table_scan_ctx.clone();

                Some((table, table_scan_ctx))
            } else {
                None
            };

        if let Some((table, table_scan_ctx)) = build_scan_opt {
            return self.scan_builder.build(table, table_scan_ctx).await;
        }

        let children = plan.children().clone();
        // Occur some node isn't table scan but without children? It should return, too.
        if children.is_empty() {
            return Ok(plan);
        }

        // Resolve children if exist.
        let mut new_children = Vec::with_capacity(children.len());
        for child in children {
            let child = self.resolve_sub_scan(child).await?;

            new_children.push(child);
        }

        plan.with_new_children(new_children)
    }

    fn find_table(&self, table_ident: &TableIdentifier) -> DfResult<TableRef> {
        let catalog = self
            .catalog_manager
            .catalog_by_name(&table_ident.catalog)
            .map_err(|e| DataFusionError::Internal(format!("failed to find catalog, err:{e}")))?
            .ok_or(DataFusionError::Internal("catalog not found".to_string()))?;

        let schema = catalog
            .schema_by_name(&table_ident.schema)
            .map_err(|e| DataFusionError::Internal(format!("failed to find schema, err:{e}")))?
            .ok_or(DataFusionError::Internal("schema not found".to_string()))?;

        schema
            .table_by_name(&table_ident.table)
            .map_err(|e| DataFusionError::Internal(format!("failed to find table, err:{e}")))?
            .ok_or(DataFusionError::Internal("table not found".to_string()))
    }
}

#[cfg(test)]
mod test {

    use datafusion::physical_plan::displayable;

    use crate::dist_sql_query::test_util::TestContext;

    #[test]
    fn test_basic_partitioned_scan() {
        let ctx = TestContext::new();
        let plan = ctx.build_basic_partitioned_table_plan();
        let resolver = ctx.resolver();
        let new_plan = displayable(resolver.resolve_partitioned_scan(plan).unwrap().as_ref())
            .indent(true)
            .to_string();
        insta::assert_snapshot!(new_plan);
    }

    #[tokio::test]
    async fn test_basic_sub_scan() {
        let ctx = TestContext::new();
        let plan = ctx.build_basic_sub_table_plan();
        let resolver = ctx.resolver();
        let new_plan = displayable(resolver.resolve_sub_scan(plan).await.unwrap().as_ref())
            .indent(true)
            .to_string();
        insta::assert_snapshot!(new_plan);
    }

    #[tokio::test]
    async fn test_unprocessed_plan() {
        let ctx = TestContext::new();
        let plan = ctx.build_unprocessed_plan();
        let resolver = ctx.resolver();

        let original_plan_display = displayable(plan.as_ref()).indent(true).to_string();

        // It should not be processed by `resolve_partitioned_scan`.
        let new_plan = resolver.resolve_partitioned_scan(plan.clone()).unwrap();

        let new_plan_display = displayable(new_plan.as_ref()).indent(true).to_string();

        assert_eq!(original_plan_display, new_plan_display);

        // It should not be processed by `resolve_sub_scan_internal`.
        let new_plan = resolver.resolve_sub_scan(plan.clone()).await.unwrap();

        let new_plan_display = displayable(new_plan.as_ref()).indent(true).to_string();

        assert_eq!(original_plan_display, new_plan_display);
    }

    #[test]
    fn test_aggr_push_down() {
        let ctx = TestContext::new();
        let plan = ctx.build_aggr_push_down_plan();
        let resolver = ctx.resolver();
        let new_plan = displayable(resolver.resolve_partitioned_scan(plan).unwrap().as_ref())
            .indent(true)
            .to_string();
        insta::assert_snapshot!(new_plan);
    }

    #[test]
    fn test_compounded_aggr_push_down() {
        let ctx = TestContext::new();
        let plan = ctx.build_compounded_aggr_push_down_plan();
        let resolver = ctx.resolver();
        let new_plan = displayable(resolver.resolve_partitioned_scan(plan).unwrap().as_ref())
            .indent(true)
            .to_string();
        insta::assert_snapshot!(new_plan);
    }

    #[test]
    fn test_node_with_multiple_partitioned_scan_children() {
        let ctx = TestContext::new();
        let plan = ctx.build_union_plan();
        let resolver = ctx.resolver();
        let new_plan = displayable(resolver.resolve_partitioned_scan(plan).unwrap().as_ref())
            .indent(true)
            .to_string();
        insta::assert_snapshot!(new_plan);
    }
}
