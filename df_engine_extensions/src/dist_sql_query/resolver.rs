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

use catalog::manager::ManagerRef as CatalogManagerRef;
use datafusion::{
    error::{DataFusionError, Result as DfResult},
    physical_plan::ExecutionPlan,
};
use table_engine::{remote::model::TableIdentifier, table::TableRef};

use crate::dist_sql_query::{
    physical_plan::{ResolvedPartitionedScan, UnresolvedPartitionedScan, UnresolvedSubTableScan},
    ExecutableScanBuilder, RemotePhysicalPlanExecutor,
};

/// Resolver which makes datafuison dist query related plan executable.
///
/// The reason we define a `Resolver` rather than `physical optimization rule`
/// is: As I see, physical optimization rule is responsible for optimizing a bad
/// plan to good one, rather than making a inexecutable plan executable.
/// So we define `Resolver` to make it, it may be somthing similar to task
/// generator responsible for generating task for executor to run based on
/// physical plan.
pub trait Resolver {
    fn resolve(&self, plan: Arc<dyn ExecutionPlan>) -> DfResult<Arc<dyn ExecutionPlan>>;
}

/// Resolver which makes the partitioned table scan plan executable
struct PartitionedScanResolver<R> {
    remote_executor: R,
}

impl<R: RemotePhysicalPlanExecutor> PartitionedScanResolver<R> {
    #[allow(dead_code)]
    pub fn new(remote_executor: R) -> Self {
        Self { remote_executor }
    }

    fn resolve_plan(&self, plan: Arc<dyn ExecutionPlan>) -> DfResult<Arc<dyn ExecutionPlan>> {
        // Leave node, let's resolve it and return.
        if let Some(unresolved) = plan.as_any().downcast_ref::<UnresolvedPartitionedScan>() {
            let sub_tables = unresolved.sub_tables.clone();
            let remote_plans = sub_tables
                .into_iter()
                .map(|table| {
                    let plan = Arc::new(UnresolvedSubTableScan {
                        table: table.clone(),
                        read_request: unresolved.read_request.clone(),
                    });
                    (table, plan as _)
                })
                .collect::<Vec<_>>();

            return Ok(Arc::new(ResolvedPartitionedScan {
                remote_executor: self.remote_executor.clone(),
                remote_exec_plans: remote_plans,
            }));
        }

        let children = plan.children().clone();
        // Occur some node isn't table scan but without children? It should return, too.
        if children.is_empty() {
            return Ok(plan);
        }

        // Resolve children if exist.
        let mut new_children = Vec::with_capacity(children.len());
        for child in children {
            let child = self.resolve_plan(child)?;

            new_children.push(child);
        }

        // TODO: Push down the computation physical node here rather than simply
        // rebuild.
        plan.with_new_children(new_children)
    }
}

impl<R: RemotePhysicalPlanExecutor> Resolver for PartitionedScanResolver<R> {
    fn resolve(&self, plan: Arc<dyn ExecutionPlan>) -> DfResult<Arc<dyn ExecutionPlan>> {
        self.resolve_plan(plan)
    }
}

/// Resolver which makes the sub table scan plan executable
struct SubScanResolver<B> {
    catalog_manager: CatalogManagerRef,
    scan_builder: B,
}

impl<B: ExecutableScanBuilder> SubScanResolver<B> {
    #[allow(dead_code)]
    pub fn new(catalog_manager: CatalogManagerRef, scan_builder: B) -> Self {
        Self {
            catalog_manager,
            scan_builder,
        }
    }

    fn resolve_plan(&self, plan: Arc<dyn ExecutionPlan>) -> DfResult<Arc<dyn ExecutionPlan>> {
        // Leave node, let's resolve it and return.
        if let Some(unresolved) = plan.as_any().downcast_ref::<UnresolvedSubTableScan>() {
            let table = self.find_table(&unresolved.table)?;
            return self
                .scan_builder
                .build(table, unresolved.read_request.clone());
        }

        let children = plan.children().clone();
        // Occur some node isn't table scan but without children? It should return, too.
        if children.is_empty() {
            return Ok(plan);
        }

        // Resolve children if exist.
        let mut new_children = Vec::with_capacity(children.len());
        for child in children {
            let child = self.resolve_plan(child)?;

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

impl<B: ExecutableScanBuilder> Resolver for SubScanResolver<B> {
    fn resolve(&self, plan: Arc<dyn ExecutionPlan>) -> DfResult<Arc<dyn ExecutionPlan>> {
        self.resolve_plan(plan)
    }
}

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use async_trait::async_trait;
    use datafusion::{
        error::Result as DfResult,
        physical_plan::{displayable, DisplayAs, ExecutionPlan, SendableRecordBatchStream},
    };
    use table_engine::{
        remote::model::TableIdentifier,
        table::{ReadRequest, TableRef},
    };

    use crate::dist_sql_query::{
        resolver::{PartitionedScanResolver, RemotePhysicalPlanExecutor, SubScanResolver},
        test_util::TestContext,
        ExecutableScanBuilder,
    };

    #[test]
    fn test_resolve_simple_partitioned_scan() {
        let ctx = TestContext::new();
        let plan = ctx.build_basic_partitioned_table_plan();
        let resolver = PartitionedScanResolver {
            remote_executor: MockRemotePhysicalPlanExecutor,
        };
        let new_plan = displayable(resolver.resolve_plan(plan).unwrap().as_ref())
            .indent(true)
            .to_string();
        insta::assert_snapshot!(new_plan);
    }

    #[test]
    fn test_resolve_simple_sub_scan() {
        let ctx = TestContext::new();
        let plan = ctx.build_basic_sub_table_plan();
        let resolver = SubScanResolver {
            catalog_manager: ctx.catalog_manager(),
            scan_builder: MockScanBuilder,
        };
        let new_plan = displayable(resolver.resolve_plan(plan).unwrap().as_ref())
            .indent(true)
            .to_string();
        insta::assert_snapshot!(new_plan);
    }

    // Mock scan and its builder
    #[derive(Debug)]
    struct MockScanBuilder;

    impl ExecutableScanBuilder for MockScanBuilder {
        fn build(
            &self,
            _table: TableRef,
            read_request: ReadRequest,
        ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
            Ok(Arc::new(MockScan {
                request: read_request,
            }))
        }
    }

    #[derive(Debug)]
    struct MockScan {
        request: ReadRequest,
    }

    impl ExecutionPlan for MockScan {
        fn as_any(&self) -> &dyn std::any::Any {
            unimplemented!()
        }

        fn schema(&self) -> arrow::datatypes::SchemaRef {
            self.request.projected_schema.to_projected_arrow_schema()
        }

        fn output_partitioning(&self) -> datafusion::physical_plan::Partitioning {
            datafusion::physical_plan::Partitioning::UnknownPartitioning(
                self.request.opts.read_parallelism,
            )
        }

        fn output_ordering(&self) -> Option<&[datafusion::physical_expr::PhysicalSortExpr]> {
            None
        }

        fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
            vec![]
        }

        fn with_new_children(
            self: Arc<Self>,
            _children: Vec<Arc<dyn ExecutionPlan>>,
        ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
            unimplemented!()
        }

        fn execute(
            &self,
            _partition: usize,
            _context: Arc<datafusion::execution::TaskContext>,
        ) -> datafusion::error::Result<datafusion::physical_plan::SendableRecordBatchStream>
        {
            unimplemented!()
        }

        fn statistics(&self) -> datafusion::physical_plan::Statistics {
            unimplemented!()
        }
    }

    impl DisplayAs for MockScan {
        fn fmt_as(
            &self,
            _t: datafusion::physical_plan::DisplayFormatType,
            f: &mut std::fmt::Formatter,
        ) -> std::fmt::Result {
            write!(f, "MockScan")
        }
    }

    // Mock remote executor
    #[derive(Debug, Clone)]
    struct MockRemotePhysicalPlanExecutor;

    #[async_trait]
    impl RemotePhysicalPlanExecutor for MockRemotePhysicalPlanExecutor {
        async fn execute(
            &self,
            _table: TableIdentifier,
            _physical_plan: Arc<dyn ExecutionPlan>,
        ) -> DfResult<SendableRecordBatchStream> {
            unimplemented!()
        }
    }
}
