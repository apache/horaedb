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

        // There may be `ResolvedPartitionedScan` node in children, try to extend such
        // children.
        // TODO: push down the computation physical node here.
        Self::maybe_extend_partitioned_scan(new_children, plan)
    }

    fn maybe_extend_partitioned_scan(
        new_children: Vec<Arc<dyn ExecutionPlan>>,
        current_node: Arc<dyn ExecutionPlan>,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        if new_children.is_empty() {
            return Ok(current_node);
        }

        current_node.with_new_children(new_children)
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
    use catalog::{manager::ManagerRef, test_util::MockCatalogManagerBuilder};
    use common_types::{projected_schema::ProjectedSchema, tests::build_schema_for_cpu};
    use datafusion::{
        error::Result as DfResult,
        logical_expr::{expr_fn, Literal, Operator},
        physical_plan::{
            displayable,
            expressions::{binary, col, lit},
            filter::FilterExec,
            projection::ProjectionExec,
            DisplayAs, ExecutionPlan, PhysicalExpr, SendableRecordBatchStream,
        },
        scalar::ScalarValue,
    };
    use table_engine::{
        memory::MemoryTable,
        predicate::PredicateBuilder,
        remote::model::TableIdentifier,
        table::{ReadOptions, ReadRequest, TableId, TableRef},
        ANALYTIC_ENGINE_TYPE,
    };
    use trace_metric::MetricsCollector;

    use crate::dist_sql_query::{
        physical_plan::{UnresolvedPartitionedScan, UnresolvedSubTableScan},
        resolver::{PartitionedScanResolver, RemotePhysicalPlanExecutor, SubScanResolver},
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

    // Test context
    struct TestContext {
        request: ReadRequest,
        sub_tables: Vec<TableIdentifier>,
        physical_filter: Arc<dyn PhysicalExpr>,
        physical_projection: Vec<(Arc<dyn PhysicalExpr>, String)>,
        catalog_manager: ManagerRef,
    }

    impl TestContext {
        fn new() -> Self {
            let test_schema = build_schema_for_cpu();
            let sub_tables = vec![
                "__test_1".to_string(),
                "__test_2".to_string(),
                "__test_3".to_string(),
            ]
            .into_iter()
            .map(|table| TableIdentifier {
                catalog: "test_catalog".to_string(),
                schema: "test_schema".to_string(),
                table,
            })
            .collect::<Vec<_>>();

            // Logical exprs.
            // Projection: [time, tag1, tag2, value, field2]
            let projection = vec![1_usize, 2, 3, 4, 5];
            let projected_schema =
                ProjectedSchema::new(test_schema.clone(), Some(projection)).unwrap();
            // Filter: time < 1691974518000 and tag1 == 'test_tag'
            let logical_filters = vec![(expr_fn::col("time")
                .lt(ScalarValue::TimestampMillisecond(Some(1691974518000), None).lit()))
            .and(expr_fn::col("tag1").eq("test_tag".lit()))];

            // Physical exprs.
            let arrow_projected_schema = projected_schema.to_projected_arrow_schema();
            let physical_projection = vec![
                (
                    col("time", &arrow_projected_schema).unwrap(),
                    "time".to_string(),
                ),
                (
                    col("tag1", &arrow_projected_schema).unwrap(),
                    "tag1".to_string(),
                ),
                (
                    col("tag2", &arrow_projected_schema).unwrap(),
                    "tag2".to_string(),
                ),
                (
                    col("value", &arrow_projected_schema).unwrap(),
                    "value".to_string(),
                ),
                (
                    col("field2", &arrow_projected_schema).unwrap(),
                    "field2".to_string(),
                ),
            ];

            let physical_filter1: Arc<dyn PhysicalExpr> = binary(
                col("time", &arrow_projected_schema).unwrap(),
                Operator::Lt,
                lit(ScalarValue::TimestampMillisecond(Some(1691974518000), None)),
                &arrow_projected_schema,
            )
            .unwrap();
            let physical_filter2: Arc<dyn PhysicalExpr> = binary(
                col("tag1", &arrow_projected_schema).unwrap(),
                Operator::Eq,
                lit("test_tag"),
                &arrow_projected_schema,
            )
            .unwrap();
            let physical_filter: Arc<dyn PhysicalExpr> = binary(
                physical_filter1,
                Operator::And,
                physical_filter2,
                &arrow_projected_schema,
            )
            .unwrap();

            // Build the physical plan.
            let predicate = PredicateBuilder::default()
                .add_pushdown_exprs(&logical_filters)
                .extract_time_range(&test_schema, &logical_filters)
                .build();
            let read_request = ReadRequest {
                request_id: 42.into(),
                opts: ReadOptions::default(),
                projected_schema,
                predicate,
                metrics_collector: MetricsCollector::default(),
            };

            // Build the test catalog
            let table = Arc::new(MemoryTable::new(
                "__test_1".to_string(),
                TableId::from(42),
                build_schema_for_cpu(),
                ANALYTIC_ENGINE_TYPE.to_string(),
            ));

            let catalog_manager_builder = MockCatalogManagerBuilder::new(
                "test_catalog".to_string(),
                "test_schema".to_string(),
                vec![table],
            );
            let catalog_manager = catalog_manager_builder.build();

            Self {
                request: read_request,
                sub_tables,
                physical_filter,
                physical_projection,
                catalog_manager,
            }
        }

        // Return test catalog manager
        fn catalog_manager(&self) -> ManagerRef {
            self.catalog_manager.clone()
        }

        // Basic plan includes:
        // Projection
        //      Filter
        //          Scan
        fn build_basic_partitioned_table_plan(&self) -> Arc<dyn ExecutionPlan> {
            let unresolved_scan = Arc::new(UnresolvedPartitionedScan {
                sub_tables: self.sub_tables.clone(),
                read_request: self.request.clone(),
            });

            let filter: Arc<dyn ExecutionPlan> = Arc::new(
                FilterExec::try_new(self.physical_filter.clone(), unresolved_scan).unwrap(),
            );

            Arc::new(ProjectionExec::try_new(self.physical_projection.clone(), filter).unwrap())
        }

        // Basic plan includes:
        // Projection
        //      Filter
        //          Scan
        fn build_basic_sub_table_plan(&self) -> Arc<dyn ExecutionPlan> {
            let unresolved_scan = Arc::new(UnresolvedSubTableScan {
                table: self.sub_tables[0].clone(),
                read_request: self.request.clone(),
            });

            let filter: Arc<dyn ExecutionPlan> = Arc::new(
                FilterExec::try_new(self.physical_filter.clone(), unresolved_scan).unwrap(),
            );

            Arc::new(ProjectionExec::try_new(self.physical_projection.clone(), filter).unwrap())
        }
    }
}
