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
    execution::{runtime_env::RuntimeEnv, FunctionRegistry},
    physical_plan::ExecutionPlan,
};
use datafusion_proto::{
    physical_plan::{AsExecutionPlan, PhysicalExtensionCodec},
    protobuf,
};
use prost::Message;
use table_engine::{remote::model::TableIdentifier, table::TableRef};

use crate::{
    codec::PhysicalExtensionCodecImpl,
    dist_sql_query::{
        physical_plan::{
            ResolvedPartitionedScan, UnresolvedPartitionedScan, UnresolvedSubTableScan,
        },
        ExecutableScanBuilderRef, RemotePhysicalPlanExecutorRef,
    },
};

/// Resolver which makes datafuison dist query related plan executable.
///
/// The reason we define a `Resolver` rather than `physical optimization rule`
/// is: As I see, physical optimization rule is responsible for optimizing a bad
/// plan to good one, rater than making a inexecutable plan executable.
/// So we define`Resolver` should be defined to finish it, it may be like task
/// generator responsible for generating task for executor to run based on
/// physical plan.
pub struct Resolver {
    remote_executor: RemotePhysicalPlanExecutorRef,
    catalog_manager: CatalogManagerRef,
    scan_builder: ExecutableScanBuilderRef,

    // TODO: hold `SessionContext` here rather than these two parts.
    runtime_env: Arc<RuntimeEnv>,
    function_registry: Arc<dyn FunctionRegistry + Send + Sync>,

    extension_codec: Arc<dyn PhysicalExtensionCodec>,
}

impl Resolver {
    pub fn new(
        remote_executor: RemotePhysicalPlanExecutorRef,
        catalog_manager: CatalogManagerRef,
        scan_builder: ExecutableScanBuilderRef,
        runtime_env: Arc<RuntimeEnv>,
        function_registry: Arc<dyn FunctionRegistry + Send + Sync>,
    ) -> Self {
        Self {
            remote_executor,
            catalog_manager,
            scan_builder,
            runtime_env,
            function_registry,
            extension_codec: Arc::new(PhysicalExtensionCodecImpl::new()),
        }
    }

    /// Resolve partitioned scan
    pub fn resolve_partitioned_scan(
        &self,
        plan: Arc<dyn ExecutionPlan>,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
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
                extension_codec: self.extension_codec.clone(),
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
            let child = self.resolve_partitioned_scan(child)?;

            new_children.push(child);
        }

        // There may be `ResolvedPartitionedScan` node in children, try to extend such
        // children.
        // TODO: push down the computation physical node here.
        Self::maybe_extend_partitioned_scan(new_children, plan)
    }

    /// Resolve encoded sub table scanning plan.
    pub fn resolve_sub_scan(&self, encoded_plan: &[u8]) -> DfResult<Arc<dyn ExecutionPlan>> {
        // Decode to datafusion physical plan.
        let protobuf = protobuf::PhysicalPlanNode::decode(encoded_plan).map_err(|e| {
            DataFusionError::Plan(format!("failed to decode bytes to physical plan, err:{e}"))
        })?;
        protobuf.try_into_physical_plan(
            self.function_registry.as_ref(),
            &self.runtime_env,
            self.extension_codec.as_ref(),
        )
    }

    pub fn resolve_sub_scan_internal(
        &self,
        plan: Arc<dyn ExecutionPlan>,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
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
            let child = self.resolve_sub_scan_internal(child)?;

            new_children.push(child);
        }

        plan.with_new_children(new_children)
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
    use std::sync::Arc;

    use async_trait::async_trait;
    use catalog::{manager::ManagerRef, test_util::MockCatalogManagerBuilder};
    use common_types::{projected_schema::ProjectedSchema, tests::build_schema_for_cpu};
    use datafusion::{
        error::Result as DfResult,
        execution::{runtime_env::RuntimeEnv, FunctionRegistry, TaskContext},
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

    use crate::{
        codec::PhysicalExtensionCodecImpl,
        dist_sql_query::{
            physical_plan::{UnresolvedPartitionedScan, UnresolvedSubTableScan},
            resolver::Resolver,
            EncodedPlan, ExecutableScanBuilder, RemotePhysicalPlanExecutor,
        },
    };

    #[test]
    fn test_resolve_simple_partitioned_scan() {
        let ctx = TestContext::new();
        let plan = ctx.build_basic_partitioned_table_plan();
        let resolver = ctx.resolver();
        let new_plan = displayable(resolver.resolve_partitioned_scan(plan).unwrap().as_ref())
            .indent(true)
            .to_string();
        insta::assert_snapshot!(new_plan);
    }

    #[test]
    fn test_resolve_simple_sub_scan() {
        let ctx = TestContext::new();
        let plan = ctx.build_basic_sub_table_plan();
        let resolver = ctx.resolver();
        let new_plan = displayable(resolver.resolve_sub_scan_internal(plan).unwrap().as_ref())
            .indent(true)
            .to_string();
        insta::assert_snapshot!(new_plan);
    }

    // Mock function registry
    struct MockFunctionRegistry;

    impl FunctionRegistry for MockFunctionRegistry {
        fn udfs(&self) -> std::collections::HashSet<String> {
            unimplemented!()
        }

        fn udf(&self, _name: &str) -> DfResult<Arc<datafusion::logical_expr::ScalarUDF>> {
            unimplemented!()
        }

        fn udaf(&self, _name: &str) -> DfResult<Arc<datafusion::logical_expr::AggregateUDF>> {
            unimplemented!()
        }

        fn udwf(&self, _name: &str) -> DfResult<Arc<datafusion::logical_expr::WindowUDF>> {
            unimplemented!()
        }
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
            _task_context: &TaskContext,
            _encoded_plan: EncodedPlan,
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

        // Return resolver
        fn resolver(&self) -> Resolver {
            Resolver {
                remote_executor: Arc::new(MockRemotePhysicalPlanExecutor),
                catalog_manager: self.catalog_manager.clone(),
                scan_builder: Box::new(MockScanBuilder),
                runtime_env: Arc::new(RuntimeEnv::default()),
                function_registry: Arc::new(MockFunctionRegistry),
                extension_codec: Arc::new(PhysicalExtensionCodecImpl::new()),
            }
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
