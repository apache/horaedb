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

use datafusion::{error::Result as DfResult, physical_plan::ExecutionPlan};
use table_engine::remote::RemoteEngineRef;

use crate::dist_sql_query::{
    partitioned_table_scan::{ResolvedPartitionedScan, UnresolvedPartitionedScan},
    sub_table_scan::UnresolvedSubTableScan,
};

struct PartitionedTablePreprocessor {
    remote_engine: RemoteEngineRef,
}

impl PartitionedTablePreprocessor {
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
                remote_engine: self.remote_engine.clone(),
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

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use common_types::{projected_schema::ProjectedSchema, tests::build_schema_for_cpu};
    use datafusion::{
        logical_expr::{expr_fn, Literal, Operator},
        physical_plan::{
            displayable,
            expressions::{binary, col, lit},
            filter::FilterExec,
            projection::ProjectionExec,
            ExecutionPlan, PhysicalExpr,
        },
        scalar::ScalarValue,
    };
    use table_engine::{
        memory::MockRemoteEngine,
        predicate::PredicateBuilder,
        remote::model::TableIdentifier,
        table::{ReadOptions, ReadRequest},
    };
    use trace_metric::MetricsCollector;

    use crate::dist_sql_query::{
        partitioned_scan_preprocessor::PartitionedTablePreprocessor,
        partitioned_table_scan::UnresolvedPartitionedScan,
    };

    #[test]
    fn test_resolve_plan() {
        let plan = build_test_physical_plan();
        let preprocessor = PartitionedTablePreprocessor {
            remote_engine: Arc::new(MockRemoteEngine),
        };
        let new_plan = displayable(preprocessor.resolve_plan(plan).unwrap().as_ref())
            .indent(true)
            .to_string();
        insta::assert_snapshot!(new_plan);
    }

    fn build_test_physical_plan() -> Arc<dyn ExecutionPlan> {
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
            ProjectedSchema::new(test_schema.clone(), Some(projection.clone())).unwrap();
        // Filter: time < 1691974518000 and tag1 == 'test_tag'
        let logical_filters = vec![(expr_fn::col("time").lt(ScalarValue::TimestampMillisecond(
            Some(1691974518000),
            None,
        )
        .lit()))
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
            projected_schema: projected_schema.clone(),
            predicate,
            metrics_collector: MetricsCollector::default(),
        };
        let unresolved_scan = Arc::new(UnresolvedPartitionedScan {
            sub_tables,
            read_request,
        });

        let filter: Arc<dyn ExecutionPlan> =
            Arc::new(FilterExec::try_new(physical_filter, unresolved_scan).unwrap());

        Arc::new(ProjectionExec::try_new(physical_projection, filter).unwrap())
    }
}
