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

use datafusion::physical_plan::ExecutionPlan;
use table_engine::remote::RemoteEngineRef;

use crate::dist_sql_query::{
    partitioned_table_scan::{ResolvedPartitionedScan, UnresolvedPartitionedScan},
    sub_table_scan::UnresolvedSubTableScan,
};

use datafusion::error::Result as DfResult;

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

        // There may be `ResolvedPartitionedScan` node in children, try to extend such children.
        Self::maybe_extend_partitioned_scan(new_children, plan)
    }

    fn maybe_extend_partitioned_scan(
        new_children: Vec<Arc<dyn ExecutionPlan>>,
        current_node: Arc<dyn ExecutionPlan>,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        if new_children.is_empty() {
            return Ok(current_node);
        }

        // All the extendable nodes have just one child.
        let children_num = current_node.children().len();
        if children_num > 1 {
            return current_node.with_new_children(new_children);
        }

        let child = new_children.first().unwrap();
        if child
            .as_any()
            .downcast_ref::<ResolvedPartitionedScan>()
            .is_some()
        {
            // TODO: judge and merge the plan node able to push down.
            return Ok(child.clone());
        }

        Ok(child.clone())
    }
}
