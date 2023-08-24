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
/// plan to good one, rather than making a inexecutable plan executable.
/// So we define `Resolver` to make it, it may be somthing similar to task
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

    use datafusion::physical_plan::displayable;

    use crate::dist_sql_query::test_util::TestContext;

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
}
