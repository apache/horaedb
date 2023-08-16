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

use ceresdbproto::remote_engine::{
    dist_sql_query_extension_node::{self, TypedPlan},
    extension_node::TypedExtension,
    DistSqlQueryExtensionNode,
};
use datafusion::{
    error::{DataFusionError, Result as DfResult},
    execution::FunctionRegistry,
    physical_plan::ExecutionPlan,
};

use crate::{
    codec::TypedPhysicalExtensionCodec, dist_sql_query::physical_plan::UnresolvedSubTableScan,
};

#[derive(Debug)]
pub struct DistSqlQueryCodec;

impl TypedPhysicalExtensionCodec for DistSqlQueryCodec {
    // It is possible to have more extension physical plans, so `if let` is
    // necessary.
    #[allow(irrefutable_let_patterns)]
    fn try_decode(
        &self,
        typed_extension: &TypedExtension,
        _inputs: &[Arc<dyn ExecutionPlan>],
        _registry: &dyn FunctionRegistry,
    ) -> Option<DfResult<Arc<dyn ExecutionPlan>>> {
        if let TypedExtension::DistSqlQuery(extension) = typed_extension {
            match extension.typed_plan.clone() {
                Some(TypedPlan::UnreolvedSubScan(plan_pb)) => {
                    Some(UnresolvedSubTableScan::try_from(plan_pb).map(|plan| Arc::new(plan) as _))
                }
                None => Some(Err(DataFusionError::Internal(format!(
                    "actual node not found in dist query extension plan"
                )))),
            }
        } else {
            None
        }
    }

    fn try_encode(&self, node: Arc<dyn ExecutionPlan>) -> Option<DfResult<TypedExtension>> {
        let plan_pb_res: DfResult<DistSqlQueryExtensionNode> =
            if let Some(plan) = node.as_any().downcast_ref::<UnresolvedSubTableScan>() {
                plan.clone().try_into().map(|pb| DistSqlQueryExtensionNode {
                    typed_plan: Some(TypedPlan::UnreolvedSubScan(pb)),
                })
            } else {
                return None;
            };

        Some(plan_pb_res.map(|pb| TypedExtension::DistSqlQuery(pb)))
    }
}
