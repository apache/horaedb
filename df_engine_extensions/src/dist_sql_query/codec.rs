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

use ceresdbproto::remote_engine::{extension_node::TypedExtension, DistSqlQueryExtensionNode};
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
            match extension.unresolved_sub_scan.clone() {
                Some(plan_pb) => {
                    Some(UnresolvedSubTableScan::try_from(plan_pb).map(|plan| Arc::new(plan) as _))
                }
                None => Some(Err(DataFusionError::Internal(
                    "actual node not found in dist query extension plan".to_string(),
                ))),
            }
        } else {
            None
        }
    }

    fn try_encode(&self, node: Arc<dyn ExecutionPlan>) -> Option<DfResult<TypedExtension>> {
        let plan_pb_res: DfResult<DistSqlQueryExtensionNode> =
            if let Some(plan) = node.as_any().downcast_ref::<UnresolvedSubTableScan>() {
                plan.clone().try_into().map(|pb| DistSqlQueryExtensionNode {
                    unresolved_sub_scan: Some(pb),
                })
            } else {
                return None;
            };

        Some(plan_pb_res.map(TypedExtension::DistSqlQuery))
    }
}

#[cfg(test)]
mod test {
    use datafusion::{physical_plan::displayable, prelude::SessionContext};
    use datafusion_proto::bytes::{
        physical_plan_from_bytes_with_extension_codec, physical_plan_to_bytes_with_extension_codec,
    };

    use crate::{codec::PhysicalExtensionCodecImpl, dist_sql_query::test_util::TestContext};

    #[test]
    fn test_sub_table_scan_codec() {
        let test_ctx = TestContext::default();
        let sub_table_plan = test_ctx.build_basic_sub_table_plan();
        let extension_codec = PhysicalExtensionCodecImpl::default();
        let session_ctx = SessionContext::default();

        // Encode and decode again
        let encoded_plan =
            physical_plan_to_bytes_with_extension_codec(sub_table_plan.clone(), &extension_codec)
                .unwrap();
        let re_decoded_plan = physical_plan_from_bytes_with_extension_codec(
            &encoded_plan,
            &session_ctx,
            &extension_codec,
        )
        .unwrap();

        // Compare.
        let expected = displayable(sub_table_plan.as_ref())
            .indent(true)
            .to_string();
        let re_decoded = displayable(re_decoded_plan.as_ref())
            .indent(true)
            .to_string();

        assert_eq!(expected, re_decoded);
    }
}
