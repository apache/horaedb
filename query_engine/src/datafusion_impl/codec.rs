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

use std::{fmt, sync::Arc};

use bytes_ext::Bytes;
use datafusion::execution::{runtime_env::RuntimeEnv, FunctionRegistry};
use datafusion_proto::{
    bytes::physical_plan_to_bytes_with_extension_codec,
    physical_plan::{AsExecutionPlan, PhysicalExtensionCodec as DatafusionPhysicalExtensionCodec},
    protobuf,
};
use generic_error::BoxError;
use prost::Message;
use snafu::ResultExt;

use crate::{
    codec::PhysicalPlanCodec, datafusion_impl::physical_plan::DataFusionPhysicalPlanAdapter,
    error::*, physical_planner::PhysicalPlanPtr,
};

/// Datafusion encoder powered by `datafusion-proto`
// TODO: replace `datafusion-proto` with `substrait`?
// TODO: make use of it.
pub struct DataFusionPhysicalPlanEncoderImpl<C> {
    runtime_env: Arc<RuntimeEnv>,
    function_registry: Arc<dyn FunctionRegistry + Send + Sync>,
    extension_codec: C,
}

impl<C: DatafusionPhysicalExtensionCodec> DataFusionPhysicalPlanEncoderImpl<C> {
    // TODO: use `SessionContext` to init it.
    pub fn new(
        runtime_env: Arc<RuntimeEnv>,
        function_registry: Arc<dyn FunctionRegistry + Send + Sync>,
        extension_codec: C,
    ) -> Self {
        Self {
            runtime_env,
            function_registry,
            extension_codec,
        }
    }
}

impl<C: DatafusionPhysicalExtensionCodec + Send + Sync + 'static> fmt::Debug
    for DataFusionPhysicalPlanEncoderImpl<C>
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "DataFusionPhysicalPlanEncoderImpl")
    }
}

impl<C: DatafusionPhysicalExtensionCodec + Send + Sync + 'static> PhysicalPlanCodec
    for DataFusionPhysicalPlanEncoderImpl<C>
{
    fn encode(&self, plan: &crate::physical_planner::PhysicalPlanPtr) -> Result<Bytes> {
        if let Some(plan) = plan
            .as_any()
            .downcast_ref::<DataFusionPhysicalPlanAdapter>()
        {
            physical_plan_to_bytes_with_extension_codec(
                plan.as_df_physical_plan(),
                &self.extension_codec,
            )
            .box_err()
            .context(PhysicalPlanEncoderWithCause {
                msg: "failed to encode datafusion physical plan",
            })
        } else {
            PhysicalPlanEncoderNoCause {
                msg: format!("unexpected physical plan:{plan:?}"),
            }
            .fail()
        }
    }

    fn decode(&self, bytes: &[u8]) -> Result<PhysicalPlanPtr> {
        let plan_pb = protobuf::PhysicalPlanNode::decode(bytes)
            .box_err()
            .with_context(|| PhysicalPlanEncoderWithCause {
                msg: "failed to decode bytes to protobuf plan".to_string(),
            })?;

        let df_plan = plan_pb
            .try_into_physical_plan(self.function_registry.as_ref(), &self.runtime_env, &self.extension_codec)
            .box_err()
            .with_context(|| PhysicalPlanEncoderWithCause {
                msg: format!("failed to convert protobuf plan to datafusion physical plan, protobuf plan:{plan_pb:?}") ,
            })?;

        Ok(Box::new(DataFusionPhysicalPlanAdapter::new(df_plan)))
    }
}
