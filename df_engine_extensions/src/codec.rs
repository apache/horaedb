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

use datafusion::{error::{Result as DfResult, DataFusionError}, physical_plan::ExecutionPlan};
use datafusion_proto::physical_plan::PhysicalExtensionCodec;

/// Codec for specific extension physical plan
pub trait TypedPhysicalExtensionCodec {
    fn try_decode(
        &self,
        buf: &[u8],
        inputs: &[Arc<dyn ExecutionPlan>],
        registry: &dyn FunctionRegistry,
    ) -> Option<DfResult<Arc<dyn ExecutionPlan>>>;

    fn try_encode(&self, node: Arc<dyn ExecutionPlan>, buf: &mut Vec<u8>) -> Option<DfDfResult<()>>;
}

/// CeresDB datafusion `PhysicalExtensionCodec`
/// Each extension physical plan will define its `TypedPhysicalExtensionCodec`, and register into here.
pub struct PhysicalExtensionCodecImpl {
    typed_codecs: Vec<Box<dyn TypedPhysicalExtensionCodec>>,
}

impl PhysicalExtensionCodec for PhysicalExtensionCodecImpl {
    fn try_decode(
        &self,
        buf: &[u8],
        inputs: &[Arc<dyn ExecutionPlan>],
        registry: &dyn datafusion::execution::FunctionRegistry,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        for typed_codec in &self.typed_codecs {
            if let Some(result) = typed_codec.try_decode(buf, inputs, registry) {
                return result;
            }
        }

        Err(DataFusionError::Internal("unimplemented extension physical plan".to_string()))
    }

    fn try_encode(&self, node: Arc<dyn ExecutionPlan>, buf: &mut Vec<u8>) -> DfResult<()> {
        for typed_codec in &self.typed_codecs {
            if let Some(result) = typed_codec.try_encode(node, buf) {
                return result;
            }
        }

        Err(DataFusionError::Internal("unimplemented extension physical plan".to_string()))
    }
}
