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

use ceresdbproto::remote_engine::{extension_node::TypedExtension, ExtensionNode};
use datafusion::{
    error::{DataFusionError, Result as DfResult},
    execution::FunctionRegistry,
    physical_plan::ExecutionPlan,
};
use datafusion_proto::physical_plan::PhysicalExtensionCodec;
use prost::Message;

use crate::dist_sql_query::codec::DistSqlQueryCodec;

/// Codec for specific extension physical plan
pub trait TypedPhysicalExtensionCodec: fmt::Debug + Sync + Send + 'static {
    fn try_decode(
        &self,
        typed_extension: &TypedExtension,
        inputs: &[Arc<dyn ExecutionPlan>],
        registry: &dyn FunctionRegistry,
    ) -> Option<DfResult<Arc<dyn ExecutionPlan>>>;

    fn try_encode(&self, node: Arc<dyn ExecutionPlan>) -> Option<DfResult<TypedExtension>>;
}

/// CeresDB datafusion `PhysicalExtensionCodec`
/// Each extension physical plan will define its `TypedPhysicalExtensionCodec`,
/// and register into here.
#[derive(Debug)]
pub struct PhysicalExtensionCodecImpl {
    typed_codecs: Vec<Box<dyn TypedPhysicalExtensionCodec>>,
}

impl PhysicalExtensionCodecImpl {
    pub fn new() -> Self {
        let typed_codecs = vec![Box::new(DistSqlQueryCodec) as _];

        Self { typed_codecs }
    }
}

impl PhysicalExtensionCodec for PhysicalExtensionCodecImpl {
    fn try_decode(
        &self,
        buf: &[u8],
        inputs: &[Arc<dyn ExecutionPlan>],
        registry: &dyn datafusion::execution::FunctionRegistry,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        let extension_node = ExtensionNode::decode(buf).map_err(|e| {
            DataFusionError::Internal(format!("failed to decode extension physical plan, err{e}"))
        })?;

        let typed_extension = extension_node
            .typed_extension
            .ok_or(DataFusionError::Internal(
                "typed extension not found".to_string(),
            ))?;

        for typed_codec in &self.typed_codecs {
            if let Some(result) = typed_codec.try_decode(&typed_extension, inputs, registry) {
                return result;
            }
        }

        Err(DataFusionError::Internal(
            "unimplemented extension physical plan".to_string(),
        ))
    }

    fn try_encode(&self, node: Arc<dyn ExecutionPlan>, buf: &mut Vec<u8>) -> DfResult<()> {
        for typed_codec in &self.typed_codecs {
            if let Some(result) = typed_codec.try_encode(node.clone()) {
                let typed_extension = result?;
                let extension_node = ExtensionNode {
                    typed_extension: Some(typed_extension),
                };

                return extension_node.encode(buf).map_err(|e| {
                    DataFusionError::Internal(format!(
                        "failed to encode extension physical plan, err{e}"
                    ))
                });
            }
        }

        Err(DataFusionError::Internal(
            "unimplemented extension physical plan".to_string(),
        ))
    }
}

impl Default for PhysicalExtensionCodecImpl {
    fn default() -> Self {
        Self::new()
    }
}
