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

use std::{
    fmt,
    sync::Arc,
    time::{Duration, Instant},
};

use async_trait::async_trait;
use catalog::manager::ManagerRef as CatalogManagerRef;
use common_types::{request_id::RequestId, schema::RecordSchema};
use datafusion::{
    error::{DataFusionError, Result as DfResult},
    execution::{runtime_env::RuntimeEnv, FunctionRegistry, TaskContext},
    physical_plan::{ExecutionPlan, SendableRecordBatchStream},
};
use datafusion_proto::{
    bytes::physical_plan_to_bytes_with_extension_codec,
    physical_plan::{AsExecutionPlan, PhysicalExtensionCodec},
    protobuf,
};
use df_engine_extensions::dist_sql_query::{
    resolver::Resolver, ExecutableScanBuilder, RemotePhysicalPlanExecutor,
};
use futures::future::BoxFuture;
use generic_error::BoxError;
use prost::Message;
use snafu::ResultExt;
use table_engine::{
    provider::{CeresdbOptions, ScanTable},
    remote::{
        model::{
            ExecContext, ExecutePlanRequest, PhysicalPlan, RemoteExecuteRequest, TableIdentifier,
        },
        RemoteEngineRef,
    },
    stream::ToDfStream,
    table::{ReadRequest, TableRef},
};

use crate::{datafusion_impl::physical_plan::TypedPlan, error::*};

#[allow(dead_code)]
pub struct DatafusionTaskExecContext {
    pub task_ctx: Arc<TaskContext>,
    pub preprocessor: Arc<Preprocessor>,
}

/// Preprocessor for datafusion physical plan
#[allow(dead_code)]
pub struct Preprocessor {
    dist_query_resolver: Resolver,
    runtime_env: Arc<RuntimeEnv>,
    function_registry: Arc<dyn FunctionRegistry + Send + Sync>,
    extension_codec: Arc<dyn PhysicalExtensionCodec>,
}

impl Preprocessor {
    pub fn new(
        remote_engine: RemoteEngineRef,
        catalog_manager: CatalogManagerRef,
        runtime_env: Arc<RuntimeEnv>,
        function_registry: Arc<dyn FunctionRegistry + Send + Sync>,
        extension_codec: Arc<dyn PhysicalExtensionCodec>,
    ) -> Self {
        let remote_executor = Arc::new(RemotePhysicalPlanExecutorImpl {
            remote_engine,
            extension_codec: extension_codec.clone(),
        });
        let scan_builder = Box::new(ExecutableScanBuilderImpl);
        let resolver = Resolver::new(remote_executor, catalog_manager, scan_builder);

        Self {
            dist_query_resolver: resolver,
            runtime_env,
            function_registry,
            extension_codec,
        }
    }

    pub async fn process(&self, typed_plan: &TypedPlan) -> Result<Arc<dyn ExecutionPlan>> {
        match typed_plan {
            TypedPlan::Normal(plan) => Ok(plan.clone()),
            TypedPlan::Partitioned(plan) => self.preprocess_partitioned_table_plan(plan).await,
            TypedPlan::Remote(plan) => self.preprocess_remote_plan(plan).await,
        }
    }

    async fn preprocess_remote_plan(&self, encoded_plan: &[u8]) -> Result<Arc<dyn ExecutionPlan>> {
        // Decode to datafusion physical plan.
        let protobuf = protobuf::PhysicalPlanNode::decode(encoded_plan)
            .box_err()
            .with_context(|| ExecutorWithCause {
                msg: Some("failed to decode plan".to_string()),
            })?;
        let plan = protobuf
            .try_into_physical_plan(
                self.function_registry.as_ref(),
                &self.runtime_env,
                self.extension_codec.as_ref(),
            )
            .box_err()
            .with_context(|| ExecutorWithCause {
                msg: Some("failed to rebuild physical plan from the decoded plan".to_string()),
            })?;

        self.dist_query_resolver
            .resolve_sub_scan(plan)
            .await
            .box_err()
            .with_context(|| ExecutorWithCause {
                msg: format!("failed to preprocess remote plan"),
            })
    }

    async fn preprocess_partitioned_table_plan(
        &self,
        plan: &Arc<dyn ExecutionPlan>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        self.dist_query_resolver
            .resolve_partitioned_scan(plan.clone())
            .box_err()
            .with_context(|| ExecutorWithCause {
                msg: format!("failed to preprocess partitioned table plan, plan:{plan:?}"),
            })
    }
}

impl fmt::Debug for Preprocessor {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Preprocessor")
            .field("dist_query_resolver", &"preprocess partitioned table plan")
            .field("extension_codec", &self.extension_codec)
            .finish()
    }
}

/// Remote physical plan executor impl
#[derive(Debug)]
struct RemotePhysicalPlanExecutorImpl {
    remote_engine: RemoteEngineRef,
    extension_codec: Arc<dyn PhysicalExtensionCodec>,
}

impl RemotePhysicalPlanExecutor for RemotePhysicalPlanExecutorImpl {
    fn execute(
        &self,
        table: TableIdentifier,
        task_context: &TaskContext,
        plan: Arc<dyn ExecutionPlan>,
    ) -> DfResult<BoxFuture<'static, DfResult<SendableRecordBatchStream>>> {
        // Get the custom context to rebuild execution context.
        let ceresdb_options = task_context
            .session_config()
            .options()
            .extensions
            .get::<CeresdbOptions>();
        assert!(ceresdb_options.is_some());
        let ceresdb_options = ceresdb_options.unwrap();
        let request_id = RequestId::from(ceresdb_options.request_id);
        let deadline = ceresdb_options
            .request_timeout
            .map(|n| Instant::now() + Duration::from_millis(n));
        let default_catalog = ceresdb_options.default_catalog.clone();
        let default_schema = ceresdb_options.default_schema.clone();

        let exec_ctx = ExecContext {
            request_id,
            deadline,
            default_catalog,
            default_schema,
        };

        // Encode plan and schema
        let plan_schema = RecordSchema::try_from(plan.schema()).map_err(|e| {
            DataFusionError::Internal(format!(
                "failed to convert arrow_schema to record_schema, arrow_schema:{}, err:{e}",
                plan.schema()
            ))
        })?;

        let encoded_plan =
            physical_plan_to_bytes_with_extension_codec(plan, self.extension_codec.as_ref())?;

        // Build returned stream future.
        let remote_engine = self.remote_engine.clone();
        let future = Box::pin(async move {
            let remote_request = RemoteExecuteRequest {
                context: exec_ctx,
                physical_plan: PhysicalPlan::Datafusion(encoded_plan),
            };

            let request = ExecutePlanRequest {
                table,
                plan_schema,
                remote_request,
            };

            // Remote execute.
            let stream = remote_engine
                .execute_physical_plan(request)
                .await
                .map_err(|e| {
                    DataFusionError::Internal(format!(
                        "failed to execute physical plan by remote engine, err:{e}"
                    ))
                })?;

            Ok(Box::pin(ToDfStream(stream)) as _)
        });

        Ok(future)
    }
}

/// Executable scan build impl
#[derive(Debug)]
struct ExecutableScanBuilderImpl;

#[async_trait]
impl ExecutableScanBuilder for ExecutableScanBuilderImpl {
    async fn build(
        &self,
        table: TableRef,
        read_request: ReadRequest,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        let mut scan = ScanTable::new(table, read_request);
        scan.maybe_init_stream().await.map_err(|e| {
            DataFusionError::Internal(format!("failed to build executable table scan, err:{e}"))
        })?;
        Ok(Arc::new(scan))
    }
}
