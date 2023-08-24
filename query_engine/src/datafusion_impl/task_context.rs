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
use common_types::request_id::RequestId;
use datafusion::{
    error::{DataFusionError, Result as DfResult},
    execution::{runtime_env::RuntimeEnv, FunctionRegistry, TaskContext},
    physical_plan::{ExecutionPlan, SendableRecordBatchStream},
};
use df_engine_extensions::dist_sql_query::{
    resolver::Resolver, EncodedPlan, ExecutableScanBuilder, RemotePhysicalPlanExecutor,
};
use generic_error::BoxError;
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
}

impl Preprocessor {
    pub fn new(
        remote_engine: RemoteEngineRef,
        catalog_manager: CatalogManagerRef,
        runtime_env: Arc<RuntimeEnv>,
        function_registry: Arc<dyn FunctionRegistry + Send + Sync>,
    ) -> Self {
        let remote_executor = Arc::new(RemotePhysicalPlanExecutorImpl { remote_engine });
        let scan_builder = Box::new(ExecutableScanBuilderImpl);
        let resolver = Resolver::new(
            remote_executor,
            catalog_manager,
            scan_builder,
            runtime_env,
            function_registry,
        );

        Self {
            dist_query_resolver: resolver,
        }
    }

    pub fn process(&self, typed_plan: &TypedPlan) -> Result<Arc<dyn ExecutionPlan>> {
        match typed_plan {
            TypedPlan::Normal(plan) => Ok(plan.clone()),
            TypedPlan::Partitioned(plan) => self.preprocess_partitioned_table_plan(plan),
            TypedPlan::Remote(plan) => self.preprocess_remote_plan(plan),
        }
    }

    fn preprocess_remote_plan(&self, encoded_plan: &[u8]) -> Result<Arc<dyn ExecutionPlan>> {
        self.dist_query_resolver
            .resolve_sub_scan(encoded_plan)
            .box_err()
            .with_context(|| ExecutorWithCause {
                msg: format!("failed to preprocess remote plan"),
            })
    }

    fn preprocess_partitioned_table_plan(
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
            .finish()
    }
}

/// Remote physical plan executor impl
#[derive(Debug)]
struct RemotePhysicalPlanExecutorImpl {
    remote_engine: RemoteEngineRef,
}

#[async_trait]
impl RemotePhysicalPlanExecutor for RemotePhysicalPlanExecutorImpl {
    async fn execute(
        &self,
        table: TableIdentifier,
        task_context: &TaskContext,
        encoded_plan: EncodedPlan,
    ) -> DfResult<SendableRecordBatchStream> {
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

        // Build request.
        let remote_request = RemoteExecuteRequest {
            context: exec_ctx,
            physical_plan: PhysicalPlan::Datafusion(encoded_plan.plan),
        };

        let request = ExecutePlanRequest {
            table,
            plan_schema: encoded_plan.schema,
            remote_request,
        };

        // Remote execute.
        let stream = self
            .remote_engine
            .execute_physical_plan(request)
            .await
            .map_err(|e| {
                DataFusionError::Internal(format!(
                    "failed to execute physical plan by remote engine, err:{e}"
                ))
            })?;

        Ok(Box::pin(ToDfStream(stream)))
    }
}

/// Executable scan build impl
#[derive(Debug)]
struct ExecutableScanBuilderImpl;

impl ExecutableScanBuilder for ExecutableScanBuilderImpl {
    fn build(
        &self,
        table: TableRef,
        read_request: ReadRequest,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(ScanTable::new(table, read_request)))
    }
}
