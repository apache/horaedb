// Copyright 2023 CeresDB Project Authors. Licensed under Apache-2.0.

//! The proxy module provides features such as forwarding and authentication,
//! adapts to different protocols.

pub(crate) mod error;
#[allow(dead_code)]
pub mod forward;
pub(crate) mod grpc;
pub mod hotspot;
mod hotspot_lru;
pub(crate) mod http;
pub(crate) mod util;

use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use ::http::StatusCode;
use catalog::schema::{CreateOptions, CreateTableRequest, DropOptions, DropTableRequest};
use ceresdbproto::storage::{
    storage_service_client::StorageServiceClient, PrometheusRemoteQueryRequest,
    PrometheusRemoteQueryResponse, SqlQueryRequest, SqlQueryResponse,
};
use common_types::{request_id::RequestId, table::DEFAULT_SHARD_ID};
use common_util::{error::BoxError, runtime::Runtime};
use futures::FutureExt;
use interpreters::{context::Context as InterpreterContext, factory::Factory, interpreter::Output};
use log::{error, warn};
use query_engine::executor::Executor as QueryExecutor;
use query_frontend::{frontend, plan::Plan};
use router::{endpoint::Endpoint, Router};
use snafu::{OptionExt, ResultExt};
use table_engine::{
    engine::{EngineRuntimes, TableState},
    remote::model::{GetTableInfoRequest, TableIdentifier},
    table::TableId,
    PARTITION_TABLE_ENGINE_TYPE,
};
use tonic::{transport::Channel, IntoRequest};

use crate::{
    instance::InstanceRef,
    proxy::{
        error::{ErrNoCause, ErrWithCause, Error, Internal, Result},
        forward::{ForwardRequest, ForwardResult, Forwarder, ForwarderRef},
        hotspot::HotspotRecorder,
    },
    schema_config_provider::SchemaConfigProviderRef,
};

pub struct Proxy<Q> {
    router: Arc<dyn Router + Send + Sync>,
    forwarder: ForwarderRef,
    instance: InstanceRef<Q>,
    resp_compress_min_length: usize,
    auto_create_table: bool,
    schema_config_provider: SchemaConfigProviderRef,
    hotspot_recorder: Arc<HotspotRecorder>,
    engine_runtimes: Arc<EngineRuntimes>,
}

impl<Q: QueryExecutor + 'static> Proxy<Q> {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        router: Arc<dyn Router + Send + Sync>,
        instance: InstanceRef<Q>,
        forward_config: forward::Config,
        local_endpoint: Endpoint,
        resp_compress_min_length: usize,
        auto_create_table: bool,
        schema_config_provider: SchemaConfigProviderRef,
        hotspot_config: hotspot::Config,
        engine_runtimes: Arc<EngineRuntimes>,
    ) -> Self {
        let forwarder = Arc::new(Forwarder::new(
            forward_config,
            router.clone(),
            local_endpoint,
        ));
        let hotspot_recorder = Arc::new(HotspotRecorder::new(
            hotspot_config,
            engine_runtimes.default_runtime.clone(),
        ));

        Self {
            router,
            instance,
            forwarder,
            resp_compress_min_length,
            auto_create_table,
            schema_config_provider,
            hotspot_recorder,
            engine_runtimes,
        }
    }

    pub fn instance(&self) -> InstanceRef<Q> {
        self.instance.clone()
    }

    async fn maybe_forward_sql_query(
        &self,
        req: &SqlQueryRequest,
    ) -> Result<Option<ForwardResult<SqlQueryResponse, Error>>> {
        let table_name = frontend::parse_table_name_with_sql(&req.sql)
            .box_err()
            .with_context(|| Internal {
                msg: format!("Failed to parse table name with sql, sql:{}", req.sql),
            })?;
        if table_name.is_none() {
            warn!("Unable to forward sql query without table name, req:{req:?}",);
            return Ok(None);
        }

        let req_ctx = req.context.as_ref().unwrap();
        let forward_req = ForwardRequest {
            schema: req_ctx.database.clone(),
            table: table_name.unwrap(),
            req: req.clone().into_request(),
        };
        let do_query = |mut client: StorageServiceClient<Channel>,
                        request: tonic::Request<SqlQueryRequest>,
                        _: &Endpoint| {
            let query = async move {
                client
                    .sql_query(request)
                    .await
                    .map(|resp| resp.into_inner())
                    .box_err()
                    .context(ErrWithCause {
                        code: StatusCode::INTERNAL_SERVER_ERROR,
                        msg: "Forwarded sql query failed",
                    })
            }
            .boxed();

            Box::new(query) as _
        };

        let forward_result = self.forwarder.forward(forward_req, do_query).await;
        Ok(match forward_result {
            Ok(forward_res) => Some(forward_res),
            Err(e) => {
                error!("Failed to forward sql req but the error is ignored, err:{e}");
                None
            }
        })
    }

    async fn maybe_forward_prom_remote_query(
        &self,
        metric: String,
        req: PrometheusRemoteQueryRequest,
    ) -> Result<Option<ForwardResult<PrometheusRemoteQueryResponse, Error>>> {
        let req_ctx = req.context.as_ref().unwrap();
        let forward_req = ForwardRequest {
            schema: req_ctx.database.clone(),
            table: metric,
            req: req.into_request(),
        };
        let do_query = |mut client: StorageServiceClient<Channel>,
                        request: tonic::Request<PrometheusRemoteQueryRequest>,
                        _: &Endpoint| {
            let query = async move {
                client
                    .prom_remote_query(request)
                    .await
                    .map(|resp| resp.into_inner())
                    .box_err()
                    .context(ErrWithCause {
                        code: StatusCode::INTERNAL_SERVER_ERROR,
                        msg: "Forwarded sql query failed",
                    })
            }
            .boxed();

            Box::new(query) as _
        };

        let forward_result = self.forwarder.forward(forward_req, do_query).await;
        Ok(match forward_result {
            Ok(forward_res) => Some(forward_res),
            Err(e) => {
                error!("Failed to forward prom req but the error is ignored, err:{e}");
                None
            }
        })
    }

    async fn maybe_open_partition_table_if_not_exist(
        &self,
        catalog_name: &str,
        schema_name: &str,
        table_name: &str,
    ) -> Result<()> {
        let partition_table_info = self
            .router
            .fetch_partition_table_info(schema_name, table_name)
            .await?;
        if partition_table_info.is_none() {
            return Ok(());
        }

        let partition_table_info = partition_table_info.unwrap();

        let catalog = self
            .instance
            .catalog_manager
            .catalog_by_name(catalog_name)
            .box_err()
            .with_context(|| ErrWithCause {
                code: StatusCode::INTERNAL_SERVER_ERROR,
                msg: format!("Failed to find catalog, catalog_name:{catalog_name}"),
            })?
            .with_context(|| ErrNoCause {
                code: StatusCode::BAD_REQUEST,
                msg: format!("Catalog not found, catalog_name:{catalog_name}"),
            })?;

        // TODO: support create schema if not exist
        let schema = catalog
            .schema_by_name(schema_name)
            .box_err()
            .with_context(|| ErrWithCause {
                code: StatusCode::INTERNAL_SERVER_ERROR,
                msg: format!("Failed to find schema, schema_name:{schema_name}"),
            })?
            .context(ErrNoCause {
                code: StatusCode::BAD_REQUEST,
                msg: format!("Schema not found, schema_name:{schema_name}"),
            })?;
        let table = schema
            .table_by_name(&partition_table_info.name)
            .box_err()
            .with_context(|| ErrWithCause {
                code: StatusCode::INTERNAL_SERVER_ERROR,
                msg: format!(
                    "Failed to find table, table_name:{}",
                    partition_table_info.name
                ),
            })?;

        if let Some(table) = table {
            if table.id().as_u64() == partition_table_info.id {
                return Ok(());
            }

            // Drop partition table if table id not match.
            let opts = DropOptions {
                table_engine: self.instance.partition_table_engine.clone(),
            };
            schema
                .drop_table(
                    DropTableRequest {
                        catalog_name: catalog_name.to_string(),
                        schema_name: schema_name.to_string(),
                        table_name: table_name.to_string(),
                        engine: PARTITION_TABLE_ENGINE_TYPE.to_string(),
                    },
                    opts,
                )
                .await
                .box_err()
                .with_context(|| ErrWithCause {
                    code: StatusCode::INTERNAL_SERVER_ERROR,
                    msg: format!("Failed to drop partition table, table_name:{table_name}"),
                })?;
        }

        // If table not exists, open it.
        // Get table_schema from first sub partition table.
        let first_sub_partition_table_name = util::get_sub_partition_name(
            &partition_table_info.name,
            &partition_table_info.partition_info,
            0usize,
        );
        let table = self
            .instance
            .remote_engine_ref
            .get_table_info(GetTableInfoRequest {
                table: TableIdentifier {
                    catalog: catalog_name.to_string(),
                    schema: schema_name.to_string(),
                    table: first_sub_partition_table_name,
                },
            })
            .await
            .box_err()
            .with_context(|| ErrWithCause {
                code: StatusCode::INTERNAL_SERVER_ERROR,
                msg: "Failed to get table",
            })?;

        // Partition table is a virtual table, so we need to create it manually.
        // Partition info is stored in ceresmeta, so we need to use create_table_request
        // to create it.
        let create_table_request = CreateTableRequest {
            catalog_name: catalog_name.to_string(),
            schema_name: schema_name.to_string(),
            table_name: partition_table_info.name,
            table_id: Some(TableId::new(partition_table_info.id)),
            table_schema: table.table_schema,
            engine: table.engine,
            options: table.options,
            state: TableState::Stable,
            shard_id: DEFAULT_SHARD_ID,
            partition_info: Some(partition_table_info.partition_info),
        };
        let create_opts = CreateOptions {
            table_engine: self.instance.partition_table_engine.clone(),
            create_if_not_exists: true,
        };
        schema
            .create_table(create_table_request.clone(), create_opts)
            .await
            .box_err()
            .with_context(|| ErrWithCause {
                code: StatusCode::INTERNAL_SERVER_ERROR,
                msg: format!("Failed to create table, request:{create_table_request:?}"),
            })?;
        Ok(())
    }
}

#[derive(Clone)]
pub struct Context {
    pub timeout: Option<Duration>,
    pub runtime: Arc<Runtime>,
}

async fn execute_plan<Q: QueryExecutor + 'static>(
    request_id: RequestId,
    catalog: &str,
    schema: &str,
    instance: InstanceRef<Q>,
    plan: Plan,
    deadline: Option<Instant>,
) -> Result<Output> {
    instance
        .limiter
        .try_limit(&plan)
        .box_err()
        .context(ErrWithCause {
            code: StatusCode::INTERNAL_SERVER_ERROR,
            msg: "Request is blocked",
        })?;

    let interpreter_ctx = InterpreterContext::builder(request_id, deadline)
        // Use current ctx's catalog and schema as default catalog and schema
        .default_catalog_and_schema(catalog.to_string(), schema.to_string())
        .build();
    let interpreter_factory = Factory::new(
        instance.query_executor.clone(),
        instance.catalog_manager.clone(),
        instance.table_engine.clone(),
        instance.table_manipulator.clone(),
    );
    let interpreter = interpreter_factory
        .create(interpreter_ctx, plan)
        .box_err()
        .context(ErrWithCause {
            code: StatusCode::INTERNAL_SERVER_ERROR,
            msg: "Failed to create interpreter",
        })?;

    if let Some(deadline) = deadline {
        tokio::time::timeout_at(
            tokio::time::Instant::from_std(deadline),
            interpreter.execute(),
        )
        .await
        .box_err()
        .context(ErrWithCause {
            code: StatusCode::INTERNAL_SERVER_ERROR,
            msg: "Plan execution timeout",
        })
        .and_then(|v| {
            v.box_err().context(ErrWithCause {
                code: StatusCode::INTERNAL_SERVER_ERROR,
                msg: "Failed to execute interpreter",
            })
        })
    } else {
        interpreter.execute().await.box_err().context(ErrWithCause {
            code: StatusCode::INTERNAL_SERVER_ERROR,
            msg: "Failed to execute interpreter",
        })
    }
}
