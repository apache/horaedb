// Copyright 2023 CeresDB Project Authors. Licensed under Apache-2.0.

//! The proxy module provides features such as forwarding and authentication,
//! adapts to different protocols.

pub(crate) mod error;
#[allow(dead_code)]
pub mod forward;
pub(crate) mod grpc;
pub mod hotspot;
pub mod hotspot_lru;
pub(crate) mod http;
pub(crate) mod util;

use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use ::http::StatusCode;
use ceresdbproto::storage::{
    storage_service_client::StorageServiceClient, SqlQueryRequest, SqlQueryResponse,
};
use common_types::request_id::RequestId;
use common_util::{error::BoxError, runtime::Runtime};
use futures::FutureExt;
use interpreters::{context::Context as InterpreterContext, factory::Factory, interpreter::Output};
use log::{error, warn};
use query_engine::executor::Executor as QueryExecutor;
use router::{endpoint::Endpoint, Router};
use snafu::ResultExt;
use sql::plan::Plan;
use tonic::{transport::Channel, IntoRequest};

use crate::{
    instance::InstanceRef,
    proxy::{
        error::{ErrWithCause, Error, Result},
        forward::{ForwardRequest, ForwardResult, Forwarder, ForwarderRef},
        hotspot::HotspotRecorder,
        util::parse_table_name_with_sql,
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
}

impl<Q: QueryExecutor + 'static> Proxy<Q> {
    pub fn new(
        router: Arc<dyn Router + Send + Sync>,
        instance: InstanceRef<Q>,
        forward_config: forward::Config,
        local_endpoint: Endpoint,
        resp_compress_min_length: usize,
        auto_create_table: bool,
        schema_config_provider: SchemaConfigProviderRef,
        hotspot_config: hotspot::Config,
        runtime: Arc<Runtime>,
    ) -> Self {
        let forwarder = Arc::new(Forwarder::new(
            forward_config,
            router.clone(),
            local_endpoint,
        ));
        let hotspot_recorder = Arc::new(HotspotRecorder::new(hotspot_config, runtime));

        Self {
            router,
            instance,
            forwarder,
            resp_compress_min_length,
            auto_create_table,
            schema_config_provider,
            hotspot_recorder,
        }
    }

    pub fn instance(&self) -> InstanceRef<Q> {
        self.instance.clone()
    }

    async fn maybe_forward_sql_query(
        &self,
        req: &SqlQueryRequest,
    ) -> Option<ForwardResult<SqlQueryResponse, Error>> {
        let table_name = parse_table_name_with_sql(&req.sql);
        if table_name.is_none() {
            warn!("Unable to forward sql query without table name, req:{req:?}",);
            return None;
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
        match forward_result {
            Ok(forward_res) => Some(forward_res),
            Err(e) => {
                error!("Failed to forward sql req but the error is ignored, err:{e}");
                None
            }
        }
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
