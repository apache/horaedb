// Copyright 2023 CeresDB Project Authors. Licensed under Apache-2.0.

//! The proxy module provides features such as forwarding and authentication,
//! adapts to different protocols.

pub(crate) mod error;
#[allow(dead_code)]
pub mod forward;
pub(crate) mod grpc;
pub(crate) mod http;

use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use ::http::StatusCode;
use common_types::request_id::RequestId;
use common_util::{error::BoxError, runtime::Runtime};
use interpreters::{context::Context as InterpreterContext, factory::Factory, interpreter::Output};
use query_engine::executor::Executor as QueryExecutor;
use router::{endpoint::Endpoint, Router};
use snafu::ResultExt;
use sql::plan::Plan;

use crate::{
    instance::InstanceRef,
    proxy::{
        error::{ErrWithCause, Result},
        forward::{Forwarder, ForwarderRef},
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
    ) -> Self {
        let forwarder = Arc::new(Forwarder::new(
            forward_config,
            router.clone(),
            local_endpoint,
        ));
        Self {
            router,
            instance,
            forwarder,
            resp_compress_min_length,
            auto_create_table,
            schema_config_provider,
        }
    }

    pub fn instance(&self) -> InstanceRef<Q> {
        self.instance.clone()
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
