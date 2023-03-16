// Copyright 2023 CeresDB Project Authors. Licensed under Apache-2.0.

pub(crate) mod error;
#[allow(dead_code)]
pub mod forward;
pub(crate) mod grpc;

use std::{str::FromStr, sync::Arc, time::Duration};

use common_util::{error::BoxError, runtime::Runtime};
use query_engine::executor::Executor as QueryExecutor;
use router::{endpoint::Endpoint, Router};
use snafu::ResultExt;

use crate::{
    instance::InstanceRef,
    proxy::{
        error::{Internal, Result},
        forward::{Forwarder, ForwarderRef},
    },
    schema_config_provider::SchemaConfigProviderRef,
};

pub struct Proxy<Q: QueryExecutor + 'static> {
    router: Arc<dyn Router + Send + Sync>,
    forwarder: ForwarderRef,
    instance: InstanceRef<Q>,
    resp_compress_min_length: usize,
    auto_create_table: bool,
    schema_config_provider: SchemaConfigProviderRef,
}

impl<Q: QueryExecutor + 'static> Proxy<Q> {
    pub fn try_new(
        router: Arc<dyn Router + Send + Sync>,
        instance: InstanceRef<Q>,
        forward_config: forward::Config,
        local_endpoint: String,
        resp_compress_min_length: usize,
        auto_create_table: bool,
        schema_config_provider: SchemaConfigProviderRef,
    ) -> Result<Self> {
        let local_endpoint = Endpoint::from_str(&local_endpoint).with_context(|| Internal {
            msg: format!("invalid local endpoint, input:{local_endpoint}"),
        })?;
        let forwarder = Arc::new(
            Forwarder::try_new(forward_config, router.clone(), local_endpoint)
                .box_err()
                .context(Internal {
                    msg: "fail to init forward",
                })?,
        );
        Ok(Self {
            router,
            instance,
            forwarder,
            resp_compress_min_length,
            auto_create_table,
            schema_config_provider,
        })
    }
}

#[derive(Clone)]
pub struct Context {
    pub timeout: Option<Duration>,
    pub runtime: Arc<Runtime>,
}
