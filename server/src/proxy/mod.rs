// Copyright 2023 CeresDB Project Authors. Licensed under Apache-2.0.

//! The proxy module provides features such as forwarding and authentication,
//! adapts to different protocols.

pub(crate) mod error;
#[allow(dead_code)]
pub mod forward;
pub(crate) mod grpc;
pub mod hotspot;
pub mod hotspot_lru;
pub mod util;

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
        hotspot::HotspotRecorder,
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
    hotspot_recorder: Arc<HotspotRecorder>,
}

impl<Q: QueryExecutor + 'static> Proxy<Q> {
    #[allow(clippy::too_many_arguments)]
    pub fn try_new(
        router: Arc<dyn Router + Send + Sync>,
        instance: InstanceRef<Q>,
        forward_config: forward::Config,
        local_endpoint: String,
        resp_compress_min_length: usize,
        auto_create_table: bool,
        schema_config_provider: SchemaConfigProviderRef,
        hotspot_config: hotspot::Config,
        runtime: Arc<Runtime>,
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
        let hotspot_recorder = Arc::new(HotspotRecorder::new(hotspot_config, runtime));
        Ok(Self {
            router,
            instance,
            forwarder,
            resp_compress_min_length,
            auto_create_table,
            schema_config_provider,
            hotspot_recorder,
        })
    }
}

#[derive(Clone)]
pub struct Context {
    pub timeout: Option<Duration>,
    pub runtime: Arc<Runtime>,
}
