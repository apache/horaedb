// Copyright 2023 CeresDB Project Authors. Licensed under Apache-2.0.

pub(crate) mod error;
#[allow(dead_code)]
mod forward;
pub(crate) mod grpc;

use std::{sync::Arc, time::Duration};

use common_util::runtime::Runtime;
use query_engine::executor::Executor as QueryExecutor;
use router::Router;

use crate::{
    instance::InstanceRef, proxy::forward::ForwarderRef,
    schema_config_provider::SchemaConfigProviderRef,
};

pub struct Proxy<Q: QueryExecutor + 'static> {
    router: Arc<dyn Router + Send + Sync>,
    forwarder: ForwarderRef,
    instance: InstanceRef<Q>,
    resp_compress_min_length: usize,
    schema_config_provider: SchemaConfigProviderRef,
}

impl<Q: QueryExecutor + 'static> Proxy<Q> {
    #[allow(dead_code)]
    pub fn new(
        router: Arc<dyn Router + Send + Sync>,
        instance: InstanceRef<Q>,
        forwarder: ForwarderRef,
        resp_compress_min_length: usize,
        schema_config_provider: SchemaConfigProviderRef,
    ) -> Self {
        Self {
            router,
            instance,
            forwarder,
            resp_compress_min_length,
            schema_config_provider,
        }
    }
}

pub struct Context {
    pub timeout: Option<Duration>,
    pub runtime: Arc<Runtime>,
}
