pub(crate) mod error;
mod forward;
mod grpc;

use std::{sync::Arc, time::Duration};

use ceresdbproto::storage::{RouteRequest, RouteResponse};
use common_util::runtime::Runtime;
use query_engine::executor::Executor as QueryExecutor;
use router::Router;

use crate::{
    instance::InstanceRef,
    proxy::{error::Result, forward::ForwarderRef},
    schema_config_provider::SchemaConfigProviderRef,
};

pub struct Proxy<Q: QueryExecutor + 'static> {
    router: Arc<dyn Router + Send + Sync>,
    forwarder: Option<ForwarderRef>,
    instance: InstanceRef<Q>,
    schema_config_provider: SchemaConfigProviderRef,
}

impl<Q: QueryExecutor + 'static> Proxy<Q> {
    pub fn new(
        router: Arc<dyn Router + Send + Sync>,
        forwarder: Option<ForwarderRef>,
        instance: InstanceRef<Q>,
        schema_config_provider: SchemaConfigProviderRef,
    ) -> Self {
        Self {
            router,
            forwarder,
            instance,
            schema_config_provider,
        }
    }
}

#[derive(Default)]
pub struct Context {
    pub tenant: String,
    pub token: String,
    pub timeout: Option<Duration>,
}
