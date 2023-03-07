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
};

pub struct Proxy<Q: QueryExecutor + 'static> {
    pub router: Arc<dyn Router + Send + Sync>,
    pub forwarder: Option<ForwarderRef>,
    instance: InstanceRef<Q>,
}

impl<Q: QueryExecutor + 'static> Proxy<Q> {
    pub fn new(
        router: Arc<dyn Router + Send + Sync>,
        forwarder: Option<ForwarderRef>,
        instance: InstanceRef<Q>,
    ) -> Self {
        Self {
            router,
            forwarder,
            instance,
        }
    }
}

#[derive(Default)]
pub struct Context {
    pub tenant: String,
    pub token: String,
    // pub runtime: Runtime,
    pub timeout: Option<Duration>,
}
