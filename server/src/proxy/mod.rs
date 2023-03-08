pub(crate) mod error;
mod grpc;

use std::{sync::Arc, time::Duration};

use query_engine::executor::Executor as QueryExecutor;
use router::Router;

use crate::{instance::InstanceRef, schema_config_provider::SchemaConfigProviderRef};

pub struct Proxy<Q: QueryExecutor + 'static> {
    router: Arc<dyn Router + Send + Sync>,
    instance: InstanceRef<Q>,
    schema_config_provider: SchemaConfigProviderRef,
}

impl<Q: QueryExecutor + 'static> Proxy<Q> {
    pub fn new(
        router: Arc<dyn Router + Send + Sync>,
        instance: InstanceRef<Q>,
        schema_config_provider: SchemaConfigProviderRef,
    ) -> Self {
        Self {
            router,
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
