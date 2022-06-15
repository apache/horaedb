// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

//! Server

use std::sync::Arc;

use catalog::manager::Manager as CatalogManager;
use df_operator::registry::FunctionRegistryRef;
use grpcio::Environment;
use query_engine::executor::Executor as QueryExecutor;
use snafu::{Backtrace, OptionExt, ResultExt, Snafu};
use table_engine::engine::{EngineRuntimes, TableEngineRef};

use crate::{
    config::Config,
    grpc::{self, RpcServices},
    http::{self, Service},
    instance::{Instance, InstanceRef},
    limiter::Limiter,
};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Missing runtimes.\nBacktrace:\n{}", backtrace))]
    MissingRuntimes { backtrace: Backtrace },

    #[snafu(display("Missing catalog manager.\nBacktrace:\n{}", backtrace))]
    MissingCatalogManager { backtrace: Backtrace },

    #[snafu(display("Missing query executor.\nBacktrace:\n{}", backtrace))]
    MissingQueryExecutor { backtrace: Backtrace },

    #[snafu(display("Missing table engine.\nBacktrace:\n{}", backtrace))]
    MissingTableEngine { backtrace: Backtrace },

    #[snafu(display("Missing function registry.\nBacktrace:\n{}", backtrace))]
    MissingFunctionRegistry { backtrace: Backtrace },

    #[snafu(display("Missing limiter.\nBacktrace:\n{}", backtrace))]
    MissingLimiter { backtrace: Backtrace },

    #[snafu(display("Failed to start http service, err:{}", source))]
    StartHttpService { source: crate::http::Error },

    #[snafu(display("Failed to register system catalog, err:{}", source))]
    RegisterSystemCatalog { source: catalog::manager::Error },

    #[snafu(display("Failed to build grpc service, err:{}", source))]
    BuildGrpcService { source: crate::grpc::Error },

    #[snafu(display("Failed to start grpc service, err:{}", source))]
    StartGrpcService { source: crate::grpc::Error },
}

define_result!(Error);

// TODO(yingwen): Consider a config manager
/// Server
pub struct Server<C, Q> {
    http_service: Service<C, Q>,
    rpc_services: RpcServices,
}

impl<C, Q> Server<C, Q> {
    pub fn stop(mut self) {
        self.rpc_services.shutdown();
        self.http_service.stop();
    }

    pub async fn start(&mut self) -> Result<()> {
        self.rpc_services.start().await.context(StartGrpcService)
    }
}

#[must_use]
pub struct Builder<C, Q> {
    config: Config,
    runtimes: Option<Arc<EngineRuntimes>>,
    catalog_manager: Option<C>,
    query_executor: Option<Q>,
    table_engine: Option<TableEngineRef>,
    function_registry: Option<FunctionRegistryRef>,
    limiter: Limiter,
}

impl<C: CatalogManager + 'static, Q: QueryExecutor + 'static> Builder<C, Q> {
    pub fn new(config: Config) -> Self {
        Self {
            config,
            runtimes: None,
            catalog_manager: None,
            query_executor: None,
            table_engine: None,
            function_registry: None,
            limiter: Limiter::default(),
        }
    }

    pub fn runtimes(mut self, runtimes: Arc<EngineRuntimes>) -> Self {
        self.runtimes = Some(runtimes);
        self
    }

    pub fn catalog_manager(mut self, val: C) -> Self {
        self.catalog_manager = Some(val);
        self
    }

    pub fn query_executor(mut self, val: Q) -> Self {
        self.query_executor = Some(val);
        self
    }

    pub fn table_engine(mut self, val: TableEngineRef) -> Self {
        self.table_engine = Some(val);
        self
    }

    pub fn function_registry(mut self, val: FunctionRegistryRef) -> Self {
        self.function_registry = Some(val);
        self
    }

    pub fn limiter(mut self, val: Limiter) -> Self {
        self.limiter = val;
        self
    }

    /// Build and run the server
    pub fn build(self) -> Result<Server<C, Q>> {
        // Build runtimes
        let runtimes = self.runtimes.context(MissingRuntimes)?;

        // Build instance
        let catalog_manager = self.catalog_manager.context(MissingCatalogManager)?;
        let query_executor = self.query_executor.context(MissingQueryExecutor)?;
        let table_engine = self.table_engine.context(MissingTableEngine)?;
        let function_registry = self.function_registry.context(MissingFunctionRegistry)?;
        let instance = Instance {
            catalog_manager,
            query_executor,
            table_engine,
            function_registry,
            limiter: self.limiter,
        };
        let instance = InstanceRef::new(instance);

        // Create http config
        let http_config = http::Config {
            ip: self.config.bind_addr.clone(),
            port: self.config.http_port,
        };

        // Start http service
        let http_service = http::Builder::new(http_config)
            .runtimes(runtimes.clone())
            .instance(instance.clone())
            .build()
            .context(StartHttpService)?;

        let meta_client_config = self.config.meta_client;
        let env = Arc::new(Environment::new(self.config.grpc_server_cq_count));
        let rpc_services = grpc::Builder::new()
            .bind_addr(self.config.bind_addr)
            .port(self.config.grpc_port)
            .meta_client_config(meta_client_config)
            .env(env)
            .runtimes(runtimes)
            .instance(instance)
            .route_rules(self.config.route_rules)
            .build()
            .context(BuildGrpcService)?;

        let server = Server {
            http_service,
            rpc_services,
        };
        Ok(server)
    }
}
