// Copyright 2023 The CeresDB Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Server

use std::sync::Arc;

use catalog::manager::ManagerRef;
use cluster::ClusterRef;
use datafusion::execution::{runtime_env::RuntimeConfig, FunctionRegistry};
use df_operator::registry::FunctionRegistryRef;
use interpreters::table_manipulator::TableManipulatorRef;
use logger::{info, warn, RuntimeLevel};
use macros::define_result;
use partition_table_engine::PartitionTableEngine;
use proxy::{
    dedup_requests::RequestNotifiers,
    hotspot::HotspotRecorder,
    instance::{DynamicConfig, Instance, InstanceRef},
    limiter::Limiter,
    schema_config_provider::SchemaConfigProviderRef,
    Proxy,
};
use query_engine::{QueryEngineBuilder, QueryEngineType};
use remote_engine_client::RemoteEngineImpl;
use router::{endpoint::Endpoint, RouterRef};
use snafu::{Backtrace, OptionExt, ResultExt, Snafu};
use table_engine::{
    engine::{EngineRuntimes, TableEngineRef},
    remote::RemoteEngineRef,
};
use wal::manager::OpenedWals;

use crate::{
    config::ServerConfig,
    grpc::{self, RpcServices},
    http::{self, HttpConfig, Service},
    local_tables::{self, LocalTablesRecoverer},
    mysql,
    mysql::error::Error as MysqlError,
    postgresql,
    postgresql::error::Error as PostgresqlError,
};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Missing engine runtimes.\nBacktrace:\n{}", backtrace))]
    MissingEngineRuntimes { backtrace: Backtrace },

    #[snafu(display("Missing log runtime.\nBacktrace:\n{}", backtrace))]
    MissingLogRuntime { backtrace: Backtrace },

    #[snafu(display("Missing router.\nBacktrace:\n{}", backtrace))]
    MissingRouter { backtrace: Backtrace },

    #[snafu(display("Missing schema config provider.\nBacktrace:\n{}", backtrace))]
    MissingSchemaConfigProvider { backtrace: Backtrace },

    #[snafu(display("Missing catalog manager.\nBacktrace:\n{}", backtrace))]
    MissingCatalogManager { backtrace: Backtrace },

    #[snafu(display("Missing query engine.\nBacktrace:\n{}", backtrace))]
    MissingQueryEngine { backtrace: Backtrace },

    #[snafu(display("Missing table engine.\nBacktrace:\n{}", backtrace))]
    MissingTableEngine { backtrace: Backtrace },

    #[snafu(display("Missing table manipulator.\nBacktrace:\n{}", backtrace))]
    MissingTableManipulator { backtrace: Backtrace },

    #[snafu(display("Missing function registry.\nBacktrace:\n{}", backtrace))]
    MissingFunctionRegistry { backtrace: Backtrace },

    #[snafu(display("Missing wals.\nBacktrace:\n{}", backtrace))]
    MissingWals { backtrace: Backtrace },

    #[snafu(display("Missing limiter.\nBacktrace:\n{}", backtrace))]
    MissingLimiter { backtrace: Backtrace },

    #[snafu(display("Missing datafusion context.\nBacktrace:\n{}", backtrace))]
    MissingDatafusionContext { backtrace: Backtrace },

    #[snafu(display("Missing query engine config.\nBacktrace:\n{}", backtrace))]
    MissingQueryEngineConfig { backtrace: Backtrace },

    #[snafu(display("Missing config content.\nBacktrace:\n{}", backtrace))]
    MissingConfigContent { backtrace: Backtrace },

    #[snafu(display("Http service failed, msg:{}, err:{}", msg, source))]
    HttpService {
        msg: String,
        source: crate::http::Error,
    },

    #[snafu(display("Failed to build mysql service, err:{}", source))]
    BuildMysqlService { source: MysqlError },

    #[snafu(display("Failed to build postgresql service, err:{}", source))]
    BuildPostgresqlService { source: PostgresqlError },

    #[snafu(display("Failed to start mysql service, err:{}", source))]
    StartMysqlService { source: MysqlError },

    #[snafu(display("Failed to start postgresql service, err:{}", source))]
    StartPostgresqlService { source: PostgresqlError },

    #[snafu(display("Failed to register system catalog, err:{}", source))]
    RegisterSystemCatalog { source: catalog::manager::Error },

    #[snafu(display("Failed to build grpc service, err:{}", source))]
    BuildGrpcService { source: crate::grpc::Error },

    #[snafu(display("Failed to start grpc service, err:{}", source))]
    StartGrpcService { source: crate::grpc::Error },

    #[snafu(display("Failed to start cluster, err:{}", source))]
    StartCluster { source: cluster::Error },

    #[snafu(display("Failed to open tables in standalone mode, err:{}", source))]
    OpenLocalTables { source: local_tables::Error },

    #[snafu(display("Failed to build query engine, err:{source}"))]
    BuildQueryEngine { source: query_engine::error::Error },
}

define_result!(Error);

// TODO(yingwen): Consider a config manager
/// Server
pub struct Server {
    http_service: Service,
    rpc_services: RpcServices,
    mysql_service: mysql::MysqlService,
    postgresql_service: postgresql::PostgresqlService,
    instance: InstanceRef,
    cluster: Option<ClusterRef>,
    local_tables_recoverer: Option<LocalTablesRecoverer>,
}

impl Server {
    pub async fn stop(mut self) {
        self.rpc_services.shutdown().await;
        self.http_service.stop();
        self.mysql_service.shutdown();
        self.postgresql_service.shutdown();

        if let Some(cluster) = &self.cluster {
            cluster.stop().await.expect("fail to stop cluster");
        }
    }

    pub async fn start(&mut self) -> Result<()> {
        // Run in standalone mode
        if let Some(local_tables_recoverer) = &self.local_tables_recoverer {
            info!("Server start, open local tables");
            local_tables_recoverer
                .recover()
                .await
                .context(OpenLocalTables)?;
        }

        // Run in cluster mode
        if let Some(cluster) = &self.cluster {
            info!("Server start, start cluster");
            cluster.start().await.context(StartCluster)?;
        }

        // TODO: Is it necessary to create default schema in cluster mode?
        info!("Server start, create default schema if not exist");
        self.create_default_schema_if_not_exists().await;

        info!("Server start, start services");

        self.http_service.start().await.context(HttpService {
            msg: "start failed",
        })?;

        self.mysql_service
            .start()
            .await
            .context(StartMysqlService)?;

        self.postgresql_service
            .start()
            .await
            .context(StartPostgresqlService)?;

        self.rpc_services.start().await.context(StartGrpcService)?;

        info!("Server start finished");

        Ok(())
    }

    async fn create_default_schema_if_not_exists(&self) {
        let catalog_mgr = &self.instance.catalog_manager;
        let default_catalog = catalog_mgr
            .catalog_by_name(catalog_mgr.default_catalog_name())
            .expect("Fail to retrieve default catalog")
            .expect("Default catalog doesn't exist");

        if default_catalog
            .schema_by_name(catalog_mgr.default_schema_name())
            .expect("Fail to retrieve default schema")
            .is_none()
        {
            warn!("Default schema doesn't exist and create it");
            default_catalog
                .create_schema(catalog_mgr.default_schema_name())
                .await
                .expect("Fail to create default schema");
        }
    }
}

#[must_use]
pub struct Builder {
    server_config: ServerConfig,
    node_addr: String,
    query_engine_config: Option<query_engine::config::Config>,
    config_content: Option<String>,
    engine_runtimes: Option<Arc<EngineRuntimes>>,
    log_runtime: Option<Arc<RuntimeLevel>>,
    catalog_manager: Option<ManagerRef>,
    table_engine: Option<TableEngineRef>,
    table_manipulator: Option<TableManipulatorRef>,
    function_registry: Option<FunctionRegistryRef>,
    limiter: Limiter,
    cluster: Option<ClusterRef>,
    router: Option<RouterRef>,
    schema_config_provider: Option<SchemaConfigProviderRef>,
    local_tables_recoverer: Option<LocalTablesRecoverer>,
    opened_wals: Option<OpenedWals>,
    remote_engine: Option<RemoteEngineRef>,
    datatfusion_context: Option<DatafusionContext>,
}

impl Builder {
    pub fn new(config: ServerConfig) -> Self {
        Self {
            server_config: config,
            node_addr: "".to_string(),
            query_engine_config: None,
            config_content: None,
            engine_runtimes: None,
            log_runtime: None,
            catalog_manager: None,
            table_engine: None,
            table_manipulator: None,
            function_registry: None,
            limiter: Limiter::default(),
            cluster: None,
            router: None,
            schema_config_provider: None,
            local_tables_recoverer: None,
            opened_wals: None,
            remote_engine: None,
            datatfusion_context: None,
        }
    }

    pub fn node_addr(mut self, node_addr: String) -> Self {
        self.node_addr = node_addr;
        self
    }

    pub fn config_content(mut self, config_content: String) -> Self {
        self.config_content = Some(config_content);
        self
    }

    pub fn query_engine_config(
        mut self,
        query_engine_config: query_engine::config::Config,
    ) -> Self {
        self.query_engine_config = Some(query_engine_config);
        self
    }

    pub fn engine_runtimes(mut self, engine_runtimes: Arc<EngineRuntimes>) -> Self {
        self.engine_runtimes = Some(engine_runtimes);
        self
    }

    pub fn log_runtime(mut self, log_runtime: Arc<RuntimeLevel>) -> Self {
        self.log_runtime = Some(log_runtime);
        self
    }

    pub fn catalog_manager(mut self, val: ManagerRef) -> Self {
        self.catalog_manager = Some(val);
        self
    }

    pub fn table_engine(mut self, val: TableEngineRef) -> Self {
        self.table_engine = Some(val);
        self
    }

    pub fn table_manipulator(mut self, val: TableManipulatorRef) -> Self {
        self.table_manipulator = Some(val);
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

    pub fn cluster(mut self, cluster: ClusterRef) -> Self {
        self.cluster = Some(cluster);
        self
    }

    pub fn router(mut self, router: RouterRef) -> Self {
        self.router = Some(router);
        self
    }

    pub fn schema_config_provider(
        mut self,
        schema_config_provider: SchemaConfigProviderRef,
    ) -> Self {
        self.schema_config_provider = Some(schema_config_provider);
        self
    }

    pub fn local_tables_recoverer(mut self, local_tables_recoverer: LocalTablesRecoverer) -> Self {
        self.local_tables_recoverer = Some(local_tables_recoverer);
        self
    }

    pub fn opened_wals(mut self, opened_wals: OpenedWals) -> Self {
        self.opened_wals = Some(opened_wals);
        self
    }

    pub fn remote_engine(mut self, remote_engine: RemoteEngineRef) -> Self {
        self.remote_engine = Some(remote_engine);
        self
    }

    pub fn datafusion_context(mut self, datafusion_context: DatafusionContext) -> Self {
        self.datatfusion_context = Some(datafusion_context);
        self
    }

    /// Build and run the server
    pub fn build(self) -> Result<Server> {
        // Build instance
        let catalog_manager = self.catalog_manager.context(MissingCatalogManager)?;
        let table_engine = self.table_engine.context(MissingTableEngine)?;
        let table_manipulator = self.table_manipulator.context(MissingTableManipulator)?;
        let function_registry = self.function_registry.context(MissingFunctionRegistry)?;
        let opened_wals = self.opened_wals.context(MissingWals)?;
        let router = self.router.context(MissingRouter)?;
        let provider = self
            .schema_config_provider
            .context(MissingSchemaConfigProvider)?;
        let log_runtime = self.log_runtime.context(MissingLogRuntime)?;
        let engine_runtimes = self.engine_runtimes.context(MissingEngineRuntimes)?;
        let config_content = self.config_content.context(MissingConfigContent)?;
        let query_engine_config = self.query_engine_config.context(MissingQueryEngineConfig)?;
        let datafusion_context = self.datatfusion_context.context(MissingDatafusionContext)?;

        let hotspot_recorder = Arc::new(HotspotRecorder::new(
            self.server_config.hotspot,
            engine_runtimes.default_runtime.clone(),
        ));

        // Build remote engine.
        let remote_engine_ref = Arc::new(RemoteEngineImpl::new(
            self.server_config.remote_client.clone(),
            router.clone(),
            engine_runtimes.io_runtime.clone(),
        ));

        // Build partitioned table engine.
        // TODO: remove the partitioned table engine.
        let partition_table_engine = Arc::new(PartitionTableEngine::new(remote_engine_ref.clone()));

        // Build query engine.
        let query_engine_builder = QueryEngineBuilder::default()
            .config(query_engine_config)
            .catalog_manager(catalog_manager.clone())
            .df_function_registry(datafusion_context.function_registry)
            .df_runtime_config(datafusion_context.runtime_config)
            .remote_engine(remote_engine_ref.clone());
        let query_engine = query_engine_builder
            .build(QueryEngineType::Datafusion)
            .context(BuildQueryEngine)?;

        // TODO: build dynamic config from server config.
        let proxy_dyn_config = DynamicConfig::default();
        let instance = {
            let instance = Instance {
                catalog_manager,
                query_engine,
                table_engine,
                partition_table_engine,
                function_registry,
                limiter: self.limiter,
                table_manipulator,
                remote_engine_ref,
                dyn_config: proxy_dyn_config,
            };
            InstanceRef::new(instance)
        };

        let grpc_endpoint = Endpoint {
            addr: self.server_config.bind_addr.clone(),
            port: self.server_config.grpc_port,
        };

        // Create http config
        let http_endpoint = Endpoint {
            addr: self.server_config.bind_addr.clone(),
            port: self.server_config.http_port,
        };

        let http_config = HttpConfig {
            endpoint: http_endpoint,
            max_body_size: self.server_config.http_max_body_size.as_byte(),
            timeout: self.server_config.timeout.map(|v| v.0),
        };

        let request_notifiers = if self.server_config.query_dedup.enable {
            Some(Arc::new(RequestNotifiers::default()))
        } else {
            None
        };

        let proxy = Arc::new(Proxy::new(
            router.clone(),
            instance.clone(),
            self.server_config.forward,
            Endpoint::new(self.node_addr, self.server_config.grpc_port),
            self.server_config.resp_compress_min_length.as_byte() as usize,
            self.server_config.auto_create_table,
            provider.clone(),
            hotspot_recorder.clone(),
            engine_runtimes.clone(),
            self.cluster.is_some(),
            self.server_config.sub_table_access_perm,
            request_notifiers,
        ));

        let http_service = http::Builder::new(http_config)
            .engine_runtimes(engine_runtimes.clone())
            .log_runtime(log_runtime)
            .config_content(config_content)
            .cluster(self.cluster.clone())
            .proxy(proxy.clone())
            .opened_wals(opened_wals.clone())
            .build()
            .context(HttpService {
                msg: "build failed",
            })?;

        let mysql_config = mysql::MysqlConfig {
            ip: self.server_config.bind_addr.clone(),
            port: self.server_config.mysql_port,
            timeout: self.server_config.timeout.map(|v| v.0),
        };

        let mysql_service = mysql::Builder::new(mysql_config)
            .runtimes(engine_runtimes.clone())
            .proxy(proxy.clone())
            .build()
            .context(BuildMysqlService)?;

        let postgresql_service = postgresql::Builder::new()
            .ip(self.server_config.bind_addr.clone())
            .port(self.server_config.postgresql_port)
            .proxy(proxy.clone())
            .runtimes(engine_runtimes.clone())
            .build()
            .context(BuildPostgresqlService)?;

        let rpc_services = grpc::Builder::new()
            .endpoint(grpc_endpoint.to_string())
            .runtimes(engine_runtimes)
            .instance(instance.clone())
            .cluster(self.cluster.clone())
            .opened_wals(opened_wals)
            .timeout(self.server_config.timeout.map(|v| v.0))
            .proxy(proxy)
            .hotspot_recorder(hotspot_recorder)
            .query_dedup(self.server_config.query_dedup)
            .build()
            .context(BuildGrpcService)?;

        let server = Server {
            http_service,
            rpc_services,
            mysql_service,
            postgresql_service,
            instance,
            cluster: self.cluster,
            local_tables_recoverer: self.local_tables_recoverer,
        };
        Ok(server)
    }
}

pub struct DatafusionContext {
    pub function_registry: Arc<dyn FunctionRegistry + Send + Sync>,
    pub runtime_config: RuntimeConfig,
}
