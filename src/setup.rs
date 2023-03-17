// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

//! Setup server

use std::sync::Arc;

use analytic_engine::{
    self,
    setup::{EngineBuilder, KafkaWalsOpener, ObkvWalsOpener, RocksDBWalsOpener, WalsOpener},
    WalStorageConfig,
};
use catalog::{manager::ManagerRef, schema::OpenOptions, CatalogRef};
use catalog_impls::{table_based::TableBasedManager, volatile, CatalogManagerImpl};
use cluster::{
    cluster_impl::ClusterImpl, config::ClusterConfig, shard_tables_cache::ShardTablesCache,
};
use common_util::runtime;
use df_operator::registry::FunctionRegistryImpl;
use interpreters::table_manipulator::{catalog_based, meta_based};
use log::info;
use logger::RuntimeLevel;
use meta_client::{meta_impl, types::NodeMetaInfo};
use query_engine::executor::{Executor, ExecutorImpl};
use router::{rule_based::ClusterView, ClusterBasedRouter, RuleBasedRouter};
use server::{
    config::{StaticRouteConfig, StaticTopologyConfig},
    limiter::Limiter,
    local_tables::LocalTablesRecoverer,
    schema_config_provider::{
        cluster_based::ClusterBasedProvider, config_based::ConfigBasedProvider,
    },
    server::Builder,
    table_engine::{MemoryTableEngine, TableEngineProxy},
};
use table_engine::engine::EngineRuntimes;
use tracing_util::{
    self,
    tracing_appender::{non_blocking::WorkerGuard, rolling::Rotation},
};

use crate::{
    config::{ClusterDeployment, Config, RuntimeConfig},
    signal_handler,
};

/// Setup log with given `config`, returns the runtime log level switch.
pub fn setup_logger(config: &Config) -> RuntimeLevel {
    logger::init_log(&config.logger).expect("Failed to init log.")
}

/// Setup tracing with given `config`, returns the writer guard.
pub fn setup_tracing(config: &Config) -> WorkerGuard {
    tracing_util::init_tracing_with_file(&config.tracing, Rotation::NEVER)
}

fn build_runtime(name: &str, threads_num: usize) -> runtime::Runtime {
    runtime::Builder::default()
        .worker_threads(threads_num)
        .thread_name(name)
        .enable_all()
        .build()
        .expect("Failed to create runtime")
}

fn build_engine_runtimes(config: &RuntimeConfig) -> EngineRuntimes {
    EngineRuntimes {
        read_runtime: Arc::new(build_runtime("ceres-read", config.read_thread_num)),
        write_runtime: Arc::new(build_runtime("ceres-write", config.write_thread_num)),
        meta_runtime: Arc::new(build_runtime("ceres-meta", config.meta_thread_num)),
        bg_runtime: Arc::new(build_runtime("ceres-bg", config.background_thread_num)),
    }
}

/// Run a server, returns when the server is shutdown by user
pub fn run_server(config: Config, log_runtime: RuntimeLevel) {
    let runtimes = Arc::new(build_engine_runtimes(&config.runtime));
    let engine_runtimes = runtimes.clone();
    let log_runtime = Arc::new(log_runtime);

    info!("Server starts up, config:{:#?}", config);

    runtimes.bg_runtime.block_on(async {
        match config.analytic.wal {
            WalStorageConfig::RocksDB(_) => {
                run_server_with_runtimes::<RocksDBWalsOpener>(config, engine_runtimes, log_runtime)
                    .await
            }

            WalStorageConfig::Obkv(_) => {
                run_server_with_runtimes::<ObkvWalsOpener>(config, engine_runtimes, log_runtime)
                    .await;
            }

            WalStorageConfig::Kafka(_) => {
                run_server_with_runtimes::<KafkaWalsOpener>(config, engine_runtimes, log_runtime)
                    .await;
            }
        }
    });
}

async fn run_server_with_runtimes<T>(
    config: Config,
    engine_runtimes: Arc<EngineRuntimes>,
    log_runtime: Arc<RuntimeLevel>,
) where
    T: WalsOpener,
{
    // Init function registry.
    let mut function_registry = FunctionRegistryImpl::new();
    function_registry
        .load_functions()
        .expect("Failed to create function registry");
    let function_registry = Arc::new(function_registry);

    // Create query executor
    let query_executor = ExecutorImpl::new(config.query_engine.clone());

    // Config limiter
    let limiter = Limiter::new(config.limiter.clone());
    let config_content = toml::to_string(&config).expect("Fail to serialize config");

    let builder = Builder::new(config.server.clone())
        .node_addr(config.node.addr.clone())
        .config_content(config_content)
        .engine_runtimes(engine_runtimes.clone())
        .log_runtime(log_runtime.clone())
        .query_executor(query_executor)
        .function_registry(function_registry)
        .limiter(limiter);

    let engine_builder = T::default();
    let builder = match &config.cluster_deployment {
        None => {
            build_without_meta(
                &config,
                &StaticRouteConfig::default(),
                builder,
                engine_runtimes.clone(),
                engine_builder,
            )
            .await
        }
        Some(ClusterDeployment::NoMeta(v)) => {
            build_without_meta(&config, v, builder, engine_runtimes.clone(), engine_builder).await
        }
        Some(ClusterDeployment::WithMeta(cluster_config)) => {
            build_with_meta(
                &config,
                cluster_config,
                builder,
                engine_runtimes.clone(),
                engine_builder,
            )
            .await
        }
    };

    // Build and start server
    let mut server = builder.build().expect("Failed to create server");
    server.start().await.expect("Failed to start server");

    // Wait for signal
    signal_handler::wait_for_signal();

    // Stop server
    server.stop().await;
}

// Build proxy for all table engines.
async fn build_table_engine_proxy(engine_builder: EngineBuilder<'_>) -> Arc<TableEngineProxy> {
    // Create memory engine
    let memory = MemoryTableEngine;
    // Create analytic engine
    let analytic = engine_builder
        .build()
        .await
        .expect("Failed to setup analytic engine");

    // Create table engine proxy
    Arc::new(TableEngineProxy {
        memory,
        analytic: analytic.clone(),
    })
}

async fn build_with_meta<Q: Executor + 'static, T: WalsOpener>(
    config: &Config,
    cluster_config: &ClusterConfig,
    builder: Builder<Q>,
    runtimes: Arc<EngineRuntimes>,
    wal_opener: T,
) -> Builder<Q> {
    // Build meta related modules.
    let node_meta_info = NodeMetaInfo {
        addr: config.node.addr.clone(),
        port: config.server.grpc_port,
        zone: config.node.zone.clone(),
        idc: config.node.idc.clone(),
        binary_version: config.node.binary_version.clone(),
    };
    let meta_client =
        meta_impl::build_meta_client(cluster_config.meta_client.clone(), node_meta_info)
            .await
            .expect("fail to build meta client");

    let shard_tables_cache = ShardTablesCache::default();
    let cluster = {
        let cluster_impl = ClusterImpl::new(
            shard_tables_cache.clone(),
            meta_client.clone(),
            cluster_config.clone(),
            runtimes.meta_runtime.clone(),
        )
        .unwrap();
        Arc::new(cluster_impl)
    };
    let router = Arc::new(ClusterBasedRouter::new(
        cluster.clone(),
        config.server.route_cache.clone(),
    ));

    let opened_wals = wal_opener
        .open_wals(&config.analytic.wal, runtimes.clone())
        .await
        .expect("Failed to setup analytic engine");
    let engine_builder = EngineBuilder {
        config: &config.analytic,
        router: Some(router.clone()),
        engine_runtimes: runtimes.clone(),
        opened_wals: opened_wals.clone(),
    };
    let engine_proxy = build_table_engine_proxy(engine_builder).await;

    let meta_based_manager_ref = Arc::new(volatile::ManagerImpl::new(
        shard_tables_cache,
        meta_client.clone(),
    ));

    // Build catalog manager.
    let catalog_manager = Arc::new(CatalogManagerImpl::new(meta_based_manager_ref));

    let table_manipulator = Arc::new(meta_based::TableManipulatorImpl::new(meta_client));

    let schema_config_provider = Arc::new(ClusterBasedProvider::new(cluster.clone()));
    builder
        .table_engine(engine_proxy)
        .catalog_manager(catalog_manager)
        .table_manipulator(table_manipulator)
        .cluster(cluster)
        .opened_wals(opened_wals)
        .router(router)
        .schema_config_provider(schema_config_provider)
}

async fn build_without_meta<Q: Executor + 'static, T: WalsOpener>(
    config: &Config,
    static_route_config: &StaticRouteConfig,
    builder: Builder<Q>,
    runtimes: Arc<EngineRuntimes>,
    wal_builder: T,
) -> Builder<Q> {
    let opened_wals = wal_builder
        .open_wals(&config.analytic.wal, runtimes.clone())
        .await
        .expect("Failed to setup analytic engine");
    let engine_builder = EngineBuilder {
        config: &config.analytic,
        router: None,
        engine_runtimes: runtimes.clone(),
        opened_wals: opened_wals.clone(),
    };
    let engine_proxy = build_table_engine_proxy(engine_builder).await;

    // Create catalog manager, use analytic engine as backend.
    let analytic = engine_proxy.analytic.clone();
    let mut table_based_manager = TableBasedManager::new(analytic)
        .await
        .expect("Failed to create catalog manager");

    // Get collected table infos.
    let table_infos = table_based_manager
        .fetch_table_infos()
        .await
        .expect("Failed to fetch table infos for opening");

    let catalog_manager = Arc::new(CatalogManagerImpl::new(Arc::new(table_based_manager)));
    let table_manipulator = Arc::new(catalog_based::TableManipulatorImpl::new(
        catalog_manager.clone(),
    ));

    // Iterate the table infos to recover.
    let default_catalog = default_catalog(catalog_manager.clone());
    let open_opts = OpenOptions {
        table_engine: engine_proxy.clone(),
    };

    // Create local tables recoverer.
    let local_tables_recoverer = LocalTablesRecoverer::new(table_infos, default_catalog, open_opts);

    // Create schema in default catalog.
    create_static_topology_schema(
        catalog_manager.clone(),
        static_route_config.topology.clone(),
    )
    .await;

    // Build static router and schema config provider
    let cluster_view = ClusterView::from(&static_route_config.topology);
    let schema_configs = cluster_view.schema_configs.clone();
    let router = Arc::new(RuleBasedRouter::new(
        cluster_view,
        static_route_config.rules.clone(),
    ));
    let schema_config_provider = Arc::new(ConfigBasedProvider::new(schema_configs));

    builder
        .table_engine(engine_proxy)
        .catalog_manager(catalog_manager)
        .table_manipulator(table_manipulator)
        .router(router)
        .opened_wals(opened_wals)
        .schema_config_provider(schema_config_provider)
        .local_tables_recoverer(local_tables_recoverer)
}

async fn create_static_topology_schema(
    catalog_mgr: ManagerRef,
    static_topology_config: StaticTopologyConfig,
) {
    let default_catalog = catalog_mgr
        .catalog_by_name(catalog_mgr.default_catalog_name())
        .expect("Fail to retrieve default catalog")
        .expect("Default catalog doesn't exist");
    for schema_shard_view in static_topology_config.schema_shards {
        default_catalog
            .create_schema(&schema_shard_view.schema)
            .await
            .unwrap_or_else(|_| panic!("Fail to create schema:{}", schema_shard_view.schema));
        info!(
            "Create static topology in default catalog:{}, schema:{}",
            catalog_mgr.default_catalog_name(),
            &schema_shard_view.schema
        );
    }
}

fn default_catalog(catalog_manager: ManagerRef) -> CatalogRef {
    let default_catalog_name = catalog_manager.default_catalog_name();
    catalog_manager
        .catalog_by_name(default_catalog_name)
        .expect("fail to get default catalog")
        .expect("default catalog is not found")
}
