// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

//! Setup server

use std::sync::Arc;

use analytic_engine::{
    self,
    setup::{
        EngineBuildContext, EngineBuildContextBuilder, EngineBuilder, KafkaWalEngineBuilder,
        ObkvWalEngineBuilder, RocksDBWalEngineBuilder,
    },
    WalStorageConfig,
};
use catalog::{manager::ManagerRef, schema::OpenOptions, CatalogRef};
use catalog_impls::{table_based::TableBasedManager, volatile, CatalogManagerImpl};
use cluster::{cluster_impl::ClusterImpl, shard_tables_cache::ShardTablesCache};
use common_util::runtime;
use df_operator::registry::FunctionRegistryImpl;
use interpreters::table_manipulator::{catalog_based, meta_based};
use log::info;
use logger::RuntimeLevel;
use meta_client::meta_impl;
use query_engine::executor::{Executor, ExecutorImpl};
use router::{rule_based::ClusterView, ClusterBasedRouter, RuleBasedRouter};
use server::{
    config::{Config, DeployMode, RuntimeConfig, StaticTopologyConfig},
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

use crate::signal_handler;

/// Setup log with given `config`, returns the runtime log level switch.
pub fn setup_log(config: &Config) -> RuntimeLevel {
    server::logger::init_log(config).expect("Failed to init log.")
}

/// Setup tracing with given `config`, returns the writer guard.
pub fn setup_tracing(config: &Config) -> WorkerGuard {
    tracing_util::init_tracing_with_file(
        &config.tracing_log_name,
        &config.tracing_log_dir,
        &config.tracing_level,
        Rotation::NEVER,
    )
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
        match config.analytic.wal_storage {
            WalStorageConfig::RocksDB => {
                run_server_with_runtimes::<RocksDBWalEngineBuilder>(
                    config,
                    engine_runtimes,
                    log_runtime,
                )
                .await
            }

            WalStorageConfig::Obkv(_) => {
                run_server_with_runtimes::<ObkvWalEngineBuilder>(
                    config,
                    engine_runtimes,
                    log_runtime,
                )
                .await;
            }

            WalStorageConfig::Kafka(_) => {
                run_server_with_runtimes::<KafkaWalEngineBuilder>(
                    config,
                    engine_runtimes,
                    log_runtime,
                )
                .await;
            }
        }
    });
}

async fn run_server_with_runtimes<T>(
    config: Config,
    runtimes: Arc<EngineRuntimes>,
    log_runtime: Arc<RuntimeLevel>,
) where
    T: EngineBuilder,
{
    // Init function registry.
    let mut function_registry = FunctionRegistryImpl::new();
    function_registry
        .load_functions()
        .expect("Failed to create function registry");
    let function_registry = Arc::new(function_registry);

    // Create query executor
    let query_executor = ExecutorImpl::new(config.query.clone());

    // Config limiter
    let limiter = Limiter::new(config.limiter.clone());

    let builder = Builder::new(config.clone())
        .engine_runtimes(runtimes.clone())
        .log_runtime(log_runtime.clone())
        .query_executor(query_executor)
        .function_registry(function_registry)
        .limiter(limiter);

    let engine_builder = T::default();
    let builder = match config.deploy_mode {
        DeployMode::Standalone => {
            build_in_standalone_mode(&config, builder, runtimes.clone(), engine_builder).await
        }
        DeployMode::Cluster => {
            build_in_cluster_mode(&config, builder, runtimes.clone(), engine_builder).await
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

async fn build_table_engine<T: EngineBuilder>(
    context: EngineBuildContext,
    runtimes: Arc<EngineRuntimes>,
    engine_builder: T,
) -> Arc<TableEngineProxy> {
    // Build all table engine
    // Create memory engine
    let memory = MemoryTableEngine;
    // Create analytic engine
    let analytic = engine_builder
        .build(context, runtimes.clone())
        .await
        .expect("Failed to setup analytic engine");

    // Create table engine proxy
    Arc::new(TableEngineProxy {
        memory,
        analytic: analytic.clone(),
    })
}

async fn build_in_cluster_mode<Q: Executor + 'static, T: EngineBuilder>(
    config: &Config,
    builder: Builder<Q>,
    runtimes: Arc<EngineRuntimes>,
    engine_builder: T,
) -> Builder<Q> {
    // Build meta related modules.
    let meta_client = meta_impl::build_meta_client(
        config.cluster.meta_client.clone(),
        config.cluster.node.clone(),
    )
    .await
    .expect("fail to build meta client");

    let shard_tables_cache = ShardTablesCache::default();
    let cluster = {
        let cluster_impl = ClusterImpl::new(
            shard_tables_cache.clone(),
            meta_client.clone(),
            config.cluster.clone(),
            runtimes.meta_runtime.clone(),
        )
        .unwrap();
        Arc::new(cluster_impl)
    };
    let router = Arc::new(ClusterBasedRouter::new(cluster.clone()));

    // Build table engine.
    let build_context_builder = EngineBuildContextBuilder::default();
    let build_context = build_context_builder
        .config(config.analytic.clone())
        .router(router.clone())
        .build();
    let engine_proxy = build_table_engine(build_context, runtimes.clone(), engine_builder).await;

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
        .router(router)
        .schema_config_provider(schema_config_provider)
}

async fn build_in_standalone_mode<Q: Executor + 'static, T: EngineBuilder>(
    config: &Config,
    builder: Builder<Q>,
    runtimes: Arc<EngineRuntimes>,
    engine_builder: T,
) -> Builder<Q> {
    // Build table engine.
    let build_context_builder = EngineBuildContextBuilder::default();
    let build_context = build_context_builder
        .config(config.analytic.clone())
        .build();
    let engine_proxy = build_table_engine(build_context, runtimes.clone(), engine_builder).await;

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
        config.static_route.topology.clone(),
    )
    .await;

    // Build static router and schema config provider
    let cluster_view = ClusterView::from(&config.static_route.topology);
    let schema_configs = cluster_view.schema_configs.clone();
    let router = Arc::new(RuleBasedRouter::new(
        cluster_view,
        config.static_route.rules.clone(),
    ));
    let schema_config_provider = Arc::new(ConfigBasedProvider::new(schema_configs));

    builder
        .table_engine(engine_proxy)
        .catalog_manager(catalog_manager)
        .table_manipulator(table_manipulator)
        .router(router)
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
