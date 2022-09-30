// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

//! Setup server

use std::sync::Arc;

use analytic_engine::{
    self,
    setup::{EngineBuilder, ReplicatedEngineBuilder, RocksEngineBuilder},
};
use catalog_impls::{table_based::TableBasedManager, volatile, CatalogManagerImpl};
use cluster::cluster_impl::ClusterImpl;
use common_util::runtime;
use df_operator::registry::FunctionRegistryImpl;
use log::info;
use logger::RuntimeLevel;
use meta_client::meta_impl;
use query_engine::executor::{Executor, ExecutorImpl};
use server::{
    config::{Config, DeployMode, RuntimeConfig},
    route::{
        cluster_based::ClusterBasedRouter,
        rule_based::{ClusterView, RuleBasedRouter},
    },
    schema_config_provider::{
        cluster_based::ClusterBasedProvider, config_based::ConfigBasedProvider,
    },
    server::Builder,
    table_engine::{MemoryTableEngine, TableEngineProxy},
};
use table_engine::engine::{EngineRuntimes, TableEngineRef};
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
pub fn run_server(config: Config) {
    let runtimes = Arc::new(build_engine_runtimes(&config.runtime));
    let engine_runtimes = runtimes.clone();

    info!("Server starts up, config:{:#?}", config);

    runtimes.bg_runtime.block_on(async {
        if config.analytic.obkv_wal.enable {
            run_server_with_runtimes::<ReplicatedEngineBuilder>(config, engine_runtimes).await;
        } else {
            run_server_with_runtimes::<RocksEngineBuilder>(config, engine_runtimes).await;
        }
    });
}

async fn run_server_with_runtimes<T>(config: Config, runtimes: Arc<EngineRuntimes>)
where
    T: EngineBuilder,
{
    // Build all table engine
    // Create memory engine
    let memory = MemoryTableEngine;
    // Create analytic engine
    let analytic_config = config.analytic.clone();
    let analytic_engine_builder = T::default();
    let analytic = analytic_engine_builder
        .build(analytic_config, runtimes.clone())
        .await
        .expect("Failed to setup analytic engine");

    // Create table engine proxy
    let engine_proxy = Arc::new(TableEngineProxy {
        memory,
        analytic: analytic.clone(),
    });

    // Init function registry.
    let mut function_registry = FunctionRegistryImpl::new();
    function_registry
        .load_functions()
        .expect("Failed to create function registry");
    let function_registry = Arc::new(function_registry);

    // Create query executor
    let query_executor = ExecutorImpl::new(config.query.clone());

    let builder = Builder::new(config.clone())
        .runtimes(runtimes.clone())
        .query_executor(query_executor)
        .table_engine(engine_proxy.clone())
        .function_registry(function_registry);

    let builder = match config.deploy_mode {
        DeployMode::Standalone => {
            build_in_standalone_mode(&config, builder, analytic, engine_proxy).await
        }
        DeployMode::Cluster => {
            build_in_cluster_mode(&config, builder, &runtimes, engine_proxy).await
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

async fn build_in_cluster_mode<Q: Executor + 'static>(
    config: &Config,
    builder: Builder<Q>,
    runtimes: &EngineRuntimes,
    engine_proxy: TableEngineRef,
) -> Builder<Q> {
    let meta_client = meta_impl::build_meta_client(
        config.cluster.meta_client.clone(),
        config.cluster.node.clone(),
        runtimes.meta_runtime.clone(),
    )
    .expect("fail to build meta client");

    let catalog_manager = { Arc::new(volatile::ManagerImpl::new(meta_client.clone()).await) };

    let cluster = {
        let cluster_impl = ClusterImpl::new(
            meta_client,
            engine_proxy,
            config.cluster.clone(),
            runtimes.meta_runtime.clone(),
        )
        .unwrap();
        Arc::new(cluster_impl)
    };

    let router = Arc::new(ClusterBasedRouter::new(cluster.clone()));
    let schema_config_provider = Arc::new(ClusterBasedProvider::new(cluster.clone()));
    builder
        .catalog_manager(catalog_manager)
        .cluster(cluster)
        .router(router)
        .schema_config_provider(schema_config_provider)
}

async fn build_in_standalone_mode<Q: Executor + 'static>(
    config: &Config,
    builder: Builder<Q>,
    table_engine: TableEngineRef,
    engine_proxy: TableEngineRef,
) -> Builder<Q> {
    let table_based_manager = TableBasedManager::new(table_engine, engine_proxy.clone())
        .await
        .expect("Failed to create catalog manager");

    // Create catalog manager, use analytic table as backend
    let catalog_manager = Arc::new(CatalogManagerImpl::new(Arc::new(table_based_manager)));

    // Build static router and schema config provider
    let cluster_view = ClusterView::from(&config.static_route.topology);
    let schema_configs = cluster_view.schema_configs.clone();
    let router = Arc::new(RuleBasedRouter::new(
        cluster_view,
        config.static_route.rules.clone(),
    ));
    let schema_config_provider = Arc::new(ConfigBasedProvider::new(schema_configs));

    builder
        .catalog_manager(catalog_manager)
        .router(router)
        .schema_config_provider(schema_config_provider)
}
