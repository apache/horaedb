// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

//! Setup server

use std::sync::Arc;

use analytic_engine::{self, setup};
use catalog_impls::{table_based::TableBasedManager, CatalogManagerImpl};
use common_util::runtime;
use df_operator::registry::FunctionRegistryImpl;
use log::info;
use logger::RuntimeLevel;
use query_engine::executor::ExecutorImpl;
use server::{
    config::{Config, RuntimeConfig},
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
        .unwrap_or_else(|e| {
            //TODO(yingwen) replace panic with fatal
            panic!("Failed to create runtime, err:{}", e);
        })
}

fn build_engine_runtimes(config: &RuntimeConfig) -> EngineRuntimes {
    EngineRuntimes {
        read_runtime: Arc::new(build_runtime("cse-read", config.read_thread_num)),
        write_runtime: Arc::new(build_runtime("cse-write", config.write_thread_num)),
        bg_runtime: Arc::new(build_runtime("cse-bg", config.background_thread_num)),
    }
}

/// Run a server, returns when the server is shutdown by user
pub fn run_server(config: Config) {
    let runtimes = Arc::new(build_engine_runtimes(&config.runtime));
    let engine_runtimes = runtimes.clone();

    info!("Server starts up, config:{:#?}", config);

    runtimes.bg_runtime.block_on(async {
        // Build all table engine
        // Create memory engine
        let memory = MemoryTableEngine;
        // Create analytic engine
        let analytic_config = config.analytic.clone();
        let analytic = setup::open_analytic_table_engine(analytic_config, engine_runtimes)
            .await
            .unwrap_or_else(|e| {
                panic!("Failed to setup analytic engine, err:{}", e);
            });

        // Create table engine proxy
        let engine_proxy = Arc::new(TableEngineProxy {
            memory,
            analytic: analytic.clone(),
        });

        // Create catalog manager, use analytic table as backend
        let catalog_manager = CatalogManagerImpl::new(
            TableBasedManager::new(analytic.clone(), engine_proxy.clone())
                .await
                .unwrap_or_else(|e| {
                    panic!("Failed to create catalog manager, err:{}", e);
                }),
        );

        // Init function registry.
        let mut function_registry = FunctionRegistryImpl::new();
        function_registry.load_functions().unwrap_or_else(|e| {
            panic!("Failed to create function registry, err:{}", e);
        });
        let function_registry = Arc::new(function_registry);

        // Create query executor
        let query_executor = ExecutorImpl::new();

        // Build and start server
        let mut server = Builder::new(config)
            .runtimes(runtimes.clone())
            .catalog_manager(catalog_manager)
            .query_executor(query_executor)
            .table_engine(engine_proxy)
            .function_registry(function_registry)
            .build()
            .unwrap_or_else(|e| {
                panic!("Failed to create server, err:{}", e);
            });
        server.start().await.unwrap_or_else(|e| {
            panic!("Failed to start server,, err:{}", e);
        });

        // Wait for signal
        signal_handler::wait_for_signal();

        // Stop server
        server.stop();
    });
}
