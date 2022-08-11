// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

//! Setup server

use std::sync::Arc;

use analytic_engine::{
    self,
    setup::{EngineBuilder, ReplicatedEngineBuilder, RocksEngineBuilder},
};
use async_trait::async_trait;
use catalog::{
    manager::ManagerRef as CatalogManagerRef,
    schema::{CloseOptions, CloseTableRequest, NameRef, OpenOptions, OpenTableRequest, SchemaRef},
};
use catalog_impls::{
    table_based::TableBasedManager, volatile, CatalogManagerImpl, SchemaIdAlloc, TableIdAlloc,
};
use cluster::{cluster_impl::ClusterImpl, TableManipulator};
use common_util::runtime::{self, Runtime};
use df_operator::registry::FunctionRegistryImpl;
use log::{debug, info};
use logger::RuntimeLevel;
use meta_client_v2::{
    meta_impl,
    types::{DropTableRequest, Node, NodeMetaInfo},
    MetaClient,
};
use query_engine::executor::{Executor, ExecutorImpl};
use server::{
    config::{Config, DeployMode, RuntimeConfig},
    server::Builder,
    table_engine::{MemoryTableEngine, TableEngineProxy},
};
use table_engine::{
    engine::{EngineRuntimes, TableEngineRef},
    table::{SchemaId, TableId},
    ANALYTIC_ENGINE_TYPE,
};
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
        .unwrap_or_else(|e| {
            panic!("Failed to setup analytic engine, err:{}", e);
        });

    // Create table engine proxy
    let engine_proxy = Arc::new(TableEngineProxy {
        memory,
        analytic: analytic.clone(),
    });

    // Init function registry.
    let mut function_registry = FunctionRegistryImpl::new();
    function_registry.load_functions().unwrap_or_else(|e| {
        panic!("Failed to create function registry, err:{}", e);
    });
    let function_registry = Arc::new(function_registry);

    // Create query executor
    let query_executor = ExecutorImpl::new();

    let builder = Builder::new(config.clone())
        .runtimes(runtimes.clone())
        .query_executor(query_executor)
        .table_engine(engine_proxy.clone())
        .function_registry(function_registry);

    let builder = match config.deploy_mode {
        DeployMode::Standalone => build_in_standalone_mode(builder, analytic, engine_proxy).await,
        DeployMode::Cluster => {
            build_in_cluster_mode(&config, builder, &runtimes, engine_proxy).await
        }
    };

    // Build and start server
    let mut server = builder.build().unwrap_or_else(|e| {
        panic!("Failed to create server, err:{}", e);
    });
    server.start().await.unwrap_or_else(|e| {
        panic!("Failed to start server,, err:{}", e);
    });

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
    let meta_client = build_meta_client(config, runtimes.meta_runtime.clone());

    let catalog_manager = {
        let schema_id_alloc = SchemaIdAllocWrapper(meta_client.clone());
        let table_id_alloc = TableIdAllocWrapper(meta_client.clone());
        Arc::new(volatile::ManagerImpl::new(schema_id_alloc, table_id_alloc).await)
    };

    let cluster = {
        let table_manipulator = Arc::new(TableManipulatorImpl {
            catalog_manager: catalog_manager.clone(),
            table_engine: engine_proxy,
        });
        Arc::new(
            ClusterImpl::new(
                meta_client,
                table_manipulator,
                config.cluster.clone(),
                runtimes.meta_runtime.clone(),
            )
            .unwrap(),
        )
    };

    builder.catalog_manager(catalog_manager).cluster(cluster)
}

async fn build_in_standalone_mode<Q: Executor + 'static>(
    builder: Builder<Q>,
    table_engine: TableEngineRef,
    engine_proxy: TableEngineRef,
) -> Builder<Q> {
    let table_based_manager = TableBasedManager::new(table_engine, engine_proxy.clone())
        .await
        .unwrap_or_else(|e| {
            panic!("Failed to create catalog manager, err:{}", e);
        });

    // Create catalog manager, use analytic table as backend
    let catalog_manager = Arc::new(CatalogManagerImpl::new(Arc::new(table_based_manager)));
    builder.catalog_manager(catalog_manager)
}

fn build_meta_client(config: &Config, runtime: Arc<Runtime>) -> Arc<dyn MetaClient + Send + Sync> {
    let node = Node {
        addr: config.cluster.node.clone(),
        port: config.cluster.port,
    };
    let node_meta_info = NodeMetaInfo {
        node,
        zone: config.cluster.zone.clone(),
        idc: config.cluster.idc.clone(),
        binary_version: config.cluster.binary_version.clone(),
    };
    meta_impl::build_meta_client(
        config.cluster.meta_client_config.clone(),
        node_meta_info,
        runtime,
    )
    .unwrap()
}

struct SchemaIdAllocWrapper(Arc<dyn MetaClient + Send + Sync>);

#[async_trait]
impl SchemaIdAlloc for SchemaIdAllocWrapper {
    type Error = meta_client_v2::Error;

    async fn alloc_schema_id<'a>(
        &'a self,
        schema_name: NameRef<'a>,
    ) -> Result<SchemaId, Self::Error> {
        self.0
            .alloc_schema_id(cluster::AllocSchemaIdRequest {
                name: schema_name.to_string(),
            })
            .await
            .map(|resp| SchemaId::from(resp.id))
    }
}
struct TableIdAllocWrapper(Arc<dyn MetaClient + Send + Sync>);

#[async_trait]
impl TableIdAlloc for TableIdAllocWrapper {
    type Error = meta_client_v2::Error;

    async fn alloc_table_id<'a>(
        &'a self,
        schema_name: NameRef<'a>,
        table_name: NameRef<'a>,
    ) -> Result<TableId, Self::Error> {
        self.0
            .alloc_table_id(cluster::AllocTableIdRequest {
                schema_name: schema_name.to_string(),
                name: table_name.to_string(),
            })
            .await
            .map(|v| TableId::from(v.id))
    }

    async fn invalidate_table_id<'a>(
        &'a self,
        schema_name: NameRef<'a>,
        table_name: NameRef<'a>,
        table_id: TableId,
    ) -> Result<(), Self::Error> {
        self.0
            .drop_table(DropTableRequest {
                schema_name: schema_name.to_string(),
                name: table_name.to_string(),
                id: table_id.as_u64(),
            })
            .await
            .map(|_| ())
    }
}

struct TableManipulatorImpl {
    catalog_manager: CatalogManagerRef,
    table_engine: TableEngineRef,
}
impl TableManipulatorImpl {
    fn catalog_schema_by_name(
        &self,
        schema_name: &str,
    ) -> Result<(NameRef, Option<SchemaRef>), Box<dyn std::error::Error + Send + Sync>> {
        let default_catalog_name = self.catalog_manager.default_catalog_name();
        let default_catalog = self
            .catalog_manager
            .catalog_by_name(default_catalog_name)
            .map_err(Box::new)?
            .unwrap();
        let schema = default_catalog
            .schema_by_name(schema_name)
            .map_err(Box::new)?;
        Ok((default_catalog_name, schema))
    }
}
#[async_trait]
impl TableManipulator for TableManipulatorImpl {
    async fn open_table(
        &self,
        schema_name: &str,
        table_name: &str,
        table_id: u64,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let (default_catalog_name, schema) = self.catalog_schema_by_name(schema_name)?;
        let schema = schema.unwrap();
        let table_id = TableId::from(table_id);
        let req = OpenTableRequest {
            catalog_name: default_catalog_name.to_string(),
            schema_name: schema_name.to_string(),
            schema_id: schema.id(),
            table_name: table_name.to_string(),
            table_id,
            engine: ANALYTIC_ENGINE_TYPE.to_string(),
        };
        let opts = OpenOptions {
            table_engine: self.table_engine.clone(),
        };
        let table = schema.open_table(req, opts).await.map_err(Box::new)?;
        debug!(
            "Finish opening table:{}-{}, catalog:{}, schema:{}, really_opened:{}",
            table_name,
            table_id,
            default_catalog_name,
            schema_name,
            table.is_some()
        );
        Ok(())
    }

    async fn close_table(
        &self,
        schema_name: &str,
        table_name: &str,
        table_id: u64,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let (default_catalog_name, schema) = self.catalog_schema_by_name(schema_name)?;
        let schema = schema.unwrap();
        let table_id = TableId::from(table_id);
        let req = CloseTableRequest {
            catalog_name: default_catalog_name.to_string(),
            schema_name: schema_name.to_string(),
            schema_id: schema.id(),
            table_name: table_name.to_string(),
            table_id,
            engine: ANALYTIC_ENGINE_TYPE.to_string(),
        };
        let opts = CloseOptions {
            table_engine: self.table_engine.clone(),
        };
        schema.close_table(req, opts).await.map_err(Box::new)?;
        debug!(
            "Finish closing table:{}-{}, catalog:{}, schema:{}",
            table_name, table_id, default_catalog_name, schema_name
        );
        Ok(())
    }
}
