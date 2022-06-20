// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

use std::{error::Error as StdError, net::SocketAddr, sync::Arc};

use catalog::manager::Manager as CatalogManager;
use common_util::runtime::JoinHandle;
use log::info;
use opensrv_mysql::AsyncMysqlIntermediary;
use query_engine::executor::Executor as QueryExecutor;
use snafu::{Backtrace, ResultExt, Snafu};
use table_engine::engine::EngineRuntimes;

use crate::{
    instance::{Instance, InstanceRef},
    mysql::mysql_worker::MysqlWorker,
};
pub struct MysqlHandler<C, Q> {
    instance: InstanceRef<C, Q>,
    runtimes: Arc<EngineRuntimes>,
    socket_addr: SocketAddr,
    join_handler: Option<JoinHandle<()>>,
}

impl<C, Q> MysqlHandler<C, Q> {
    pub fn new(
        instance: Arc<Instance<C, Q>>,
        runtimes: Arc<EngineRuntimes>,
        socket_addr: SocketAddr,
    ) -> MysqlHandler<C, Q> {
        Self {
            instance,
            runtimes,
            socket_addr,
            join_handler: None,
        }
    }
}

impl<C: CatalogManager + 'static, Q: QueryExecutor + 'static> MysqlHandler<C, Q> {
    pub async fn start(&mut self) -> Result<()> {
        info!("Mysql Server started in {}", self.socket_addr);
        let rt = self.runtimes.clone();
        self.join_handler = Some(rt.bg_runtime.spawn(Self::loop_accept(
            self.instance.clone(),
            self.runtimes.clone(),
            self.socket_addr,
        )));
        Ok(())
    }

    pub fn shutdown(&self) {
        // if let Some(join_handle) = self.join_handler.take() {
        //     if let Err(error) = join_handle.await {
        //         log::error!("Unexcepted error during shutdown MySQLHandler.
        // err: {}", error)     }
        // }
    }

    async fn loop_accept(
        instance: InstanceRef<C, Q>,
        runtimes: Arc<EngineRuntimes>,
        socket_addr: SocketAddr,
    ) {
        let listener = match tokio::net::TcpListener::bind(socket_addr)
            .await
            .context(ServerNotRunning)
        {
            Ok(l) => l,
            Err(err) => {
                log::error!("Mysql Server listener address fail. err: {}", err);
                return;
            }
        };
        loop {
            let (stream, _) = match listener.accept().await.context(AcceptConnection) {
                Ok((s, addr)) => (s, addr),
                Err(err) => {
                    log::error!("Mysql Server accept new connection fail. err: {}", err);
                    return;
                }
            };
            let instance = instance.clone();
            let runtimes = runtimes.clone();

            let rt = runtimes.bg_runtime.clone();
            rt.spawn(AsyncMysqlIntermediary::run_on(
                MysqlWorker::new(instance, runtimes.clone()),
                stream,
            ));
        }
    }
}

define_result!(Error);

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display(
        "Mysql Server not running, err: {}.\nBacktrace:\n{}",
        source,
        backtrace
    ))]
    ServerNotRunning {
        backtrace: Backtrace,
        source: std::io::Error,
    },

    #[snafu(display("Failed to create request context, err:{}", source))]
    CreateContext { source: crate::context::Error },

    #[snafu(display("Failed to handle request, err:{}", source))]
    HandleRequest {
        source: crate::handlers::error::Error,
    },

    #[snafu(display(
        "Mysql Server Accept a new Connection fail, err:{}.\nBacktrace:\n{},",
        source,
        backtrace
    ))]
    AcceptConnection {
        backtrace: Backtrace,
        source: std::io::Error,
    },

    #[snafu(display("Missing runtimes to build service.\nBacktrace:\n{}", backtrace))]
    MissingRuntimes { backtrace: Backtrace },

    #[snafu(display("Missing instance to build service.\nBacktrace:\n{}", backtrace))]
    MissingInstance { backtrace: Backtrace },

    #[snafu(display(
        "Fail to do heap profiling, err:{}.\nBacktrace:\n{}",
        source,
        backtrace
    ))]
    ProfileHeap {
        source: profile::Error,
        backtrace: Backtrace,
    },

    #[snafu(display("Fail to join async task, err:{}.", source))]
    JoinAsyncTask { source: common_util::runtime::Error },

    #[snafu(display(
        "Failed to parse ip addr, ip:{}, err:{}.\nBacktrace:\n{}",
        ip,
        source,
        backtrace
    ))]
    ParseIpAddr {
        ip: String,
        source: std::net::AddrParseError,
        backtrace: Backtrace,
    },

    #[snafu(display("Internal err:{}.", source))]
    Internal {
        source: Box<dyn StdError + Send + Sync>,
    },
}
