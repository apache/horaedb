// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

use std::{error::Error as StdError, net::SocketAddr, sync::Arc};

use catalog::manager::Manager as CatalogManager;
use log::info;
use msql_srv::MysqlIntermediary;
use query_engine::executor::Executor as QueryExecutor;
use snafu::{Backtrace, ResultExt, Snafu};
use threadpool::ThreadPool;

use crate::{
    instance::{Instance, InstanceRef},
    mysql::mysql_worker::MysqlWorker,
};

pub struct MysqlHandler<C, Q> {
    instance: InstanceRef<C, Q>,
    thread_num: usize,
    socket_addr: SocketAddr,
}

impl<C, Q> MysqlHandler<C, Q> {
    pub fn new(
        instance: Arc<Instance<C, Q>>,
        socket_addr: SocketAddr,
        thread_num: usize,
    ) -> MysqlHandler<C, Q> {
        Self {
            instance,
            thread_num,
            socket_addr,
        }
    }

    pub fn shutdown(&mut self) {
        todo!()
    }
}

impl<C: CatalogManager + 'static, Q: QueryExecutor + 'static> MysqlHandler<C, Q> {
    pub fn start(&self) -> Result<()> {
        info!("Mysql Server ip_addr {}", self.socket_addr);
        let listener = std::net::TcpListener::bind(self.socket_addr).context(ServerNotRunning)?;
        let pool = ThreadPool::new(self.thread_num);
        for stream in listener.incoming() {
            let stream = stream.context(AcceptConnection)?;
            let instance = self.instance.clone();
            pool.execute(move || {
                if let Err(err) = MysqlIntermediary::run_on_tcp(MysqlWorker::new(instance), stream)
                {
                    log::error!("Execute sql has error, {}", err);
                }
            });
        }

        Ok(())
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
