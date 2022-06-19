// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

use std::{error::Error as StdError, net::SocketAddr, sync::Arc};

use catalog::manager::Manager as CatalogManager;
use query_engine::executor::Executor as QueryExecutor;
use snafu::{Backtrace, OptionExt, ResultExt, Snafu};
use table_engine::engine::EngineRuntimes;

use super::mysql_handler::MysqlHandler;
use crate::instance::InstanceRef;

pub struct Builder<C, Q> {
    config: Config,
    runtimes: Option<Arc<EngineRuntimes>>,
    instance: Option<InstanceRef<C, Q>>,
}

impl<C, Q> Builder<C, Q> {
    pub fn new(config: Config) -> Self {
        Self {
            config,
            runtimes: None,
            instance: None,
        }
    }

    pub fn runtimes(mut self, runtimes: Arc<EngineRuntimes>) -> Self {
        self.runtimes = Some(runtimes);
        self
    }

    pub fn instance(mut self, instance: InstanceRef<C, Q>) -> Self {
        self.instance = Some(instance);
        self
    }
}

impl<C: CatalogManager + 'static, Q: QueryExecutor + 'static> Builder<C, Q> {
    pub fn build(self) -> Result<MysqlHandler<C, Q>> {
        let instance = self.instance.context(MissingInstance)?;

        let addr: SocketAddr = format!("{}:{}", self.config.ip, self.config.port)
            .parse()
            .context(ParseIpAddr { ip: self.config.ip })?;
        let mysql_handler = MysqlHandler::new(instance, addr, self.config.thread_num);
        Ok(mysql_handler)
    }
}

#[derive(Debug)]
pub struct Config {
    pub ip: String,
    pub port: u16,
    pub thread_num: usize,
}

define_result!(Error);

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Failed to create request context, err:{}", source))]
    CreateContext { source: crate::context::Error },

    #[snafu(display("Failed to handle request, err:{}", source))]
    HandleRequest {
        source: crate::handlers::error::Error,
    },

    #[snafu(display("Missing runtimes to build service.\nBacktrace:\n{}", backtrace))]
    MissingRuntimes { backtrace: Backtrace },

    #[snafu(display("Missing instance to build service.\nBacktrace:\n{}", backtrace))]
    MissingInstance { backtrace: Backtrace },

    #[snafu(display("Build Mysql service fail.\nBacktrace:\n{}", backtrace))]
    BuildFail { backtrace: Backtrace },

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
