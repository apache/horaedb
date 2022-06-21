// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

use std::{net::SocketAddr, sync::Arc};

use catalog::manager::Manager as CatalogManager;
use query_engine::executor::Executor as QueryExecutor;
use snafu::{Backtrace, OptionExt, ResultExt, Snafu};
use table_engine::engine::EngineRuntimes;

use crate::{instance::InstanceRef, mysql::handler::MysqlHandler};

pub struct Builder<C, Q> {
    config: Config,
    runtimes: Option<Arc<EngineRuntimes>>,
    instance: Option<InstanceRef<C, Q>>,
}

#[derive(Debug)]
pub struct Config {
    pub ip: String,
    pub port: u16,
}

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Missing runtimes to build service.\nBacktrace:\n{}", backtrace))]
    MissingRuntimes { backtrace: Backtrace },

    #[snafu(display("Missing instance to build service.\nBacktrace:\n{}", backtrace))]
    MissingInstance { backtrace: Backtrace },

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
}

define_result!(Error);

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
        let runtimes = self.runtimes.context(MissingRuntimes)?;
        let instance = self.instance.context(MissingInstance)?;

        let addr: SocketAddr = format!("{}:{}", self.config.ip, self.config.port)
            .parse()
            .context(ParseIpAddr { ip: self.config.ip })?;

        let mysql_handler = MysqlHandler::new(instance, runtimes, addr);
        Ok(mysql_handler)
    }
}
