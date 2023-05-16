// Copyright 2022-2023 CeresDB Project Authors. Licensed under Apache-2.0.

use std::{net::SocketAddr, sync::Arc, time::Duration};

use proxy::Proxy;
use query_engine::executor::Executor as QueryExecutor;
use snafu::{OptionExt, ResultExt};
use table_engine::engine::EngineRuntimes;

use crate::mysql::{
    error::{MissingInstance, MissingRuntimes, ParseIpAddr, Result},
    service::MysqlService,
};

pub struct Builder<Q> {
    config: Config,
    runtimes: Option<Arc<EngineRuntimes>>,
    proxy: Option<Arc<Proxy<Q>>>,
}

#[derive(Debug)]
pub struct Config {
    pub ip: String,
    pub port: u16,
    pub timeout: Option<Duration>,
}

impl<Q> Builder<Q> {
    pub fn new(config: Config) -> Self {
        Self {
            config,
            runtimes: None,
            proxy: None,
        }
    }

    pub fn runtimes(mut self, runtimes: Arc<EngineRuntimes>) -> Self {
        self.runtimes = Some(runtimes);
        self
    }

    pub fn proxy(mut self, proxy: Arc<Proxy<Q>>) -> Self {
        self.proxy = Some(proxy);
        self
    }
}

impl<Q: QueryExecutor + 'static> Builder<Q> {
    pub fn build(self) -> Result<MysqlService<Q>> {
        let runtimes = self.runtimes.context(MissingRuntimes)?;
        let proxy = self.proxy.context(MissingInstance)?;

        let addr: SocketAddr = format!("{}:{}", self.config.ip, self.config.port)
            .parse()
            .context(ParseIpAddr { ip: self.config.ip })?;

        let mysql_handler = MysqlService::new(proxy, runtimes, addr, self.config.timeout);
        Ok(mysql_handler)
    }
}
