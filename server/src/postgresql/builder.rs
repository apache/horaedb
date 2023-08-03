use std::{net::SocketAddr, sync::Arc, time::Duration};

use proxy::Proxy;
use query_engine::executor::Executor as QueryExecutor;
use snafu::{OptionExt, ResultExt};
use table_engine::engine::EngineRuntimes;

use crate::postgresql::{
    error::{MissingInstance, MissingRuntimes, ParseIpAddr, Result},
    PostgresqlService,
};

pub struct Builder<Q> {
    ip: String,
    port: u16,
    runtimes: Option<Arc<EngineRuntimes>>,
    proxy: Option<Arc<Proxy<Q>>>,
    timeout: Option<Duration>,
}

impl<Q: QueryExecutor + 'static> Builder<Q> {
    pub fn new() -> Self {
        Self {
            ip: "127.0.0.1".to_string(),
            port: 5433,
            runtimes: None,
            proxy: None,
            timeout: None,
        }
    }

    pub fn build(self) -> Result<PostgresqlService<Q>> {
        let runtimes = self.runtimes.context(MissingRuntimes)?;
        let proxy = self.proxy.context(MissingInstance)?;

        let addr: SocketAddr = format!("{}:{}", self.ip, self.port)
            .parse()
            .context(ParseIpAddr { ip: self.ip })?;

        Ok(PostgresqlService::new(proxy, runtimes, addr, self.timeout))
    }

    pub fn ip(mut self, ip: String) -> Self {
        self.ip = ip;
        self
    }

    pub fn port(mut self, port: u16) -> Self {
        self.port = port;
        self
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
