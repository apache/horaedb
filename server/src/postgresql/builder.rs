// Copyright 2023 The CeresDB Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::{net::SocketAddr, sync::Arc, time::Duration};

use proxy::Proxy;
use query_engine::{executor::Executor as QueryExecutor, physical_planner::PhysicalPlanner};
use snafu::{OptionExt, ResultExt};
use table_engine::engine::EngineRuntimes;

use crate::postgresql::{
    error::{MissingInstance, MissingRuntimes, ParseIpAddr, Result},
    PostgresqlService,
};

pub struct Builder<Q, P> {
    ip: String,
    port: u16,
    runtimes: Option<Arc<EngineRuntimes>>,
    proxy: Option<Arc<Proxy<Q, P>>>,
    timeout: Option<Duration>,
}

impl<Q: QueryExecutor + 'static, P: PhysicalPlanner> Builder<Q, P> {
    pub fn new() -> Self {
        Self {
            ip: "127.0.0.1".to_string(),
            port: 5433,
            runtimes: None,
            proxy: None,
            timeout: None,
        }
    }

    pub fn build(self) -> Result<PostgresqlService<Q, P>> {
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

    pub fn proxy(mut self, proxy: Arc<Proxy<Q, P>>) -> Self {
        self.proxy = Some(proxy);
        self
    }
}
