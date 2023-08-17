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

use crate::mysql::{
    error::{MissingInstance, MissingRuntimes, ParseIpAddr, Result},
    service::MysqlService,
};

pub struct Builder {
    config: Config,
    runtimes: Option<Arc<EngineRuntimes>>,
    proxy: Option<Arc<Proxy>>,
}

#[derive(Debug)]
pub struct Config {
    pub ip: String,
    pub port: u16,
    pub timeout: Option<Duration>,
}

impl Builder {
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

    pub fn proxy(mut self, proxy: Arc<Proxy>) -> Self {
        self.proxy = Some(proxy);
        self
    }
}

impl Builder {
    pub fn build(self) -> Result<MysqlService> {
        let runtimes = self.runtimes.context(MissingRuntimes)?;
        let proxy = self.proxy.context(MissingInstance)?;

        let addr: SocketAddr = format!("{}:{}", self.config.ip, self.config.port)
            .parse()
            .context(ParseIpAddr { ip: self.config.ip })?;

        let mysql_handler = MysqlService::new(proxy, runtimes, addr, self.config.timeout);
        Ok(mysql_handler)
    }
}
