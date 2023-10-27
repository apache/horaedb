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

use logger::{error, info};
use pgwire::api::{
    auth::noop::NoopStartupHandler, query::PlaceholderExtendedQueryHandler, MakeHandler,
    StatelessMakeHandler,
};
use proxy::Proxy;
use runtime::JoinHandle;
use table_engine::engine::EngineRuntimes;
use tokio::sync::oneshot::{self, Receiver, Sender};

use crate::postgresql::{error::Result, handler::PostgresqlHandler};

pub struct PostgresqlService {
    addr: SocketAddr,
    proxy: Arc<Proxy>,
    runtimes: Arc<EngineRuntimes>,
    join_handler: Option<JoinHandle<()>>,
    tx: Option<Sender<()>>,
    timeout: Option<Duration>,
}

impl PostgresqlService {
    pub fn new(
        proxy: Arc<Proxy>,
        runtimes: Arc<EngineRuntimes>,
        addr: SocketAddr,
        timeout: Option<Duration>,
    ) -> Self {
        Self {
            proxy,
            runtimes,
            addr,
            join_handler: None,
            tx: None,
            timeout,
        }
    }

    pub async fn start(&mut self) -> Result<()> {
        let rt = self.runtimes.clone();
        let (tx, rx) = oneshot::channel();
        self.tx = Some(tx);

        info!("PostgreSQL server tries to listen on {}", self.addr);

        self.join_handler = Some(rt.default_runtime.spawn(Self::loop_accept(
            self.proxy.clone(),
            self.timeout,
            self.runtimes.clone(),
            self.addr,
            rx,
        )));

        Ok(())
    }

    pub fn shutdown(self) {
        if let Some(tx) = self.tx {
            let _ = tx.send(());
        }
    }

    async fn loop_accept(
        proxy: Arc<Proxy>,
        timeout: Option<Duration>,
        runtimes: Arc<EngineRuntimes>,
        socket_addr: SocketAddr,
        mut rx: Receiver<()>,
    ) {
        let listener = tokio::net::TcpListener::bind(socket_addr)
            .await
            .unwrap_or_else(|e| {
                panic!("PostgreSQL server listens failed, err:{e}");
            });

        let authenticator = Arc::new(StatelessMakeHandler::new(Arc::new(NoopStartupHandler)));
        let processor = Arc::new(StatelessMakeHandler::new(Arc::new(PostgresqlHandler {
            proxy: proxy.clone(),
            timeout,
        })));
        let placeholder = Arc::new(StatelessMakeHandler::new(Arc::new(
            PlaceholderExtendedQueryHandler,
        )));
        let rt = runtimes.read_runtime.clone();
        loop {
            tokio::select! {
                    conn_result = listener.accept() => {
                        let (stream, _) = match conn_result {
                            Ok((s, addr)) => (s, addr),
                            Err(err) => {
                                error!("PostgreSQL Server accept new connection fail. err: {}", err);
                                break;
                            }
                        };
                        rt.spawn(pgwire::tokio::process_socket(
                            stream,
                            None,
                            authenticator.make(),
                            processor.make(),
                            placeholder.make(),
                        ));
                    },
                    _ = &mut rx => {
                        break;
                    }
            }
        }
    }
}
