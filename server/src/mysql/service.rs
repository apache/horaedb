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
use opensrv_mysql::AsyncMysqlIntermediary;
use proxy::Proxy;
use runtime::JoinHandle;
use table_engine::engine::EngineRuntimes;
use tokio::sync::oneshot::{self, Receiver, Sender};

use crate::mysql::{error::Result, worker::MysqlWorker};

pub struct MysqlService {
    proxy: Arc<Proxy>,
    runtimes: Arc<EngineRuntimes>,
    socket_addr: SocketAddr,
    join_handler: Option<JoinHandle<()>>,
    tx: Option<Sender<()>>,
    timeout: Option<Duration>,
}

impl MysqlService {
    pub fn new(
        proxy: Arc<Proxy>,
        runtimes: Arc<EngineRuntimes>,
        socket_addr: SocketAddr,
        timeout: Option<Duration>,
    ) -> MysqlService {
        Self {
            proxy,
            runtimes,
            socket_addr,
            join_handler: None,
            tx: None,
            timeout,
        }
    }
}

impl MysqlService {
    pub async fn start(&mut self) -> Result<()> {
        let (tx, rx) = oneshot::channel();

        let rt = self.runtimes.clone();
        self.tx = Some(tx);

        info!("MySQL server tries to listen on {}", self.socket_addr);

        self.join_handler = Some(rt.default_runtime.spawn(Self::loop_accept(
            self.proxy.clone(),
            self.runtimes.clone(),
            self.socket_addr,
            self.timeout,
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
        runtimes: Arc<EngineRuntimes>,
        socket_addr: SocketAddr,
        timeout: Option<Duration>,
        mut rx: Receiver<()>,
    ) {
        let listener = tokio::net::TcpListener::bind(socket_addr)
            .await
            .unwrap_or_else(|e| {
                panic!("Mysql server listens failed, err:{e}");
            });
        loop {
            tokio::select! {
                conn_result = listener.accept() => {
                    let (stream, _) = match conn_result {
                        Ok((s, addr)) => (s, addr),
                        Err(err) => {
                            error!("Mysql Server accept new connection fail. err: {}", err);
                            break;
                        }
                    };
                    let proxy = proxy.clone();

                    let rt = runtimes.read_runtime.clone();
                    rt.spawn(AsyncMysqlIntermediary::run_on(
                        MysqlWorker::new(proxy,  timeout),
                        stream,
                    ));
                },
                _ = &mut rx => {
                    break;
                }
            }
        }
    }
}
