// Copyright 2022-2023 CeresDB Project Authors. Licensed under Apache-2.0.

use std::{net::SocketAddr, sync::Arc, time::Duration};

use common_util::runtime::JoinHandle;
use log::{error, info};
use opensrv_mysql::AsyncMysqlIntermediary;
use proxy::instance::{Instance, InstanceRef};
use query_engine::executor::Executor as QueryExecutor;
use router::RouterRef;
use table_engine::engine::EngineRuntimes;
use tokio::sync::oneshot::{self, Receiver, Sender};

use crate::mysql::{error::Result, worker::MysqlWorker};

pub struct MysqlService<Q> {
    instance: InstanceRef<Q>,
    runtimes: Arc<EngineRuntimes>,
    router: RouterRef,
    socket_addr: SocketAddr,
    join_handler: Option<JoinHandle<()>>,
    tx: Option<Sender<()>>,
    timeout: Option<Duration>,
}

impl<Q> MysqlService<Q> {
    pub fn new(
        instance: Arc<Instance<Q>>,
        runtimes: Arc<EngineRuntimes>,
        router: RouterRef,
        socket_addr: SocketAddr,
        timeout: Option<Duration>,
    ) -> MysqlService<Q> {
        Self {
            instance,
            runtimes,
            router,
            socket_addr,
            join_handler: None,
            tx: None,
            timeout,
        }
    }
}

impl<Q: QueryExecutor + 'static> MysqlService<Q> {
    pub async fn start(&mut self) -> Result<()> {
        let (tx, rx) = oneshot::channel();

        let rt = self.runtimes.clone();
        self.tx = Some(tx);

        info!("MySQL server tries to listen on {}", self.socket_addr);

        self.join_handler = Some(rt.default_runtime.spawn(Self::loop_accept(
            self.instance.clone(),
            self.runtimes.clone(),
            self.router.clone(),
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
        instance: InstanceRef<Q>,
        runtimes: Arc<EngineRuntimes>,
        router: RouterRef,
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
                    let instance = instance.clone();
                    let router = router.clone();

                    let rt = runtimes.read_runtime.clone();
                    rt.spawn(AsyncMysqlIntermediary::run_on(
                        MysqlWorker::new(instance, router, timeout),
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
