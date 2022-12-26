// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

//! Forward for grpc services
use std::{
    collections::HashMap,
    sync::{Arc, RwLock},
    time::Duration, pin::Pin,
};

use ceresdbproto::storage::{storage_service_client::StorageServiceClient, RouteRequest};
use log::{error, warn};
use serde_derive::Deserialize;
use snafu::{Backtrace, ResultExt, Snafu};
use tonic::transport::{self, Channel};

use crate::{config::Endpoint, consts::TENANT_HEADER, route::RouterRef};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display(
        "Invalid endpoint, endpoint:{}, err:{}.\nBacktrace:\n{}",
        endpoint,
        source,
        backtrace
    ))]
    InvalidEndpoint {
        endpoint: String,
        source: tonic::transport::Error,
        backtrace: Backtrace,
    },

    #[snafu(display(
        "Failed to connect endpoint, endpoint:{}, err:{}.\nBacktrace:\n{}",
        endpoint,
        source,
        backtrace
    ))]
    Connect {
        endpoint: String,
        source: tonic::transport::Error,
        backtrace: Backtrace,
    },
}

define_result!(Error);

pub type ForwarderRef = Arc<Forwarder>;

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct Config {
    /// Thread num for grpc polling
    pub thread_num: usize,
    /// -1 means unlimited
    pub max_send_msg_len: i32,
    /// -1 means unlimited
    pub max_recv_msg_len: i32,
    /// Sets an interval for HTTP2 Ping frames should be sent to keep a
    /// connection alive.
    pub keep_alive_interval: Duration,
    /// A timeout for receiving an acknowledgement of the keep-alive ping
    /// If the ping is not acknowledged within the timeout, the connection will
    /// be closed
    pub keep_alive_timeout: Duration,
    /// default keep http2 connections alive while idle
    pub keep_alive_while_idle: bool,
    pub connect_timeout: Duration,
    pub write_timeout: Duration,
    pub read_timeout: Duration,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            thread_num: 4,
            // 20MB
            max_send_msg_len: 20 * (1 << 20),
            // 1GB
            max_recv_msg_len: 1 << 30,
            keep_alive_interval: Duration::from_secs(60 * 10),
            keep_alive_timeout: Duration::from_secs(3),
            keep_alive_while_idle: true,
            connect_timeout: Duration::from_secs(3),
            write_timeout: Duration::from_secs(5),
            read_timeout: Duration::from_secs(60),
        }
    }
}

pub struct Forwarder {
    config: Config,
    router: RouterRef,
    clients: RwLock<HashMap<Endpoint, StorageServiceClient<Channel>>>,
}

pub enum ForwardResult<Req, Resp, Err> {
    OrigReq(Req),
    Resp(std::result::Result<Resp, Err>),
}

#[derive(Debug, Clone)]
pub struct ForwardRequest<Req: std::fmt::Debug + Clone> {
    pub schema: String,
    pub metric: String,
    pub req: Req,
}

impl Forwarder {
    pub fn new(config: Config, router: RouterRef) -> Self {
        Self {
            config,
            router,
            clients: RwLock::new(HashMap::new()),
        }
    }

    pub async fn forward<Req, Resp, Err, F>(
        &self,
        forward_req: ForwardRequest<Req>,
        do_rpc: F,
    ) -> Result<ForwardResult<Req, Resp, Err>>
    where
        F: FnOnce(
            StorageServiceClient<Channel>,
            Req,
        )
            -> Pin<Box<dyn std::future::Future<Output = std::result::Result<Resp, Err>> + Send>>,
        Req: std::fmt::Debug + Clone,
    {
        let ForwardRequest {
            schema,
            metric,
            req,
        } = forward_req;

        let route_req = RouteRequest {
            metrics: vec![metric],
        };

        let endpoint = match self.router.route(&schema, route_req).await {
            Ok(mut routes) => {
                if routes.len() != 1 || routes[0].endpoint.is_none() {
                    warn!(
                        "Fail to forward request for multiple route results, routes result:{:?}, req:{:?}",
                        routes, req 
                    );
                    return Ok(ForwardResult::OrigReq(req));
                }

                Endpoint::from(routes.remove(0).endpoint.unwrap())
            }
            Err(e) => {
                error!("Fail to route request, req:{:?}, err:{}", req, e);
                return Ok(ForwardResult::OrigReq(req));
            }
        };

        // TODO: Detect the loopback forwarding to avoid endless calling.
        let is_local_ip = true;
        let is_local_port = true;
        if is_local_ip && is_local_port {
            return Ok(ForwardResult::OrigReq(req));
        }

        let client = self.get_or_create_client(&endpoint).await?;

        match do_rpc(client, req).await {
            Err(e) => {
                // Release the grpc client for the error doesn't belong to the normal error.
                self.release_client(&endpoint);
                return Ok(ForwardResult::Resp(Err(e)));
            }
            Ok(resp) => return Ok(ForwardResult::Resp(Ok(resp))),
        }
    }

    async fn get_or_create_client(
        &self,
        endpoint: &Endpoint,
    ) -> Result<StorageServiceClient<Channel>> {
        {
            let clients = self.clients.read().unwrap();
            if let Some(v) = clients.get(endpoint) {
                return Ok(v.clone());
            }
        }

        let new_client = self.build_client(endpoint).await?;
        {
            let mut clients = self.clients.write().unwrap();
            if let Some(v) = clients.get(endpoint) {
                return Ok(v.clone());
            }
            clients.insert(endpoint.clone(), new_client.clone());
        }

        Ok(new_client)
    }

    /// Release the client for the given endpoint.
    fn release_client(&self, endpoint: &Endpoint) -> Option<StorageServiceClient<Channel>> {
        let mut clients = self.clients.write().unwrap();
        clients.remove(endpoint)
    }

    async fn build_client(&self, endpoint: &Endpoint) -> Result<StorageServiceClient<Channel>> {
        let endpoint_with_scheme = Self::make_endpoint_with_scheme(endpoint);
        let configured_endpoint = transport::Endpoint::from_shared(endpoint_with_scheme.clone())
            .context(InvalidEndpoint {
                endpoint: &endpoint_with_scheme,
            })?;

        let configured_endpoint = match self.config.keep_alive_while_idle {
            true => configured_endpoint
                .connect_timeout(self.config.connect_timeout)
                .keep_alive_timeout(self.config.keep_alive_timeout)
                .keep_alive_while_idle(true)
                .http2_keep_alive_interval(self.config.keep_alive_interval),
            false => configured_endpoint
                .connect_timeout(self.config.connect_timeout)
                .keep_alive_while_idle(false),
        };
        let channel = configured_endpoint.connect().await.context(Connect {
            endpoint: &endpoint_with_scheme,
        })?;

        Ok(StorageServiceClient::new(channel))
    }

    #[inline]
    fn make_endpoint_with_scheme(endpoint: &Endpoint) -> String {
        format!("http://{}:{}", endpoint.addr, endpoint.port)
    }
}
