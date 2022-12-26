// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

//! Forward for grpc services
use std::{
    collections::HashMap,
    sync::{Arc, RwLock},
    time::Duration, net::{Ipv4Addr, AddrParseError}, 
};

use ceresdbproto::storage::{storage_service_client::StorageServiceClient, RouteRequest};
use log::{error, warn};
use serde_derive::Deserialize;
use snafu::{Backtrace, ResultExt, Snafu, ensure};
use tonic::{transport::{self, Channel}, metadata::errors::InvalidMetadataValue};

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
        "Invalid ip address, addr:{}, err:{}.\nBacktrace:\n{}",
        ip_addr,
        source,
        backtrace
    ))]
    InvalidIpAddr {
        ip_addr: String,
        source: AddrParseError, 
        backtrace: Backtrace,
    },

    #[snafu(display(
        "Local ip addr should not be loopback, addr:{}.\nBacktrace:\n{}",
        ip_addr,
        backtrace
    ))]
    LoopbackLocalIpAddr {
        ip_addr: String,
        backtrace: Backtrace,
    },

    #[snafu(display(
        "Invalid schema, schema:{}, err:{}.\nBacktrace:\n{}",
        schema,
        source,
        backtrace
    ))]
    InvalidSchema {
        schema: String,
        source: InvalidMetadataValue,
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
    pub enable: bool,
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
            enable: false,
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
    local_endpoint: Endpoint,
    clients: RwLock<HashMap<Endpoint, StorageServiceClient<Channel>>>,
}

pub enum ForwardResult<Resp, Err> {
    Original,
    Forwarded(std::result::Result<Resp, Err>),
}

pub struct ForwardRequest<Req: std::fmt::Debug + Clone> {
    pub schema: String,
    pub metric: String,
    pub req: tonic::Request<Req>,
}

impl Forwarder {
    pub fn try_new(config: Config, router: RouterRef, local_endpoint: Endpoint) -> Result<Self> {
        ensure!(!Self::is_loopback_ip(&local_endpoint.addr)?, LoopbackLocalIpAddr {
            ip_addr: &local_endpoint.addr,
        });

        Ok(Self {
            config,
            local_endpoint,
            router,
            clients: RwLock::new(HashMap::new()),
        })
    }

    pub async fn forward<Req, Resp, Err, F>(
        &self,
        forward_req: ForwardRequest<Req>,
        do_rpc: F,
    ) -> Result<ForwardResult<Resp, Err>>
    where
        F: FnOnce(
            StorageServiceClient<Channel>,
            tonic::Request<Req>,
        )
            -> Box<dyn std::future::Future<Output = std::result::Result<Resp, Err>> + Send + Unpin>,
        Req: std::fmt::Debug + Clone,
    {
        if !self.config.enable {
            return Ok(ForwardResult::Original)
        }

        let ForwardRequest {
            schema,
            metric,
            mut req,
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
                    return Ok(ForwardResult::Original);
                }

                Endpoint::from(routes.remove(0).endpoint.unwrap())
            }
            Err(e) => {
                error!("Fail to route request, req:{:?}, err:{}", req, e);
                return Ok(ForwardResult::Original);
            }
        };

        // TODO: Detect the loopback forwarding to avoid endless calling.
        if self.is_local_endpoint(&endpoint)? {
            return Ok(ForwardResult::Original);
        }

        let client = self.get_or_create_client(&endpoint).await?;
        let metadata = req.metadata_mut();
        metadata.insert(TENANT_HEADER, schema.parse().context(InvalidSchema{schema})?);

        match do_rpc(client, req).await {
            Err(e) => {
                // Release the grpc client for the error doesn't belong to the normal error.
                self.release_client(&endpoint);
                Ok(ForwardResult::Forwarded(Err(e)))
            }
            Ok(resp) => Ok(ForwardResult::Forwarded(Ok(resp))),
        }
    }

    fn is_loopback_ip(ip_addr: &str) -> Result<bool> {
        let ip = ip_addr.parse::<Ipv4Addr>().context(InvalidIpAddr {
            ip_addr,
        })?;

        Ok(ip.is_loopback())
    }

    fn is_local_endpoint(&self, remote: &Endpoint) -> Result<bool> {
        if &self.local_endpoint == remote {
            return Ok(true)
        }

        if self.local_endpoint.port != remote.port {
            return Ok(false)
        }

        // Only need to check the remote is loopback addr.
        Self::is_loopback_ip(&remote.addr)
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
