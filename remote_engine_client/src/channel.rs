// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

//! Channel pool

use std::num::NonZeroUsize;

use clru::CLruCache;
use common_util::partitioned_lock::PartitionedMutex;
use router::endpoint::Endpoint;
use snafu::ResultExt;
use tonic::transport::{Channel, Endpoint as TonicEndpoint};

use super::config::Config;
use crate::error::*;

/// Pool for reusing the built channel
pub struct ChannelPool {
    /// Channels in pool
    channels: PartitionedMutex<CLruCache<Endpoint, Channel>>,

    /// Channel builder
    builder: ChannelBuilder,
}

impl ChannelPool {
    pub fn new(config: Config) -> Self {
        let channels = PartitionedMutex::new(
            CLruCache::new(NonZeroUsize::new(config.channel_pool_max_size_per_partition).unwrap()),
            NonZeroUsize::new(config.channel_pool_partition_num).unwrap(),
        );
        let builder = ChannelBuilder::new(config);

        Self { channels, builder }
    }

    pub async fn get(&self, endpoint: &Endpoint) -> Result<Channel> {
        {
            let mut inner = self.channels.lock(endpoint).await;
            if let Some(channel) = inner.get(endpoint) {
                return Ok(channel.clone());
            }
        }

        let mut inner = self.channels.lock(endpoint).await;
        // Double check here.
        if let Some(channel) = inner.get(endpoint) {
            return Ok(channel.clone());
        }

        let channel = self
            .builder
            .build(endpoint.clone().to_string().as_str())
            .await?;
        inner.put(endpoint.clone(), channel.clone());

        Ok(channel)
    }
}

/// Channel builder
struct ChannelBuilder {
    config: Config,
}

impl ChannelBuilder {
    fn new(config: Config) -> Self {
        Self { config }
    }

    async fn build(&self, endpoint: &str) -> Result<Channel> {
        let formatted_endpoint = make_formatted_endpoint(endpoint);
        let configured_endpoint =
            TonicEndpoint::from_shared(formatted_endpoint.clone()).context(BuildChannel {
                addr: formatted_endpoint.clone(),
                msg: "invalid endpoint",
            })?;

        let configured_endpoint = configured_endpoint
            .connect_timeout(self.config.connect_timeout.0)
            .keep_alive_timeout(self.config.channel_keep_alive_timeout.0)
            .http2_keep_alive_interval(self.config.channel_keep_alive_interval.0)
            .keep_alive_while_idle(true);

        let channel = configured_endpoint.connect().await.context(BuildChannel {
            addr: formatted_endpoint.clone(),
            msg: "connect failed",
        })?;

        Ok(channel)
    }
}

fn make_formatted_endpoint(endpoint: &str) -> String {
    format!("http://{}", endpoint)
}
