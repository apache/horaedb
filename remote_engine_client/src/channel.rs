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

//! Channel pool

use std::{collections::HashMap, sync::RwLock};

use router::endpoint::Endpoint;
use snafu::ResultExt;
use tonic::transport::{Channel, Endpoint as TonicEndpoint};

use crate::{config::Config, error::*};

/// Pool for reusing the built channel
pub struct ChannelPool {
    /// Channels in pool
    channels: RwLock<HashMap<Endpoint, Channel>>,

    /// Channel builder
    builder: ChannelBuilder,
}

impl ChannelPool {
    pub fn new(config: Config) -> Self {
        let channels = RwLock::new(HashMap::new());
        let builder = ChannelBuilder::new(config);

        Self { channels, builder }
    }

    pub async fn get(&self, endpoint: &Endpoint) -> Result<Channel> {
        {
            let inner = self.channels.read().unwrap();
            if let Some(channel) = inner.get(endpoint) {
                return Ok(channel.clone());
            }
        }

        let channel = self.builder.build(&endpoint.to_string()).await?;
        let mut inner = self.channels.write().unwrap();
        // Double check here.
        if let Some(channel) = inner.get(endpoint) {
            return Ok(channel.clone());
        }

        inner.insert(endpoint.clone(), channel.clone());

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
    format!("http://{endpoint}")
}
