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

//! Cached router

use std::{collections::HashMap, sync::RwLock};

use ceresdbproto::storage::{self, RequestContext};
use log::debug;
use router::{endpoint::Endpoint, RouterRef};
use snafu::{OptionExt, ResultExt};
use table_engine::remote::model::TableIdentifier;
use tonic::transport::Channel as TonicChannel;

use crate::{channel::ChannelPool, config::Config, error::*};

pub struct CachedRouter {
    /// Router which can route table to its endpoint
    router: RouterRef,

    /// Cache mapping table to channel of its endpoint
    // TODO: we should add gc for the cache
    cache: RwLock<HashMap<TableIdentifier, RouteContext>>,

    /// Channel pool
    channel_pool: ChannelPool,
}

#[derive(Clone)]
pub struct RouteContext {
    pub channel: TonicChannel,
    pub endpoint: Endpoint,
}

impl CachedRouter {
    pub fn new(router: RouterRef, config: Config) -> Self {
        let cache = RwLock::new(HashMap::new());

        Self {
            router,
            cache,
            channel_pool: ChannelPool::new(config),
        }
    }

    pub async fn route(&self, table_ident: &TableIdentifier) -> Result<RouteContext> {
        // Find in cache first.
        let channel_opt = {
            let cache = self.cache.read().unwrap();
            cache.get(table_ident).cloned()
        };

        if let Some(channel) = channel_opt {
            // If found, return it.
            debug!(
                "CachedRouter found channel in cache, table_ident:{:?}",
                table_ident
            );

            Ok(channel)
        } else {
            // If not found, do real route work, and try to put it into cache(may have been
            // put by other threads).
            debug!(
                "CachedRouter didn't find channel in cache, table_ident:{:?}",
                table_ident
            );
            let channel = self.do_route(table_ident).await?;

            {
                let mut cache = self.cache.write().unwrap();
                // Double check here, if still not found, we put it.
                let channel_opt = cache.get(table_ident);
                if channel_opt.is_none() {
                    debug!(
                        "CachedRouter put the new channel to cache, table_ident:{:?}",
                        table_ident
                    );
                    let _ = cache.insert(table_ident.clone(), channel.clone());
                }
            }

            Ok(channel)
        }
    }

    pub async fn evict(&self, table_idents: &[TableIdentifier]) {
        let mut cache = self.cache.write().unwrap();
        for table_ident in table_idents {
            let _ = cache.remove(table_ident);
        }
    }

    async fn do_route(&self, table_ident: &TableIdentifier) -> Result<RouteContext> {
        let schema = &table_ident.schema;
        let table = table_ident.table.clone();
        let route_request = storage::RouteRequest {
            context: Some(RequestContext {
                database: schema.to_string(),
            }),
            tables: vec![table],
        };
        let route_infos = self
            .router
            .route(route_request)
            .await
            .context(RouteWithCause {
                table_ident: table_ident.clone(),
            })?;

        if route_infos.is_empty() {
            return RouteNoCause {
                table_ident: table_ident.clone(),
                msg: "route infos is empty",
            }
            .fail();
        }

        // Get channel from pool.
        let endpoint = route_infos
            .first()
            .unwrap()
            .endpoint
            .clone()
            .with_context(|| RouteNoCause {
                table_ident: table_ident.clone(),
                msg: "no endpoint in route info",
            })?;

        let endpoint = endpoint.into();
        let channel = self.channel_pool.get(&endpoint).await?;

        Ok(RouteContext { channel, endpoint })
    }
}
