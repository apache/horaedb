// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

//! Cached router

use std::collections::HashMap;

use ceresdbproto::storage;
use log::debug;
use router::RouterRef;
use snafu::{OptionExt, ResultExt};
use table_engine::remote::model::TableIdentifier;
use tokio::sync::RwLock;
use tonic::transport::Channel;

use crate::{channel::ChannelPool, config::Config, error::*};

pub struct CachedRouter {
    /// Router which can route table to its endpoint
    router: RouterRef,

    /// Cache mapping table to channel of its endpoint
    // TODO: we should add gc for the cache
    cache: RwLock<HashMap<TableIdentifier, Channel>>,

    /// Channel pool
    channel_pool: ChannelPool,
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

    pub async fn route(&self, table_ident: &TableIdentifier) -> Result<Channel> {
        // Find in cache first.
        let channel_opt = {
            let cache = self.cache.read().await;
            cache.get(table_ident).cloned()
        };

        let channel = if let Some(channel) = channel_opt {
            // If found, return it.
            debug!(
                "CachedRouter found channel in cache, table_ident:{:?}",
                table_ident
            );

            channel
        } else {
            // If not found, do real route work, and try to put it into cache(may have been
            // put by other threads).
            debug!(
                "CachedRouter didn't find channel in cache, table_ident:{:?}",
                table_ident
            );
            let channel = self.do_route(table_ident).await?;

            {
                let mut cache = self.cache.write().await;
                // Double check here, if still not found, we put it.
                let channel_opt = cache.get(table_ident).cloned();
                if channel_opt.is_none() {
                    debug!(
                        "CachedRouter put the new channel to cache, table_ident:{:?}",
                        table_ident
                    );
                    let _ = cache.insert(table_ident.clone(), channel.clone());
                }
            }

            channel
        };

        Ok(channel)
    }

    pub async fn evict(&self, table_ident: &TableIdentifier) {
        let mut cache = self.cache.write().await;
        let _ = cache.remove(table_ident);
    }

    async fn do_route(&self, table_ident: &TableIdentifier) -> Result<Channel> {
        let schema = &table_ident.schema;
        let table = table_ident.table.clone();
        let route_request = storage::RouteRequest {
            metrics: vec![table],
        };
        let route_infos =
            self.router
                .route(schema, route_request)
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

        Ok(channel)
    }
}
