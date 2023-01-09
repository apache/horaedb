// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

//! Cached router

use std::num::NonZeroUsize;

use ceresdbproto::storage;
use clru::CLruCache;
use router::RouterRef;
use snafu::{OptionExt, ResultExt};
use table_engine::remote::model::TableIdentifier;
use tokio::sync::Mutex;
use tonic::transport::Channel;

use crate::{channel::ChannelPool, config::Config, error::*};

pub struct CachedRouter {
    /// Router which can route table to its endpoint
    router: RouterRef,

    /// Cache mapping table to channel of its endpoint
    // TODO: should be replaced with a cache(like "moka")
    // or partition the lock.
    cache: Mutex<CLruCache<TableIdentifier, Channel>>,

    /// Channel pool
    channel_pool: ChannelPool,
}

impl CachedRouter {
    pub fn new(router: RouterRef, config: Config) -> Self {
        Self {
            router,
            cache: Mutex::new(CLruCache::new(
                NonZeroUsize::new(config.route_cache_size).unwrap(),
            )),
            channel_pool: ChannelPool::new(config),
        }
    }

    pub async fn route(&self, table_ident: &TableIdentifier) -> Result<Channel> {
        // Find in cache first.
        let channel_opt = {
            let mut cache = self.cache.lock().await;
            cache.get(table_ident).cloned()
        };

        let channel = if let Some(channel) = channel_opt {
            // If found, return it.
            channel
        } else {
            // If not found, do real route and put it into cache, and return then.
            let mut cache = self.cache.lock().await;

            // Double check here, it may have been put by others.
            let channel_opt = cache.get(table_ident).cloned();
            if let Some(channel) = channel_opt {
                channel
            } else {
                self.do_route(table_ident).await?
            }
        };

        Ok(channel)
    }

    pub async fn evict(&self, table_ident: &TableIdentifier) {
        let mut cache = self.cache.lock().await;
        let _ = cache.pop(table_ident);
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
            .context(RouteNoCause {
                table_ident: table_ident.clone(),
                msg: "no endpoint in route info",
            })?;

        let endpoint = endpoint.into();
        let channel = self.channel_pool.get(&endpoint).await?;

        Ok(channel)
    }
}
