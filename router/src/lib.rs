// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

pub mod cluster_based;
pub mod endpoint;
pub(crate) mod hash;
pub mod rule_based;
use std::{sync::Arc, time::Duration};

use async_trait::async_trait;
use ceresdbproto::storage::{Route, RouteRequest};
pub use cluster_based::ClusterBasedRouter;
use common_util::{config::ReadableDuration, define_result};
pub use rule_based::{RuleBasedRouter, RuleList};
use serde::{Deserialize, Serialize};
use snafu::{Backtrace, Snafu};

#[derive(Snafu, Debug)]
#[snafu(visibility(pub))]
pub enum Error {
    #[snafu(display(
        "No route found for schema, schema:{}.\nBacktrace:\n{}",
        schema,
        backtrace
    ))]
    RouteNotFound {
        schema: String,
        backtrace: Backtrace,
    },

    #[snafu(display(
        "No shard found for metric, schema:{}, table:{}.\nBacktrace:\n{}",
        schema,
        table,
        backtrace
    ))]
    ShardNotFound {
        schema: String,
        table: String,
        backtrace: Backtrace,
    },

    #[snafu(display("Failed to parse endpoint, endpoint:{}, err:{}", endpoint, source))]
    ParseEndpoint {
        endpoint: String,
        source: Box<dyn std::error::Error + Sync + Send>,
    },

    #[snafu(display("Failure caused by others, msg:{}, err:{}", msg, source))]
    OtherWithCause {
        msg: String,
        source: Box<dyn std::error::Error + Sync + Send>,
    },

    #[snafu(display("Failure caused by others, msg:{}.\nBacktrace:\n{}", msg, backtrace))]
    OtherNoCause { msg: String, backtrace: Backtrace },
}

define_result!(Error);

pub type RouterRef = Arc<dyn Router + Sync + Send>;

#[async_trait]
pub trait Router {
    async fn route(&self, req: RouteRequest) -> Result<Vec<Route>>;
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct RouteCacheConfig {
    // enable route cache, default false
    enable: bool,
    /// Time to live (TTL) in second.
    ttl: ReadableDuration,
    /// Time to idle (TTI) in second.
    tti: ReadableDuration,
    /// how many route records can store in cache.
    capacity: u64,
}

impl Default for RouteCacheConfig {
    fn default() -> Self {
        Self {
            enable: false,
            ttl: ReadableDuration::from(Duration::from_secs(5)),
            tti: ReadableDuration::from(Duration::from_secs(5)),
            capacity: 10_000,
        }
    }
}
