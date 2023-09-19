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

pub mod cluster_based;
pub mod endpoint;
mod hash;
pub mod rule_based;
use std::{sync::Arc, time::Duration};

use async_trait::async_trait;
use ceresdbproto::storage::{Route, RouteRequest as RouteRequestPb};
pub use cluster_based::ClusterBasedRouter;
use macros::define_result;
use meta_client::types::TableInfo;
pub use rule_based::{RuleBasedRouter, RuleList};
use serde::{Deserialize, Serialize};
use snafu::{Backtrace, Snafu};
use time_ext::ReadableDuration;

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
    async fn fetch_table_info(&self, schema: &str, table: &str) -> Result<Option<TableInfo>>;
}

pub struct RouteRequest {
    pub route_with_cache: bool,
    pub inner: RouteRequestPb,
}

impl RouteRequest {
    pub fn new(request: RouteRequestPb, route_with_cache: bool) -> Self {
        Self {
            route_with_cache,
            inner: request,
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct RouteCacheConfig {
    /// Enable route cache, default false.
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
