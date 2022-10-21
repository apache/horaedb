// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

use std::sync::Arc;

use async_trait::async_trait;
use ceresdbproto::storage::{Route, RouteRequest};

pub mod cluster_based;
pub(crate) mod hash;
pub mod rule_based;

pub use cluster_based::ClusterBasedRouter;
pub use rule_based::{RuleBasedRouter, RuleList};
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
    async fn route(&self, schema: &str, req: RouteRequest) -> Result<Vec<Route>>;
}
