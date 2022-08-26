// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

use std::sync::Arc;

use async_trait::async_trait;
use ceresdbproto_deps::ceresdbproto::storage::{Route, RouteRequest};

use crate::error::Result;

pub mod cluster_based;
pub(crate) mod hash;
pub mod rule_based;

pub use cluster_based::ClusterBasedRouter;
pub use rule_based::{RuleBasedRouter, RuleList};

pub type RouterRef = Arc<dyn Router + Sync + Send>;

#[async_trait]
pub trait Router {
    async fn route(&self, schema: &str, req: RouteRequest) -> Result<Vec<Route>>;
}
