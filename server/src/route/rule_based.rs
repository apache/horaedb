// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

//! A router based on rules.

use std::{
    collections::HashMap,
    hash::{Hash, Hasher},
};

use async_trait::async_trait;
use ceresdbproto_deps::ceresdbproto::storage::{Endpoint, Route, RouteRequest};
use cluster::topology::SchemaConfig;
use log::info;
use meta_client::types::ShardId;
use serde_derive::Deserialize;
use twox_hash::XxHash64;

use crate::{
    config::Node,
    error::{ErrNoCause, Result, StatusCode},
    route::Router,
};

/// Hash seed to build hasher. Modify the seed will result in different route
/// result!
const HASH_SEED: u64 = 0;

pub type ShardNodes = HashMap<ShardId, Node>;

#[derive(Clone, Debug, Default)]
pub struct ClusterView {
    pub schema_shards: HashMap<String, ShardNodes>,
    pub schema_configs: HashMap<String, SchemaConfig>,
}

#[derive(Clone, Debug, Deserialize)]
pub struct PrefixRule {
    /// Schema name of the prefix.
    pub schema: String,
    /// Prefix of the table name.
    pub prefix: String,
    /// The shard of matched tables.
    pub shard: ShardId,
}

#[derive(Clone, Debug, Deserialize)]
pub struct HashRule {
    /// Schema name of the prefix.
    pub schema: String,
    /// The shard list for hash rule.
    pub shards: Vec<ShardId>,
}

#[derive(Clone, Debug, Default, Deserialize)]
#[serde(default)]
pub struct RuleList {
    pub prefix_rules: Vec<PrefixRule>,
    pub hash_rules: Vec<HashRule>,
}

impl RuleList {
    pub fn split_by_schema(self) -> SchemaRules {
        let mut schema_rules = HashMap::new();

        for rule in self.prefix_rules {
            let rule_list = match schema_rules.get_mut(&rule.schema) {
                Some(v) => v,
                None => schema_rules
                    .entry(rule.schema.clone())
                    .or_insert_with(RuleList::default),
            };

            rule_list.prefix_rules.push(rule);
        }

        for rule in self.hash_rules {
            let rule_list = match schema_rules.get_mut(&rule.schema) {
                Some(v) => v,
                None => schema_rules
                    .entry(rule.schema.clone())
                    .or_insert_with(RuleList::default),
            };

            rule_list.hash_rules.push(rule);
        }

        schema_rules
    }
}

// Schema -> Rule list of the schema.
type SchemaRules = HashMap<String, RuleList>;

pub struct RuleBasedRouter {
    cluster_view: ClusterView,
    schema_rules: SchemaRules,
}

impl RuleBasedRouter {
    pub fn new(cluster_view: ClusterView, rules: RuleList) -> Self {
        let schema_rules = rules.split_by_schema();

        info!(
            "RuleBasedRouter init with rules, rules:{:?}, cluster_view:{:?}",
            schema_rules, cluster_view
        );

        Self {
            schema_rules,
            cluster_view,
        }
    }

    fn maybe_route_by_rule(metric: &str, rule_list: &RuleList) -> Option<ShardId> {
        for prefix_rule in &rule_list.prefix_rules {
            if metric.starts_with(&prefix_rule.prefix) {
                return Some(prefix_rule.shard);
            }
        }

        if let Some(hash_rule) = rule_list.hash_rules.get(0) {
            let total_shards = hash_rule.shards.len();
            let hash_value = hash_metric(metric);
            let index = hash_value as usize % total_shards;

            return Some(hash_rule.shards[index]);
        }

        None
    }

    #[inline]
    fn route_by_hash(metric: &str, total_shards: usize) -> ShardId {
        let hash_value = hash_metric(metric);
        (hash_value as usize % total_shards) as ShardId
    }

    fn route_metric(
        metric: &str,
        rule_list_opt: Option<&RuleList>,
        total_shards: usize,
    ) -> ShardId {
        if let Some(rule_list) = rule_list_opt {
            if let Some(shard_id) = Self::maybe_route_by_rule(metric, rule_list) {
                return shard_id;
            }
        }

        // Fallback to hash route rule.
        Self::route_by_hash(metric, total_shards)
    }
}

#[async_trait]
impl Router for RuleBasedRouter {
    async fn route(&self, schema: &str, req: RouteRequest) -> Result<Vec<Route>> {
        if let Some(shard_nodes) = self.cluster_view.schema_shards.get(schema) {
            if shard_nodes.is_empty() {
                return ErrNoCause {
                    code: StatusCode::NotFound,
                    msg: "No valid shard is found",
                }
                .fail();
            }

            // Get rule list of this schema.
            let rule_list_opt = self.schema_rules.get(schema);

            // TODO(yingwen): Better way to get total shard number
            let total_shards = shard_nodes.len();
            let mut route_vec = Vec::with_capacity(req.metrics.len());
            for metric in req.metrics {
                let mut route = Route::new();
                route.set_metric(metric);

                let shard_id = Self::route_metric(route.get_metric(), rule_list_opt, total_shards);

                let mut endpoint = Endpoint::new();
                if let Some(node) = shard_nodes.get(&shard_id) {
                    endpoint.set_ip(node.addr.clone());
                    endpoint.set_port(node.port as u32);
                } else {
                    return ErrNoCause {
                        code: StatusCode::NotFound,
                        msg: format!(
                            "Shard not found, metric:{}, shard_id:{}",
                            route.get_metric(),
                            shard_id
                        ),
                    }
                    .fail();
                }

                route.set_endpoint(endpoint);
                route_vec.push(route);
            }
            return Ok(route_vec);
        }

        Ok(Vec::new())
    }
}

fn hash_metric(metric: &str) -> u64 {
    let mut hasher = XxHash64::with_seed(HASH_SEED);
    metric.hash(&mut hasher);
    hasher.finish()
}
