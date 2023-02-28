// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

//! A router based on rules.

use std::collections::HashMap;

use async_trait::async_trait;
use ceresdbproto::storage::{self, Route, RouteRequest};
use cluster::config::SchemaConfig;
use log::info;
use meta_client::types::ShardId;
use serde::Deserialize;
use snafu::{ensure, OptionExt};

use crate::{endpoint::Endpoint, hash, Result, RouteNotFound, Router, ShardNotFound};

pub type ShardNodes = HashMap<ShardId, Endpoint>;

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

    fn maybe_route_by_rule(table: &str, rule_list: &RuleList) -> Option<ShardId> {
        for prefix_rule in &rule_list.prefix_rules {
            if table.starts_with(&prefix_rule.prefix) {
                return Some(prefix_rule.shard);
            }
        }

        if let Some(hash_rule) = rule_list.hash_rules.get(0) {
            let total_shards = hash_rule.shards.len();
            let hash_value = hash::hash_table(table);
            let index = hash_value as usize % total_shards;

            return Some(hash_rule.shards[index]);
        }

        None
    }

    #[inline]
    fn route_by_hash(table: &str, total_shards: usize) -> ShardId {
        let hash_value = hash::hash_table(table);
        (hash_value as usize % total_shards) as ShardId
    }

    fn route_table(table: &str, rule_list_opt: Option<&RuleList>, total_shards: usize) -> ShardId {
        if let Some(rule_list) = rule_list_opt {
            if let Some(shard_id) = Self::maybe_route_by_rule(table, rule_list) {
                return shard_id;
            }
        }

        // Fallback to hash route rule.
        Self::route_by_hash(table, total_shards)
    }
}

#[async_trait]
impl Router for RuleBasedRouter {
    async fn route(&self, req: RouteRequest) -> Result<Vec<Route>> {
        let req_ctx = req.context.unwrap();
        let schema = &req_ctx.database;
        if let Some(shard_nodes) = self.cluster_view.schema_shards.get(schema) {
            ensure!(!shard_nodes.is_empty(), RouteNotFound { schema });

            // Get rule list of this schema.
            let rule_list_opt = self.schema_rules.get(schema);

            // TODO(yingwen): Better way to get total shard number
            let total_shards = shard_nodes.len();
            let mut route_results = Vec::with_capacity(req.tables.len());
            for table in req.tables {
                let shard_id = Self::route_table(&table, rule_list_opt, total_shards);

                let endpoint = shard_nodes.get(&shard_id).with_context(|| ShardNotFound {
                    schema,
                    table: &table,
                })?;

                let pb_endpoint = storage::Endpoint::from(endpoint.clone());
                let route = Route {
                    table,
                    endpoint: Some(pb_endpoint),
                };
                route_results.push(route);
            }
            return Ok(route_results);
        }

        Ok(Vec::new())
    }
}
