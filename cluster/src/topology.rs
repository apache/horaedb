// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

use std::collections::HashMap;

use common_types::{
    schema::{SchemaId, SchemaName},
    table::TableName,
};
use meta_client::types::{ClusterNodesRef, RouteEntry, RouteTablesResponse};

use crate::config::SchemaConfig;

/// RouteSlot is used to prevent cache penetration, that is to say, the
/// `NotExist` routing result of a table is also kept in the memory.
#[derive(Debug, Clone)]
pub enum RouteSlot {
    Exist(RouteEntry),
    NotExist,
}

#[derive(Debug, Default)]
struct SchemaTopology {
    #[allow(dead_code)]
    id: SchemaId,
    #[allow(dead_code)]
    config: SchemaConfig,
    /// The [RouteSlot] in the `route_slots` only can be `Exist` or `NotExist`.
    route_slots: HashMap<TableName, RouteSlot>,
}

#[derive(Debug, Default)]
pub struct ClusterTopology {
    version: u64,
    schema_topologies: HashMap<SchemaName, SchemaTopology>,
    nodes: Option<ClusterNodesRef>,
}

#[derive(Debug, Default, Clone)]
pub struct RouteTablesResult {
    pub version: u64,
    pub route_entries: HashMap<TableName, RouteEntry>,
    pub missing_tables: Vec<TableName>,
}

impl From<RouteTablesResult> for RouteTablesResponse {
    fn from(result: RouteTablesResult) -> Self {
        RouteTablesResponse {
            cluster_topology_version: result.version,
            entries: result.route_entries,
        }
    }
}

impl ClusterTopology {
    /// Any update on the topology should ensure the version is valid: the
    /// target version must be not older than the current version.
    pub fn is_outdated_version(&self, version: u64) -> bool {
        version >= self.version
    }

    pub fn route_tables(&self, schema_name: &str, tables: &[TableName]) -> RouteTablesResult {
        if let Some(schema_topology) = self.schema_topologies.get(schema_name) {
            let mut route_entries = HashMap::with_capacity(tables.len());
            let mut missing_tables = vec![];

            for table in tables {
                match schema_topology.route_slots.get(table) {
                    None => missing_tables.push(table.clone()),
                    Some(RouteSlot::Exist(route_entry)) => {
                        route_entries.insert(table.clone(), route_entry.clone());
                    }
                    Some(RouteSlot::NotExist) => (),
                };
            }

            return RouteTablesResult {
                version: self.version,
                route_entries,
                missing_tables,
            };
        }

        RouteTablesResult {
            version: self.version,
            route_entries: Default::default(),
            missing_tables: tables.to_vec(),
        }
    }

    /// Update the routing information into the topology if its version is
    /// valid.
    ///
    /// Return false if the version is outdated.
    pub fn update_tables(
        &mut self,
        schema_name: &str,
        tables: HashMap<TableName, RouteSlot>,
        version: u64,
    ) -> bool {
        if self.is_outdated_version(version) {
            return false;
        }

        self.schema_topologies
            .entry(schema_name.to_string())
            .or_insert_with(Default::default)
            .update_tables(tables);

        true
    }

    pub fn nodes(&self) -> Option<ClusterNodesRef> {
        self.nodes.clone()
    }

    pub fn update_nodes(&mut self, nodes: ClusterNodesRef, version: u64) -> bool {
        if self.is_outdated_version(version) {
            return false;
        }

        self.nodes = Some(nodes);

        true
    }

    pub fn version(&self) -> u64 {
        self.version
    }
}

impl SchemaTopology {
    fn update_tables(&mut self, tables: HashMap<TableName, RouteSlot>) {
        for (table_name, slot) in tables {
            self.route_slots.insert(table_name, slot);
        }
    }
}
