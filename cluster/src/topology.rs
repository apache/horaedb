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
    id: SchemaId,
    config: SchemaConfig,
    /// The [RouteSlot] in the `route_slots` only can be `Exist` or `NotExist`.
    route_slots: HashMap<TableName, RouteSlot>,
}

#[derive(Debug, Default)]
pub struct SchemaTopologies {
    version: u64,
    topologies: HashMap<SchemaName, SchemaTopology>,
}

#[derive(Clone, Debug, Default)]
pub struct NodeTopology {
    pub version: u64,
    pub nodes: ClusterNodesRef,
}

#[derive(Debug, Default)]
pub struct ClusterTopology {
    schemas: Option<SchemaTopologies>,
    nodes: Option<NodeTopology>,
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

impl SchemaTopologies {
    fn route_tables(&self, schema_name: &str, tables: &[TableName]) -> RouteTablesResult {
        if let Some(schema_topology) = self.topologies.get(schema_name) {
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
    fn maybe_update_tables(
        &mut self,
        schema_name: &str,
        tables: HashMap<TableName, RouteSlot>,
        version: u64,
    ) -> bool {
        if ClusterTopology::is_outdated_version(self.version, version) {
            return false;
        }

        self.topologies
            .entry(schema_name.to_string())
            .or_insert_with(Default::default)
            .update_tables(tables);

        true
    }
}

impl NodeTopology {
    fn maybe_update_nodes(&mut self, nodes: ClusterNodesRef, version: u64) -> bool {
        if ClusterTopology::is_newer_version(self.version, version) {
            self.nodes = nodes;
            true
        } else {
            false
        }
    }
}

impl ClusterTopology {
    #[inline]
    fn is_outdated_version(current_version: u64, check_version: u64) -> bool {
        check_version < current_version
    }

    #[inline]
    fn is_newer_version(current_version: u64, check_version: u64) -> bool {
        check_version > current_version
    }

    pub fn nodes(&self) -> Option<NodeTopology> {
        self.nodes.clone()
    }

    /// Try to update the nodes topology of the cluster.
    ///
    /// If the provided version is not newer, then the update will be
    /// ignored.
    pub fn maybe_update_nodes(&mut self, nodes: ClusterNodesRef, version: u64) -> bool {
        if self.nodes.is_none() {
            let nodes = NodeTopology { version, nodes };
            self.nodes = Some(nodes);
            return true;
        }

        self.nodes
            .as_mut()
            .unwrap()
            .maybe_update_nodes(nodes, version)
    }
}

impl SchemaTopology {
    fn update_tables(&mut self, tables: HashMap<TableName, RouteSlot>) {
        for (table_name, slot) in tables {
            self.route_slots.insert(table_name, slot);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_outdated_version() {
        // One case is (current_version, check_version, is_outdated)
        let cases = [(1, 2, false), (1, 1, false), (1, 0, true)];
        for (current_version, check_version, is_outdated) in cases {
            assert_eq!(
                is_outdated,
                ClusterTopology::is_outdated_version(current_version, check_version)
            );
        }
    }

    #[test]
    fn test_newer_version() {
        // One case is (current_version, check_version, is_newer)
        let cases = [(1, 2, true), (1, 1, false), (1, 0, false)];
        for (current_version, check_version, is_newer) in cases {
            assert_eq!(
                is_newer,
                ClusterTopology::is_newer_version(current_version, check_version)
            );
        }
    }
}
