// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

use std::{collections::HashMap, sync::Arc};

use ceresdbproto::{cluster, meta_service};
use common_types::{
    schema::{SchemaId, SchemaName},
    table::{TableId, TableName},
};
use common_util::config::ReadableDuration;
use serde_derive::Deserialize;
use snafu::OptionExt;

use crate::{Error, MissingShardInfo, MissingTableInfo, Result};

pub type ShardId = u32;
pub type ClusterNodesRef = Arc<Vec<NodeShard>>;

#[derive(Debug, Clone)]
pub struct RequestHeader {
    pub node: String,
    pub cluster_name: String,
}

#[derive(Debug)]
pub struct AllocSchemaIdRequest {
    pub name: String,
}

#[derive(Debug)]
pub struct AllocSchemaIdResponse {
    pub name: String,
    pub id: SchemaId,
}

#[derive(Debug)]
pub struct CreateTableRequest {
    pub schema_name: String,
    pub name: String,
    pub create_sql: String,
}

#[derive(Debug)]
pub struct CreateTableResponse {
    pub schema_name: String,
    pub name: String,
    pub shard_id: ShardId,
    pub schema_id: SchemaId,
    pub id: TableId,
}

#[derive(Debug, Clone)]
pub struct DropTableRequest {
    pub schema_name: String,
    pub name: String,
    pub id: TableId,
}

#[derive(Clone, Debug)]
pub struct GetShardTablesRequest {
    pub shard_ids: Vec<ShardId>,
}

#[derive(Clone, Debug)]
pub struct GetShardTablesResponse {
    pub shard_tables: HashMap<ShardId, ShardTables>,
}

#[derive(Clone, Debug)]
pub struct TableInfo {
    pub id: TableId,
    pub name: String,
    pub schema_id: SchemaId,
    pub schema_name: String,
}

impl From<meta_service::TableInfo> for TableInfo {
    fn from(pb_table_info: meta_service::TableInfo) -> Self {
        TableInfo {
            id: pb_table_info.id,
            name: pb_table_info.name,
            schema_id: pb_table_info.schema_id,
            schema_name: pb_table_info.schema_name,
        }
    }
}

#[derive(Clone, Debug)]
pub struct ShardTables {
    pub role: ShardRole,
    pub tables: Vec<TableInfo>,
    pub version: u64,
}

#[derive(Debug, Default, Clone, Deserialize)]
#[serde(default)]
pub struct NodeMetaInfo {
    pub addr: String,
    pub port: u16,
    pub zone: String,
    pub idc: String,
    pub binary_version: String,
}

impl NodeMetaInfo {
    pub fn endpoint(&self) -> String {
        format!("{}:{}", self.addr, self.port)
    }
}

#[derive(Debug, Clone)]
pub struct NodeInfo {
    pub node_meta_info: NodeMetaInfo,
    pub shards_info: Vec<ShardInfo>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShardInfo {
    pub shard_id: ShardId,
    pub role: ShardRole,
    pub version: u64,
}

impl ShardInfo {
    #[inline]
    pub fn is_leader(&self) -> bool {
        self.role == ShardRole::Leader
    }
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum ShardRole {
    Leader,
    Follower,
    PendingLeader,
    PendingFollower,
}

#[derive(Debug, Deserialize, Clone)]
#[serde(default)]
pub struct MetaClientConfig {
    pub cluster_name: String,
    pub meta_addr: String,
    pub meta_members_url: String,
    pub lease: ReadableDuration,
    pub timeout: ReadableDuration,
    pub cq_count: usize,
}

impl Default for MetaClientConfig {
    fn default() -> Self {
        Self {
            cluster_name: String::new(),
            meta_addr: "127.0.0.1:8080".to_string(),
            meta_members_url: "ceresmeta/members".to_string(),
            lease: ReadableDuration::secs(10),
            timeout: ReadableDuration::secs(5),
            cq_count: 8,
        }
    }
}

impl From<NodeInfo> for meta_service::NodeInfo {
    fn from(node_info: NodeInfo) -> Self {
        let shard_infos = node_info
            .shards_info
            .into_iter()
            .map(meta_service::ShardInfo::from)
            .collect();

        Self {
            endpoint: node_info.node_meta_info.endpoint(),
            zone: node_info.node_meta_info.zone,
            binary_version: node_info.node_meta_info.binary_version,
            shard_infos,
            lease: 0,
        }
    }
}

impl From<ShardInfo> for meta_service::ShardInfo {
    fn from(shard_info: ShardInfo) -> Self {
        let role = cluster::ShardRole::from(shard_info.role);

        Self {
            shard_id: shard_info.shard_id,
            role: role as i32,
            version: 0,
        }
    }
}

impl From<meta_service::ShardInfo> for ShardInfo {
    fn from(pb_shard_info: meta_service::ShardInfo) -> Self {
        ShardInfo {
            shard_id: pb_shard_info.shard_id,
            role: pb_shard_info.role().into(),
            version: pb_shard_info.version,
        }
    }
}

impl From<ShardRole> for cluster::ShardRole {
    fn from(shard_role: ShardRole) -> Self {
        match shard_role {
            ShardRole::Leader => cluster::ShardRole::Leader,
            ShardRole::Follower => cluster::ShardRole::Follower,
            ShardRole::PendingLeader => cluster::ShardRole::PendingLeader,
            ShardRole::PendingFollower => cluster::ShardRole::PendingFollower,
        }
    }
}

impl From<cluster::ShardRole> for ShardRole {
    fn from(pb_role: cluster::ShardRole) -> Self {
        match pb_role {
            cluster::ShardRole::Leader => ShardRole::Leader,
            cluster::ShardRole::Follower => ShardRole::Follower,
            cluster::ShardRole::PendingLeader => ShardRole::PendingLeader,
            cluster::ShardRole::PendingFollower => ShardRole::PendingFollower,
        }
    }
}

impl From<GetShardTablesRequest> for meta_service::GetShardTablesRequest {
    fn from(req: GetShardTablesRequest) -> Self {
        Self {
            header: None,
            shard_ids: req.shard_ids,
        }
    }
}

impl TryFrom<meta_service::GetShardTablesResponse> for GetShardTablesResponse {
    type Error = Error;

    fn try_from(pb_resp: meta_service::GetShardTablesResponse) -> Result<Self> {
        let shard_tables = pb_resp
            .shard_tables
            .into_iter()
            .map(|(k, v)| Ok((k, ShardTables::try_from(v)?)))
            .collect::<Result<HashMap<_, _>>>()?;

        Ok(Self { shard_tables })
    }
}

impl TryFrom<meta_service::ShardTables> for ShardTables {
    type Error = Error;

    fn try_from(pb_shard_tables: meta_service::ShardTables) -> Result<Self> {
        let shard_info = pb_shard_tables
            .shard_info
            .with_context(|| MissingShardInfo {
                msg: "in meta_service::ShardTables",
            })?;
        Ok(Self {
            role: ShardRole::from(shard_info.role()),
            tables: pb_shard_tables.tables.into_iter().map(Into::into).collect(),
            version: shard_info.version,
        })
    }
}

impl From<RequestHeader> for meta_service::RequestHeader {
    fn from(req: RequestHeader) -> Self {
        Self {
            node: req.node,
            cluster_name: req.cluster_name,
        }
    }
}

impl From<AllocSchemaIdRequest> for meta_service::AllocSchemaIdRequest {
    fn from(req: AllocSchemaIdRequest) -> Self {
        Self {
            header: None,
            name: req.name,
        }
    }
}

impl From<meta_service::AllocSchemaIdResponse> for AllocSchemaIdResponse {
    fn from(pb_resp: meta_service::AllocSchemaIdResponse) -> Self {
        Self {
            name: pb_resp.name,
            id: pb_resp.id,
        }
    }
}

impl From<CreateTableRequest> for meta_service::CreateTableRequest {
    fn from(req: CreateTableRequest) -> Self {
        Self {
            header: None,
            schema_name: req.schema_name,
            name: req.name,
            create_sql: req.create_sql,
        }
    }
}

impl From<meta_service::CreateTableResponse> for CreateTableResponse {
    fn from(pb_resp: meta_service::CreateTableResponse) -> Self {
        Self {
            schema_name: pb_resp.schema_name,
            name: pb_resp.name,
            shard_id: pb_resp.shard_id,
            schema_id: pb_resp.schema_id,
            id: pb_resp.id,
        }
    }
}

impl From<DropTableRequest> for meta_service::DropTableRequest {
    fn from(req: DropTableRequest) -> Self {
        Self {
            header: None,
            schema_name: req.schema_name,
            name: req.name,
            id: req.id,
        }
    }
}

#[derive(Debug, Clone)]
pub struct RouteTablesRequest {
    pub schema_name: SchemaName,
    pub table_names: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NodeShard {
    pub endpoint: String,
    pub shard_info: ShardInfo,
}

#[derive(Debug, Clone)]
pub struct RouteEntry {
    pub table: TableInfo,
    pub node_shards: Vec<NodeShard>,
}

#[derive(Debug, Clone)]
pub struct RouteTablesResponse {
    pub cluster_topology_version: u64,
    pub entries: HashMap<String, RouteEntry>,
}

impl RouteTablesResponse {
    pub fn contains_all_tables(&self, queried_tables: &[TableName]) -> bool {
        queried_tables
            .iter()
            .all(|table_name| self.entries.contains_key(table_name))
    }
}

impl From<RouteTablesRequest> for meta_service::RouteTablesRequest {
    fn from(req: RouteTablesRequest) -> Self {
        Self {
            header: None,
            schema_name: req.schema_name,
            table_names: req.table_names,
        }
    }
}

impl TryFrom<meta_service::NodeShard> for NodeShard {
    type Error = Error;

    fn try_from(pb: meta_service::NodeShard) -> Result<Self> {
        let pb_shard_info = pb.shard_info.with_context(|| MissingShardInfo {
            msg: "in meta_service::NodeShard",
        })?;
        Ok(NodeShard {
            endpoint: pb.endpoint,
            shard_info: ShardInfo::from(pb_shard_info),
        })
    }
}

impl TryFrom<meta_service::RouteEntry> for RouteEntry {
    type Error = Error;

    fn try_from(pb_entry: meta_service::RouteEntry) -> Result<Self> {
        let mut node_shards = Vec::with_capacity(pb_entry.node_shards.len());
        for pb_node_shard in pb_entry.node_shards {
            let node_shard = NodeShard::try_from(pb_node_shard)?;
            node_shards.push(node_shard);
        }

        let table_info = pb_entry.table.context(MissingTableInfo)?;
        Ok(RouteEntry {
            table: TableInfo::from(table_info),
            node_shards,
        })
    }
}

impl TryFrom<meta_service::RouteTablesResponse> for RouteTablesResponse {
    type Error = Error;

    fn try_from(pb_resp: meta_service::RouteTablesResponse) -> Result<Self> {
        let mut entries = HashMap::with_capacity(pb_resp.entries.len());
        for (table_name, entry) in pb_resp.entries {
            let route_entry = RouteEntry::try_from(entry)?;
            entries.insert(table_name, route_entry);
        }

        Ok(RouteTablesResponse {
            cluster_topology_version: pb_resp.cluster_topology_version,
            entries,
        })
    }
}

#[derive(Debug, Clone, Default)]
pub struct GetNodesRequest {}

pub struct GetNodesResponse {
    pub cluster_topology_version: u64,
    pub node_shards: Vec<NodeShard>,
}

impl From<GetNodesRequest> for meta_service::GetNodesRequest {
    fn from(_: GetNodesRequest) -> Self {
        meta_service::GetNodesRequest::default()
    }
}

impl TryFrom<meta_service::GetNodesResponse> for GetNodesResponse {
    type Error = Error;

    fn try_from(pb_resp: meta_service::GetNodesResponse) -> Result<Self> {
        let mut node_shards = Vec::with_capacity(pb_resp.node_shards.len());
        for node_shard in pb_resp.node_shards {
            node_shards.push(NodeShard::try_from(node_shard)?);
        }

        Ok(GetNodesResponse {
            cluster_topology_version: pb_resp.cluster_topology_version,
            node_shards,
        })
    }
}
