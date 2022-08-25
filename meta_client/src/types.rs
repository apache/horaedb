// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

use std::collections::HashMap;

use ceresdbproto_deps::ceresdbproto::{
    cluster,
    meta_service::{self, NodeHeartbeatResponse_oneof_cmd},
};
use common_types::{
    schema::{SchemaId, SchemaName},
    table::TableId,
};
use common_util::config::ReadableDuration;
use protobuf::RepeatedField;
use serde_derive::Deserialize;

pub type ShardId = u32;

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
pub struct AllocTableIdRequest {
    pub schema_name: String,
    pub name: String,
}

#[derive(Debug)]
pub struct AllocTableIdResponse {
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
    fn from(mut pb_table_info: meta_service::TableInfo) -> Self {
        TableInfo {
            id: pb_table_info.get_id(),
            name: pb_table_info.take_name(),
            schema_id: pb_table_info.get_schema_id(),
            schema_name: pb_table_info.take_schema_name(),
        }
    }
}

#[derive(Clone, Debug)]
pub struct ShardTables {
    pub role: ShardRole,
    pub tables: Vec<TableInfo>,
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

#[derive(Debug)]
pub struct NodeHeartbeatResponse {
    pub timestamp: u64,
    pub action_cmd: Option<ActionCmd>,
}

#[derive(Debug, Clone)]
pub struct ShardInfo {
    pub shard_id: ShardId,
    pub role: ShardRole,
}

impl ShardInfo {
    #[inline]
    pub fn is_leader(&self) -> bool {
        self.role == ShardRole::LEADER
    }
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum ShardRole {
    LEADER,
    FOLLOWER,
}

// TODO: now some commands are empty and fill the concrete information into
// them.
#[derive(Debug, Clone)]
pub enum ActionCmd {
    MetaNoneCmd(NoneCmd),
    MetaOpenCmd(OpenCmd),
    MetaSplitCmd(SplitCmd),
    MetaCloseCmd(CloseCmd),
    MetaChangeRoleCmd(ChangeRoleCmd),

    CreateTableCmd(CreateTableCmd),
    DropTableCmd(DropTableCmd),
}

#[derive(Debug, Clone)]
pub struct NoneCmd {}

#[derive(Debug, Clone)]
pub struct OpenCmd {
    pub shard_ids: Vec<ShardId>,
}

#[derive(Debug, Clone)]
pub struct SplitCmd {}

#[derive(Debug, Clone)]
pub struct CloseCmd {
    pub shard_ids: Vec<ShardId>,
}

#[derive(Debug, Clone)]
pub struct CreateTableCmd {
    pub schema_name: String,
    pub name: String,
    pub shard_id: ShardId,
    pub schema_id: SchemaId,
    pub id: TableId,
}

#[derive(Debug, Clone)]
pub struct DropTableCmd {
    pub schema_name: String,
    pub name: String,
}

#[derive(Debug, Clone)]
pub struct ChangeRoleCmd {}

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
        let mut pb_node_info = meta_service::NodeInfo::new();
        pb_node_info.set_endpoint(node_info.node_meta_info.endpoint());
        pb_node_info.set_zone(node_info.node_meta_info.zone);
        pb_node_info.set_binary_version(node_info.node_meta_info.binary_version);
        pb_node_info.set_shard_infos(RepeatedField::from_iter(
            node_info.shards_info.into_iter().map(|v| v.into()),
        ));
        pb_node_info
    }
}

impl From<ShardInfo> for meta_service::ShardInfo {
    fn from(shard_info: ShardInfo) -> Self {
        let mut pb_shard_info = meta_service::ShardInfo::new();
        pb_shard_info.set_shard_id(shard_info.shard_id);
        pb_shard_info.set_role(shard_info.role.into());
        pb_shard_info
    }
}

impl From<meta_service::ShardInfo> for ShardInfo {
    fn from(pb_shard_info: meta_service::ShardInfo) -> Self {
        ShardInfo {
            shard_id: pb_shard_info.shard_id,
            role: pb_shard_info.role.into(),
        }
    }
}

impl From<ShardRole> for cluster::ShardRole {
    fn from(shard_role: ShardRole) -> Self {
        match shard_role {
            ShardRole::LEADER => cluster::ShardRole::LEADER,
            ShardRole::FOLLOWER => cluster::ShardRole::FOLLOWER,
        }
    }
}

impl From<cluster::ShardRole> for ShardRole {
    fn from(pb_role: cluster::ShardRole) -> Self {
        match pb_role {
            cluster::ShardRole::LEADER => ShardRole::LEADER,
            cluster::ShardRole::FOLLOWER => ShardRole::FOLLOWER,
        }
    }
}

impl From<meta_service::NodeHeartbeatResponse> for NodeHeartbeatResponse {
    fn from(pb_resp: meta_service::NodeHeartbeatResponse) -> Self {
        let timestamp = pb_resp.get_timestamp();
        NodeHeartbeatResponse {
            timestamp,
            action_cmd: pb_resp.cmd.map(|v| v.into()),
        }
    }
}

impl From<NodeHeartbeatResponse_oneof_cmd> for ActionCmd {
    fn from(pb_cmd: NodeHeartbeatResponse_oneof_cmd) -> Self {
        match pb_cmd {
            NodeHeartbeatResponse_oneof_cmd::none_cmd(_) => ActionCmd::MetaNoneCmd(NoneCmd {}),
            NodeHeartbeatResponse_oneof_cmd::open_cmd(v) => ActionCmd::MetaOpenCmd(v.into()),
            NodeHeartbeatResponse_oneof_cmd::split_cmd(v) => ActionCmd::MetaSplitCmd(v.into()),
            NodeHeartbeatResponse_oneof_cmd::close_cmd(v) => ActionCmd::MetaCloseCmd(v.into()),
            NodeHeartbeatResponse_oneof_cmd::change_role_cmd(v) => {
                ActionCmd::MetaChangeRoleCmd(v.into())
            }
        }
    }
}

impl From<meta_service::NoneCmd> for NoneCmd {
    fn from(_: meta_service::NoneCmd) -> Self {
        Self {}
    }
}

impl From<meta_service::OpenCmd> for OpenCmd {
    fn from(mut pb_cmd: meta_service::OpenCmd) -> Self {
        Self {
            shard_ids: pb_cmd.take_shard_ids(),
        }
    }
}

impl From<meta_service::SplitCmd> for SplitCmd {
    fn from(_: meta_service::SplitCmd) -> Self {
        Self {}
    }
}

impl From<meta_service::CloseCmd> for CloseCmd {
    fn from(mut pb_cmd: meta_service::CloseCmd) -> Self {
        Self {
            shard_ids: pb_cmd.take_shard_ids(),
        }
    }
}

impl From<meta_service::ChangeRoleCmd> for ChangeRoleCmd {
    fn from(_: meta_service::ChangeRoleCmd) -> Self {
        Self {}
    }
}

impl From<GetShardTablesRequest> for meta_service::GetShardTablesRequest {
    fn from(req: GetShardTablesRequest) -> Self {
        let mut pb_req = meta_service::GetShardTablesRequest::new();
        pb_req.set_shard_ids(req.shard_ids);
        pb_req
    }
}

impl From<meta_service::GetShardTablesResponse> for GetShardTablesResponse {
    fn from(mut pb_resp: meta_service::GetShardTablesResponse) -> Self {
        Self {
            shard_tables: pb_resp
                .take_shard_tables()
                .into_iter()
                .map(|(k, v)| (k, v.into()))
                .collect(),
        }
    }
}

impl From<meta_service::ShardTables> for ShardTables {
    fn from(mut pb_shard_tables: meta_service::ShardTables) -> Self {
        Self {
            role: pb_shard_tables.get_role().into(),
            tables: pb_shard_tables
                .take_tables()
                .into_iter()
                .map(|v| v.into())
                .collect(),
        }
    }
}

impl From<RequestHeader> for meta_service::RequestHeader {
    fn from(req: RequestHeader) -> Self {
        let mut pb_header = meta_service::RequestHeader::new();
        pb_header.set_node(req.node);
        pb_header.set_cluster_name(req.cluster_name);
        pb_header
    }
}

impl From<AllocSchemaIdRequest> for meta_service::AllocSchemaIdRequest {
    fn from(req: AllocSchemaIdRequest) -> Self {
        let mut pb_req = meta_service::AllocSchemaIdRequest::new();
        pb_req.set_name(req.name);
        pb_req
    }
}

impl From<meta_service::AllocSchemaIdResponse> for AllocSchemaIdResponse {
    fn from(mut pb_resp: meta_service::AllocSchemaIdResponse) -> Self {
        Self {
            name: pb_resp.take_name(),
            id: pb_resp.get_id(),
        }
    }
}

impl From<AllocTableIdRequest> for meta_service::AllocTableIdRequest {
    fn from(req: AllocTableIdRequest) -> Self {
        let mut pb_req = meta_service::AllocTableIdRequest::new();
        pb_req.set_schema_name(req.schema_name);
        pb_req.set_name(req.name);
        pb_req
    }
}

impl From<meta_service::AllocTableIdResponse> for AllocTableIdResponse {
    fn from(mut pb_resp: meta_service::AllocTableIdResponse) -> Self {
        Self {
            schema_name: pb_resp.take_schema_name(),
            name: pb_resp.take_name(),
            shard_id: pb_resp.get_shard_id(),
            schema_id: pb_resp.get_schema_id(),
            id: pb_resp.get_id(),
        }
    }
}

impl From<DropTableRequest> for meta_service::DropTableRequest {
    fn from(req: DropTableRequest) -> Self {
        let mut pb = meta_service::DropTableRequest::new();
        pb.set_schema_name(req.schema_name);
        pb.set_name(req.name);
        pb.set_id(req.id);
        pb
    }
}

#[derive(Debug, Clone)]
pub struct RouteTablesRequest {
    pub schema_name: SchemaName,
    pub table_names: Vec<String>,
}

#[derive(Debug, Clone)]
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

impl From<RouteTablesRequest> for meta_service::RouteTablesRequest {
    fn from(req: RouteTablesRequest) -> Self {
        let mut pb_req = meta_service::RouteTablesRequest::default();
        pb_req.set_schema_name(req.schema_name);
        pb_req.set_table_names(RepeatedField::from(req.table_names));
        pb_req
    }
}

impl From<meta_service::NodeShard> for NodeShard {
    fn from(mut pb: meta_service::NodeShard) -> Self {
        NodeShard {
            endpoint: pb.take_endpoint(),
            shard_info: ShardInfo::from(pb.take_shard_info()),
        }
    }
}

impl From<meta_service::RouteEntry> for RouteEntry {
    fn from(mut pb_entry: meta_service::RouteEntry) -> Self {
        let node_shards: Vec<_> = pb_entry
            .take_node_shards()
            .into_iter()
            .map(NodeShard::from)
            .collect();

        RouteEntry {
            table: TableInfo::from(pb_entry.take_table()),
            node_shards,
        }
    }
}

impl From<meta_service::RouteTablesResponse> for RouteTablesResponse {
    fn from(mut pb_resp: meta_service::RouteTablesResponse) -> Self {
        let entries: HashMap<_, _> = pb_resp
            .take_entries()
            .into_iter()
            .map(|(table_name, entry)| (table_name, RouteEntry::from(entry)))
            .collect();

        RouteTablesResponse {
            cluster_topology_version: pb_resp.cluster_topology_version,
            entries,
        }
    }
}
