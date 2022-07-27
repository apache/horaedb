// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

use std::collections::HashMap;

use ceresdbproto_deps::ceresdbproto::{
    cluster::ShardRole as PbShardRole,
    common::ResponseHeader as PbResponseHeader,
    meta_service::{
        AllocSchemaIdRequest as PbAllocSchemaIdRequest,
        AllocSchemaIdResponse as PbAllocSchemaIdResponse,
        AllocTableIdRequest as PbAllocTableIdRequest,
        AllocTableIdResponse as PbAllocTableIdResponse, ChangeRoleCmd as PbChangeRoleCmd,
        CloseCmd as PbCloseCmd, DropTableRequest as PbDropTableRequest,
        DropTableResponse as PbDropTableResponse, GetTablesRequest as PbGetTablesRequest,
        GetTablesResponse as PbGetTablesResponse, NodeHeartbeatResponse as PbNodeHeartbeatResponse,
        NodeHeartbeatResponse_oneof_cmd, NodeInfo as PbNodeInfo, NoneCmd as PbNoneCmd,
        OpenCmd as PbOpenCmd, RequestHeader as PbRequestHeader, ShardInfo as PbShardInfo,
        ShardTables as PbShardTables, SplitCmd as PbSplitCmd, TableInfo as PbTableInfo,
    },
};
use common_util::config::ReadableDuration;
use serde_derive::Deserialize;

pub type TableId = u64;
pub type ShardId = u32;
pub type SchemaId = u32;

#[derive(Debug, Clone)]
pub struct RequestHeader {
    pub node: String,
    pub cluster_name: String,
}

#[derive(Debug, Clone)]
pub struct ResponseHeader {
    pub code: u32,
    pub err_msg: String,
}

impl ResponseHeader {
    #[inline]
    pub fn is_success(&self) -> bool {
        self.code == 0
    }
}

#[derive(Debug)]
pub struct AllocSchemaIdRequest {
    pub name: String,
}

#[derive(Debug)]
pub struct AllocSchemaIdResponse {
    pub header: ResponseHeader,

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
    pub header: ResponseHeader,

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

#[derive(Debug)]
pub struct DropTableResponse {
    pub header: ResponseHeader,
}

#[derive(Clone, Debug)]
pub struct GetTablesRequest {
    pub shard_ids: Vec<ShardId>,
}

#[derive(Clone, Debug)]
pub struct GetTablesResponse {
    pub header: ResponseHeader,

    pub tables_map: HashMap<ShardId, ShardTables>,
}

#[derive(Clone, Debug)]
pub struct TableInfo {
    pub id: TableId,
    pub name: String,
    pub schema_id: SchemaId,
    pub schema_name: String,
}

#[derive(Clone, Debug)]
pub struct ShardTables {
    pub role: ShardRole,
    pub tables: Vec<TableInfo>,
}

#[derive(Debug, Clone, Default, Deserialize)]
pub struct Node {
    pub addr: String,
    pub port: u16,
}

impl ToString for Node {
    fn to_string(&self) -> String {
        format!("{}:{}", self.addr, self.port)
    }
}

#[derive(Debug, Default, Clone, Deserialize)]
pub struct NodeMetaInfo {
    pub node: Node,
    pub zone: String,
    pub idc: String,
    pub binary_version: String,
}

#[derive(Debug, Clone)]
pub struct NodeInfo {
    pub node_meta_info: NodeMetaInfo,
    pub shards_info: Vec<ShardInfo>,
}

#[derive(Debug)]
pub struct NodeHeartbeatResponse {
    pub header: ResponseHeader,

    pub timestamp: u64,
    pub action_cmd: Option<ActionCmd>,
}

#[derive(Debug, Clone)]
pub struct ShardInfo {
    pub shard_id: ShardId,
    pub role: ShardRole,
}

#[derive(Debug, Copy, Clone)]
pub enum ShardRole {
    LEADER,
    FOLLOWER,
}

// TODO: now some commands are empty and fill the concret inforamtion into them.
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
            meta_addr: "http://127.0.0.1:8080".to_string(),
            meta_members_url: "ceresmeta/members".to_string(),
            lease: ReadableDuration::secs(10),
            timeout: ReadableDuration::secs(5),
            cq_count: 8,
        }
    }
}

impl From<NodeInfo> for PbNodeInfo {
    fn from(node_info: NodeInfo) -> Self {
        let mut pb_node_info = PbNodeInfo::new();
        pb_node_info.set_node(node_info.node_meta_info.node.to_string());
        pb_node_info.set_zone(node_info.node_meta_info.zone);
        pb_node_info.set_binary_version(node_info.node_meta_info.binary_version);
        pb_node_info.set_shardsInfo(protobuf::RepeatedField::from_vec(
            node_info
                .shards_info
                .into_iter()
                .map(|v| v.into())
                .collect(),
        ));
        pb_node_info
    }
}

impl From<ShardInfo> for PbShardInfo {
    fn from(shard_info: ShardInfo) -> Self {
        let mut pb_shard_info = PbShardInfo::new();
        pb_shard_info.set_shard_id(shard_info.shard_id);
        pb_shard_info.set_role(shard_info.role.into());
        pb_shard_info
    }
}

impl From<ShardRole> for PbShardRole {
    fn from(shard_role: ShardRole) -> Self {
        match shard_role {
            ShardRole::LEADER => PbShardRole::LEADER,
            ShardRole::FOLLOWER => PbShardRole::FOLLOWER,
        }
    }
}

impl From<PbShardRole> for ShardRole {
    fn from(pb: PbShardRole) -> Self {
        match pb {
            PbShardRole::LEADER => ShardRole::LEADER,
            PbShardRole::FOLLOWER => ShardRole::FOLLOWER,
        }
    }
}

impl From<PbNodeHeartbeatResponse> for NodeHeartbeatResponse {
    fn from(mut pb: PbNodeHeartbeatResponse) -> Self {
        let timestamp = pb.get_timestamp();
        NodeHeartbeatResponse {
            header: pb.take_header().into(),
            timestamp,
            action_cmd: pb.cmd.map(|v| v.into()),
        }
    }
}

impl From<NodeHeartbeatResponse_oneof_cmd> for ActionCmd {
    fn from(pb: NodeHeartbeatResponse_oneof_cmd) -> Self {
        match pb {
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

impl From<PbNoneCmd> for NoneCmd {
    fn from(_pb: PbNoneCmd) -> Self {
        Self {}
    }
}

impl From<PbOpenCmd> for OpenCmd {
    fn from(mut pb: PbOpenCmd) -> Self {
        Self {
            shard_ids: pb.take_shard_ids(),
        }
    }
}

impl From<PbSplitCmd> for SplitCmd {
    fn from(_pb: PbSplitCmd) -> Self {
        Self {}
    }
}

impl From<PbCloseCmd> for CloseCmd {
    fn from(mut pb: PbCloseCmd) -> Self {
        Self {
            shard_ids: pb.take_shard_ids(),
        }
    }
}

impl From<PbChangeRoleCmd> for ChangeRoleCmd {
    fn from(_pb: PbChangeRoleCmd) -> Self {
        Self {}
    }
}

impl From<GetTablesRequest> for PbGetTablesRequest {
    fn from(req: GetTablesRequest) -> Self {
        let mut pb = PbGetTablesRequest::new();
        pb.set_shard_id(req.shard_ids);
        pb
    }
}

impl From<PbGetTablesResponse> for GetTablesResponse {
    fn from(mut pb: PbGetTablesResponse) -> Self {
        Self {
            header: pb.take_header().into(),
            tables_map: pb
                .take_tables_map()
                .into_iter()
                .map(|(k, v)| (k, v.into()))
                .collect(),
        }
    }
}

impl From<PbShardTables> for ShardTables {
    fn from(mut pb: PbShardTables) -> Self {
        Self {
            role: pb.get_role().into(),
            tables: pb.take_tables().into_iter().map(|v| v.into()).collect(),
        }
    }
}

impl From<PbTableInfo> for TableInfo {
    fn from(mut pb: PbTableInfo) -> Self {
        TableInfo {
            id: pb.get_id(),
            name: pb.take_name(),
            schema_id: pb.get_schema_id(),
            schema_name: pb.take_schema_name(),
        }
    }
}

impl From<RequestHeader> for PbRequestHeader {
    fn from(req: RequestHeader) -> Self {
        let mut pb = PbRequestHeader::new();
        pb.set_node(req.node);
        pb.set_cluster_name(req.cluster_name);
        pb
    }
}

impl From<PbResponseHeader> for ResponseHeader {
    fn from(mut pb: PbResponseHeader) -> Self {
        Self {
            code: pb.get_code(),
            err_msg: pb.take_error(),
        }
    }
}

impl From<AllocSchemaIdRequest> for PbAllocSchemaIdRequest {
    fn from(req: AllocSchemaIdRequest) -> Self {
        let mut pb = PbAllocSchemaIdRequest::new();
        pb.set_name(req.name);
        pb
    }
}

impl From<PbAllocSchemaIdResponse> for AllocSchemaIdResponse {
    fn from(mut pb: PbAllocSchemaIdResponse) -> Self {
        Self {
            header: pb.take_header().into(),
            name: pb.take_name(),
            id: pb.get_id(),
        }
    }
}

impl From<AllocTableIdRequest> for PbAllocTableIdRequest {
    fn from(req: AllocTableIdRequest) -> Self {
        let mut pb = PbAllocTableIdRequest::new();
        pb.set_schema_name(req.schema_name);
        pb.set_name(req.name);
        pb
    }
}

impl From<PbAllocTableIdResponse> for AllocTableIdResponse {
    fn from(mut pb: PbAllocTableIdResponse) -> Self {
        Self {
            header: pb.take_header().into(),
            schema_name: pb.take_schema_name(),
            name: pb.take_name(),
            shard_id: pb.get_shard_id(),
            schema_id: pb.get_schema_id(),
            id: pb.get_id(),
        }
    }
}

impl From<DropTableRequest> for PbDropTableRequest {
    fn from(req: DropTableRequest) -> Self {
        let mut pb = PbDropTableRequest::new();
        pb.set_schema_name(req.schema_name);
        pb.set_name(req.name);
        pb.set_id(req.id);
        pb
    }
}

impl From<PbDropTableResponse> for DropTableResponse {
    fn from(mut pb: PbDropTableResponse) -> Self {
        Self {
            header: pb.take_header().into(),
        }
    }
}
