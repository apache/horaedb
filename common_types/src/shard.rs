// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

use ceresdbproto::{cluster, meta_service};

pub type ShardId = u32;

#[derive(Debug, Clone, PartialEq, Eq, Default)]
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

#[derive(Debug, Default, Copy, Clone, Eq, PartialEq)]
pub enum ShardRole {
    #[default]
    Leader,
    Follower,
    PendingLeader,
    PendingFollower,
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
