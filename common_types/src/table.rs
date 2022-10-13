// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

pub type TableId = u64;
pub type TableName = String;
pub type ShardId = u32;
pub const DEFAULT_SHARD_ID: u32 = 0;

#[derive(Debug, Clone, Copy, Hash, Eq, PartialEq, Ord, PartialOrd)]
pub struct Location {
    pub shard_id: ShardId,
    pub table_id: TableId,
}

impl Location {
    pub fn new(shard_id: ShardId, table_id: TableId) -> Self {
        Self { shard_id, table_id }
    }
}
