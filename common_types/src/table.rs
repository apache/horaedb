// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

pub type TableId = u64;
pub type TableName = String;
pub type ShardId = u32;
pub const DEFAULT_SHARD_ID: u32 = 0;

#[derive(Debug, Clone, Copy, Hash, Eq, PartialEq, Ord, PartialOrd)]
pub struct Location {
    pub table_id: TableId,
    pub shard_id: ShardId,
}

impl Location {
    pub fn new(table_id: TableId, shard_id: ShardId) -> Self {
        Self { table_id, shard_id }
    }
}
