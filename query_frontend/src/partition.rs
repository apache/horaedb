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

//! Parse partition statement to partition info

use datafusion::{logical_expr::Expr, prelude::Column};
use datafusion_proto::bytes::Serializeable;
use generic_error::BoxError;
use snafu::ResultExt;
use sqlparser::ast::Expr as SqlExpr;
use table_engine::partition::{
    HashPartitionInfo, KeyPartitionInfo, PartitionDefinition, PartitionInfo, RandomPartitionInfo,
};

use crate::{
    ast::{HashPartition, KeyPartition, Partition, RandomPartition},
    planner::{ParsePartitionWithCause, Result, UnsupportedPartition},
};

const DEFAULT_PARTITION_VERSION: i32 = 0;
pub const MAX_PARTITION_NUM: u64 = 1024;

pub struct PartitionParser;

impl PartitionParser {
    pub fn parse(partition_stmt: Partition) -> Result<PartitionInfo> {
        Ok(match partition_stmt {
            Partition::Random(stmt) => PartitionInfo::Random(Self::parse_random(stmt)),
            Partition::Hash(stmt) => PartitionInfo::Hash(Self::parse_hash(stmt)?),
            Partition::Key(stmt) => PartitionInfo::Key(Self::parse_key(stmt)?),
        })
    }

    fn parse_random(random_stmt: RandomPartition) -> RandomPartitionInfo {
        let definitions = make_partition_definitions(random_stmt.partition_num);
        RandomPartitionInfo { definitions }
    }

    fn parse_hash(hash_stmt: HashPartition) -> Result<HashPartitionInfo> {
        let HashPartition {
            linear,
            partition_num,
            expr,
        } = hash_stmt;

        let definitions = make_partition_definitions(partition_num);

        if let SqlExpr::Identifier(id) = expr {
            let expr = Expr::Column(Column::from_name(id.value));
            let expr = expr.to_bytes().box_err().context(ParsePartitionWithCause {
                msg: format!("found invalid expr in hash, expr:{expr}"),
            })?;

            Ok(HashPartitionInfo {
                version: DEFAULT_PARTITION_VERSION,
                definitions,
                expr,
                linear,
            })
        } else {
            UnsupportedPartition {
                msg: format!("unsupported expr:{expr}"),
            }
            .fail()
        }
    }

    fn parse_key(key_partition_stmt: KeyPartition) -> Result<KeyPartitionInfo> {
        let KeyPartition {
            linear,
            partition_num,
            partition_key,
        } = key_partition_stmt;

        let definitions = make_partition_definitions(partition_num);

        Ok(KeyPartitionInfo {
            version: DEFAULT_PARTITION_VERSION,
            definitions,
            partition_key,
            linear,
        })
    }
}

fn make_partition_definitions(partition_num: u64) -> Vec<PartitionDefinition> {
    (0..partition_num)
        .map(|p| PartitionDefinition {
            name: p.to_string(),
            origin_name: None,
        })
        .collect()
}
