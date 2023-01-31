// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

//! Parse partition statement to partition info

use datafusion::prelude::Column;
use datafusion_expr::Expr;
use datafusion_proto::bytes::Serializeable;
use snafu::ResultExt;
use sqlparser::ast::Expr as SqlExpr;
use table_engine::partition::{
    HashPartitionInfo, KeyPartitionInfo, PartitionDefinition, PartitionInfo,
};

use crate::{
    ast::{HashPartition, KeyPartition, Partition},
    planner::{ParsePartitionWithCause, Result, UnsupportedPartition},
};

const DEFAULT_PARTITION_VERSION: i32 = 0;
pub const MAX_PARTITION_NUM: u64 = 1024;

pub struct PartitionParser;

impl PartitionParser {
    pub fn parse(partition_stmt: Partition) -> Result<PartitionInfo> {
        Ok(match partition_stmt {
            Partition::Hash(stmt) => PartitionInfo::Hash(PartitionParser::parse_hash(stmt)?),
            Partition::Key(stmt) => PartitionInfo::Key(PartitionParser::parse_key_partition(stmt)?),
        })
    }

    pub fn parse_hash(hash_stmt: HashPartition) -> Result<HashPartitionInfo> {
        let HashPartition {
            linear,
            partition_num,
            expr,
        } = hash_stmt;

        let definitions = make_partition_definitions(partition_num);

        if let SqlExpr::Identifier(id) = expr {
            let expr = Expr::Column(Column::from_name(id.value));
            let expr =
                expr.to_bytes()
                    .map_err(|e| Box::new(e) as _)
                    .context(ParsePartitionWithCause {
                        msg: format!("found invalid expr in hash, expr:{}", expr),
                    })?;

            Ok(HashPartitionInfo {
                version: DEFAULT_PARTITION_VERSION,
                definitions,
                expr,
                linear,
            })
        } else {
            UnsupportedPartition {
                msg: format!("unsupported expr:{}", expr),
            }
            .fail()
        }
    }

    pub fn parse_key_partition(key_partition_stmt: KeyPartition) -> Result<KeyPartitionInfo> {
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
        .into_iter()
        .map(|p| PartitionDefinition {
            name: p.to_string(),
            origin_name: None,
        })
        .collect()
}
