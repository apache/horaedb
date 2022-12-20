// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

//! Parse partition statement to partition info

use datafusion::prelude::Column;
use datafusion_expr::Expr;
use datafusion_proto::bytes::Serializeable;
use snafu::ResultExt;
use sqlparser::ast::Expr as SqlExpr;
use table_engine::partition::{Definition, HashPartitionInfo, PartitionInfo};

use crate::{
    ast::{HashPartition, Partition},
    planner::{ParsePartitionWithCause, Result, UnsupportedPartition},
};

pub struct PartitionParser;

impl PartitionParser {
    pub fn parse(partition_stmt: Partition) -> Result<PartitionInfo> {
        Ok(match partition_stmt {
            Partition::Hash(stmt) => PartitionInfo::Hash(PartitionParser::parse_hash(stmt)?),
        })
    }

    pub fn parse_hash(hash_stmt: HashPartition) -> Result<HashPartitionInfo> {
        let HashPartition {
            linear,
            partition_num,
            expr,
        } = hash_stmt;

        let definitions = parse_to_definition(partition_num);

        if let SqlExpr::Identifier(id) = expr {
            let expr = Expr::Column(Column::from_name(id.value));
            let expr =
                expr.to_bytes()
                    .map_err(|e| Box::new(e) as _)
                    .context(ParsePartitionWithCause {
                        msg: format!("found invalid expr in hash, expr:{}", expr),
                    })?;

            Ok(HashPartitionInfo {
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
}

fn parse_to_definition(partition_num: u64) -> Vec<Definition> {
    (0..partition_num)
        .into_iter()
        .map(|p| Definition {
            name: format!("{}", p),
            origin_name: None,
        })
        .collect()
}
