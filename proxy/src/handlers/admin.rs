// Copyright 2022-2023 CeresDB Project Authors. Licensed under Apache-2.0.

use std::collections::BTreeSet;

use query_engine::physical_planner::PhysicalPlanner;

use crate::{handlers::prelude::*, limiter::BlockRule};

#[derive(Debug, Deserialize)]
pub enum Operation {
    Add,
    Set,
    Remove,
}

#[derive(Debug, Deserialize)]
pub struct BlockRequest {
    operation: Operation,
    write_block_list: Vec<String>,
    read_block_list: Vec<String>,
    block_rules: Vec<BlockRule>,
}

#[derive(Serialize)]
pub struct BlockResponse {
    write_block_list: BTreeSet<String>,
    read_block_list: BTreeSet<String>,
    block_rules: BTreeSet<BlockRule>,
}

pub async fn handle_block<Q: QueryExecutor + 'static, P: PhysicalPlanner>(
    _ctx: RequestContext,
    instance: InstanceRef<Q, P>,
    request: BlockRequest,
) -> Result<BlockResponse> {
    let limiter = &instance.limiter;
    match request.operation {
        Operation::Add => {
            limiter.add_write_block_list(request.write_block_list);
            limiter.add_read_block_list(request.read_block_list);
            limiter.add_block_rules(request.block_rules);
        }
        Operation::Set => {
            limiter.set_write_block_list(request.write_block_list);
            limiter.set_read_block_list(request.read_block_list);
            limiter.set_block_rules(request.block_rules);
        }
        Operation::Remove => {
            limiter.remove_write_block_list(request.write_block_list);
            limiter.remove_read_block_list(request.read_block_list);
            limiter.remove_block_rules(&request.block_rules);
        }
    }

    Ok(BlockResponse {
        write_block_list: limiter
            .get_write_block_list()
            .into_iter()
            .collect::<BTreeSet<_>>(),
        read_block_list: limiter
            .get_read_block_list()
            .into_iter()
            .collect::<BTreeSet<_>>(),
        block_rules: limiter.get_block_rules().into_iter().collect(),
    })
}
