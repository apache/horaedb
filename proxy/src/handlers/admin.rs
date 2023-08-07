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
