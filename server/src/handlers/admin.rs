// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

use std::collections::BTreeSet;

use crate::handlers::prelude::*;

#[derive(Debug, Deserialize)]
pub enum Operation {
    Add,
    Set,
    Remove,
}

#[derive(Debug, Deserialize)]
pub struct RejectRequest {
    operation: Operation,
    write_block_list: Vec<String>,
    read_block_list: Vec<String>,
}

#[derive(Serialize)]
pub struct RejectResponse {
    write_reject_list: BTreeSet<String>,
    read_reject_list: BTreeSet<String>,
}

pub async fn handle_block<Q: QueryExecutor + 'static>(
    _ctx: RequestContext,
    instance: InstanceRef<Q>,
    request: RejectRequest,
) -> Result<RejectResponse> {
    match request.operation {
        Operation::Add => {
            instance
                .limiter
                .add_write_block_list(request.write_block_list);
            instance
                .limiter
                .add_read_block_list(request.read_block_list);
        }
        Operation::Set => {
            instance
                .limiter
                .set_write_block_list(request.write_block_list);
            instance
                .limiter
                .set_read_block_list(request.read_block_list);
        }
        Operation::Remove => {
            instance
                .limiter
                .remove_write_block_list(request.write_block_list);
            instance
                .limiter
                .remove_read_reject_list(request.read_block_list);
        }
    }

    Ok(RejectResponse {
        write_reject_list: instance
            .limiter
            .get_write_block_list()
            .into_iter()
            .collect::<BTreeSet<_>>(),
        read_reject_list: instance
            .limiter
            .get_read_block_list()
            .into_iter()
            .collect::<BTreeSet<_>>(),
    })
}
