// Copyright 2023 CeresDB Project Authors. Licensed under Apache-2.0.

use std::time::Instant;

use ceresdbproto::storage::{WriteRequest, WriteResponse};
use common_types::request_id::RequestId;
use http::StatusCode;
use interpreters::interpreter::Output;
use log::debug;
use query_engine::executor::Executor as QueryExecutor;
use query_frontend::plan::{InsertPlan, Plan};

use crate::{
    error,
    error::{build_ok_header, ErrNoCause, Result},
    execute_plan,
    instance::InstanceRef,
    Context, Proxy,
};

#[derive(Debug)]
pub struct WriteContext {
    pub request_id: RequestId,
    pub deadline: Option<Instant>,
    pub catalog: String,
    pub schema: String,
    pub auto_create_table: bool,
}

impl<Q: QueryExecutor + 'static> Proxy<Q> {
    pub async fn handle_write(&self, ctx: Context, req: WriteRequest) -> WriteResponse {
        self.hotspot_recorder.inc_write_reqs(&req).await;
        match self.handle_write_internal(ctx, req).await {
            Err(e) => {
                error!("Failed to handle write, err:{e}");
                WriteResponse {
                    header: Some(error::build_err_header(e)),
                    ..Default::default()
                }
            }
            Ok(v) => WriteResponse {
                header: Some(build_ok_header()),
                success: v.success,
                failed: v.failed,
            },
        }
    }
}

pub async fn execute_insert_plan<Q: QueryExecutor + 'static>(
    request_id: RequestId,
    catalog: &str,
    schema: &str,
    instance: InstanceRef<Q>,
    insert_plan: InsertPlan,
    deadline: Option<Instant>,
) -> Result<usize> {
    debug!(
        "Grpc handle write table begin, table:{}, row_num:{}",
        insert_plan.table.name(),
        insert_plan.rows.num_rows()
    );
    let plan = Plan::Insert(insert_plan);
    let output = execute_plan(request_id, catalog, schema, instance, plan, deadline).await;
    output.and_then(|output| match output {
        Output::AffectedRows(n) => Ok(n),
        Output::Records(_) => ErrNoCause {
            code: StatusCode::BAD_REQUEST,
            msg: "Invalid output type, expect AffectedRows, found Records",
        }
        .fail(),
    })
}
