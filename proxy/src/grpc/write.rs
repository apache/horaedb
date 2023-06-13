// Copyright 2023 CeresDB Project Authors. Licensed under Apache-2.0.

use ceresdbproto::storage::{WriteRequest, WriteResponse};
use query_engine::executor::Executor as QueryExecutor;

use crate::{
    error, error::build_ok_header, grpc::metrics::GRPC_HANDLER_ROW_COUNTER_VEC, Context, Proxy,
};

impl<Q: QueryExecutor + 'static> Proxy<Q> {
    pub async fn handle_write(&self, ctx: Context, req: WriteRequest) -> WriteResponse {
        self.hotspot_recorder.inc_write_reqs(&req).await;

        let mut num_rows = 0;
        for table_request in &req.table_requests {
            for entry in &table_request.entries {
                num_rows += entry.field_groups.len();
            }
        }

        match self.handle_write_internal(ctx, req).await {
            Err(e) => {
                error!("Failed to handle write, err:{e}");
                GRPC_HANDLER_ROW_COUNTER_VEC
                    .write_failed
                    .inc_by(num_rows as u64);
                WriteResponse {
                    header: Some(error::build_err_header(e)),
                    ..Default::default()
                }
            }
            Ok(v) => {
                GRPC_HANDLER_ROW_COUNTER_VEC
                    .write_failed
                    .inc_by(v.failed as u64);
                GRPC_HANDLER_ROW_COUNTER_VEC
                    .write_succeeded
                    .inc_by(v.success as u64);
                WriteResponse {
                    header: Some(build_ok_header()),
                    success: v.success,
                    failed: v.failed,
                }
            }
        }
    }
}
