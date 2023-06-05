// Copyright 2023 CeresDB Project Authors. Licensed under Apache-2.0.

use ceresdbproto::storage::{WriteRequest, WriteResponse};
use query_engine::executor::Executor as QueryExecutor;

use crate::{
    error, error::build_ok_header, grpc::metrics::GRPC_HANDLER_COUNTER_VEC, Context, Proxy,
};

impl<Q: QueryExecutor + 'static> Proxy<Q> {
    pub async fn handle_write(&self, ctx: Context, req: WriteRequest) -> WriteResponse {
        self.hotspot_recorder.inc_write_reqs(&req).await;

        let mut row_count = 0;
        for table_request in &req.table_requests {
            for entry in &table_request.entries {
                row_count += entry.field_groups.len();
            }
        }
        let row_count = row_count as u64;

        match self.handle_write_internal(ctx, req).await {
            Err(e) => {
                error!("Failed to handle write, err:{e}");
                GRPC_HANDLER_COUNTER_VEC.write.failed.inc_by(row_count);
                WriteResponse {
                    header: Some(error::build_err_header(e)),
                    ..Default::default()
                }
            }
            Ok(v) => {
                GRPC_HANDLER_COUNTER_VEC
                    .write
                    .failed
                    .inc_by(row_count - v.success as u64);
                WriteResponse {
                    header: Some(build_ok_header()),
                    success: v.success,
                    failed: v.failed,
                }
            }
        }
    }
}
