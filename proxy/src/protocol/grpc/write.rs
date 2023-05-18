// Copyright 2023 CeresDB Project Authors. Licensed under Apache-2.0.

use ceresdbproto::storage::{WriteRequest, WriteResponse};
use query_engine::executor::Executor as QueryExecutor;

use crate::{error, error::build_ok_header, Context, Proxy};

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
