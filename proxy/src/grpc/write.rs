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

use ceresdbproto::storage::{WriteRequest, WriteResponse};
use query_engine::executor::Executor as QueryExecutor;

use crate::{error, error::build_ok_header, metrics::GRPC_HANDLER_COUNTER_VEC, Context, Proxy};

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
                GRPC_HANDLER_COUNTER_VEC.write_failed.inc();
                GRPC_HANDLER_COUNTER_VEC
                    .write_failed_row
                    .inc_by(num_rows as u64);
                WriteResponse {
                    header: Some(error::build_err_header(e)),
                    ..Default::default()
                }
            }
            Ok(v) => {
                GRPC_HANDLER_COUNTER_VEC.write_succeeded.inc();
                GRPC_HANDLER_COUNTER_VEC
                    .write_failed_row
                    .inc_by(v.failed as u64);
                GRPC_HANDLER_COUNTER_VEC
                    .write_succeeded_row
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
