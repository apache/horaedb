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

//! This module implements [put][1] for OpenTSDB
//! [1]: http://opentsdb.net/docs/build/html/api_http/put.html

use ceresdbproto::storage::{
    RequestContext as GrpcRequestContext, WriteRequest as GrpcWriteRequest,
};
use http::StatusCode;
use log::debug;
use query_engine::executor::Executor as QueryExecutor;

use crate::{
    context::RequestContext,
    error::{ErrNoCause, Result},
    metrics::HTTP_HANDLER_COUNTER_VEC,
    opentsdb::types::{convert_put_request, PutRequest, PutResponse},
    Context, Proxy,
};

pub mod types;

impl<Q: QueryExecutor + 'static> Proxy<Q> {
    pub async fn handle_opentsdb_put(
        &self,
        ctx: RequestContext,
        req: PutRequest,
    ) -> Result<PutResponse> {
        let write_table_requests = convert_put_request(req)?;

        let num_rows: usize = write_table_requests
            .iter()
            .map(|req| {
                req.entries
                    .iter()
                    .map(|e| e.field_groups.len())
                    .sum::<usize>()
            })
            .sum();

        let table_request = GrpcWriteRequest {
            context: Some(GrpcRequestContext {
                database: ctx.schema.clone(),
            }),
            table_requests: write_table_requests,
        };
        let proxy_context = Context {
            timeout: ctx.timeout,
            runtime: self.engine_runtimes.write_runtime.clone(),
            enable_partition_table_access: false,
            forwarded_from: None,
        };

        match self
            .handle_write_internal(proxy_context, table_request)
            .await
        {
            Ok(result) => {
                if result.failed != 0 {
                    HTTP_HANDLER_COUNTER_VEC.write_failed.inc();
                    HTTP_HANDLER_COUNTER_VEC
                        .write_failed_row
                        .inc_by(result.failed as u64);
                    ErrNoCause {
                        code: StatusCode::INTERNAL_SERVER_ERROR,
                        msg: format!("fail to write storage, failed rows:{:?}", result.failed),
                    }
                    .fail()?;
                }

                debug!(
                    "OpenTSDB write finished, catalog:{}, schema:{}, result:{result:?}",
                    ctx.catalog, ctx.schema
                );

                Ok(())
            }
            Err(e) => {
                HTTP_HANDLER_COUNTER_VEC.write_failed.inc();
                HTTP_HANDLER_COUNTER_VEC
                    .write_failed_row
                    .inc_by(num_rows as u64);
                Err(e)
            }
        }
    }
}
