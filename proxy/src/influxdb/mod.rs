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

//! This module implements [write][1] and [query][2] for InfluxDB.
//! [1]: https://docs.influxdata.com/influxdb/v1.8/tools/api/#write-http-endpoint
//! [2]: https://docs.influxdata.com/influxdb/v1.8/tools/api/#query-http-endpoint

pub mod types;

use std::time::Instant;

use ceresdbproto::storage::{
    RequestContext as GrpcRequestContext, WriteRequest as GrpcWriteRequest,
};
use common_types::request_id::RequestId;
use generic_error::BoxError;
use http::StatusCode;
use interpreters::interpreter::Output;
use log::{debug, info};
use query_engine::{executor::Executor as QueryExecutor, physical_planner::PhysicalPlanner};
use query_frontend::{
    frontend::{Context as SqlContext, Frontend},
    provider::CatalogMetaProvider,
};
use snafu::{ensure, ResultExt};
use time_ext::InstantExt;

use crate::{
    context::RequestContext,
    error::{ErrNoCause, ErrWithCause, Result},
    influxdb::types::{
        convert_influxql_output, convert_write_request, InfluxqlRequest, InfluxqlResponse,
        WriteRequest, WriteResponse,
    },
    metrics::HTTP_HANDLER_COUNTER_VEC,
    Context, Proxy,
};

impl<Q: QueryExecutor + 'static, P: PhysicalPlanner> Proxy<Q, P> {
    pub async fn handle_influxdb_query(
        &self,
        ctx: RequestContext,
        req: InfluxqlRequest,
    ) -> Result<InfluxqlResponse> {
        let output = self.fetch_influxdb_query_output(ctx, req).await?;
        convert_influxql_output(output)
    }

    pub async fn handle_influxdb_write(
        &self,
        ctx: RequestContext,
        req: WriteRequest,
    ) -> Result<WriteResponse> {
        let write_table_requests = convert_write_request(req)?;

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
        let proxy_context = Context::new(
            self.engine_runtimes.write_runtime.clone(),
            ctx.timeout,
            None,
        );

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
                    "Influxdb write finished, catalog:{}, schema:{}, result:{result:?}",
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

    async fn fetch_influxdb_query_output(
        &self,
        ctx: RequestContext,
        req: InfluxqlRequest,
    ) -> Result<Output> {
        let request_id = RequestId::next_id();
        let begin_instant = Instant::now();
        let deadline = ctx.timeout.map(|t| begin_instant + t);

        info!(
            "Influxdb query handler try to process request, request_id:{}, request:{:?}",
            request_id, req
        );

        // TODO(yingwen): Privilege check, cannot access data of other tenant
        // TODO(yingwen): Maybe move MetaProvider to instance
        let provider = CatalogMetaProvider {
            manager: self.instance.catalog_manager.clone(),
            default_catalog: &ctx.catalog,
            default_schema: &ctx.schema,
            function_registry: &*self.instance.function_registry,
        };
        let frontend = Frontend::new(provider);
        let mut sql_ctx = SqlContext::new(request_id, deadline);

        let mut stmts = frontend
            .parse_influxql(&mut sql_ctx, &req.query)
            .box_err()
            .with_context(|| ErrWithCause {
                code: StatusCode::BAD_REQUEST,
                msg: format!("Failed to parse influxql, query:{}", req.query),
            })?;

        if stmts.is_empty() {
            return Ok(Output::AffectedRows(0));
        }

        ensure!(
            stmts.len() == 1,
            ErrNoCause {
                code: StatusCode::BAD_REQUEST,
                msg: format!(
                    "Only support execute one statement now, current num:{}, query:{}.",
                    stmts.len(),
                    req.query
                ),
            }
        );

        let plan = frontend
            .influxql_stmt_to_plan(&mut sql_ctx, stmts.remove(0))
            .box_err()
            .with_context(|| ErrWithCause {
                code: StatusCode::BAD_REQUEST,
                msg: format!("Failed to build plan, query:{}", req.query),
            })?;

        self.instance
            .limiter
            .try_limit(&plan)
            .box_err()
            .context(ErrWithCause {
                code: StatusCode::INTERNAL_SERVER_ERROR,
                msg: "Query is blocked",
            })?;
        let output = self
            .execute_plan(request_id, &ctx.catalog, &ctx.schema, plan, deadline)
            .await?;

        info!(
            "Influxdb query handler finished, request_id:{}, cost:{}ms, request:{:?}",
            request_id,
            begin_instant.saturating_elapsed().as_millis(),
            req
        );

        Ok(output)
    }
}
