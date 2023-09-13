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

//! Contains common methods used by the read process.

use std::{sync::Arc, time::Instant};

use ceresdbproto::storage::{
    storage_service_client::StorageServiceClient, RequestContext, SqlQueryRequest, SqlQueryResponse,
};
use futures::FutureExt;
use generic_error::BoxError;
use http::StatusCode;
use interpreters::interpreter::Output;
use log::{error, info, warn};
use query_frontend::{
    frontend,
    frontend::{Context as SqlContext, Frontend},
    provider::CatalogMetaProvider,
};
use router::endpoint::Endpoint;
use snafu::{ensure, ResultExt};
use time_ext::InstantExt;
use tokio::sync::mpsc::{self, Sender};
use tonic::{transport::Channel, IntoRequest};

use crate::{
    dedup_requests::{ExecutionGuard, RequestNotifiers, RequestResult},
    error::{ErrNoCause, ErrWithCause, Error, Internal, Result},
    forward::{ForwardRequest, ForwardResult},
    metrics::GRPC_HANDLER_COUNTER_VEC,
    Context, Proxy,
};

pub enum SqlResponse {
    Forwarded(SqlQueryResponse),
    Local(Output),
}

impl Proxy {
    pub(crate) async fn handle_sql(
        &self,
        ctx: &Context,
        schema: &str,
        sql: &str,
        enable_partition_table_access: bool,
    ) -> Result<SqlResponse> {
        if let Some(resp) = self
            .maybe_forward_sql_query(ctx.clone(), schema, sql)
            .await?
        {
            match resp {
                ForwardResult::Forwarded(resp) => return Ok(SqlResponse::Forwarded(resp?)),
                ForwardResult::Local => (),
            }
        };

        Ok(SqlResponse::Local(
            self.fetch_sql_query_output(ctx, schema, sql, enable_partition_table_access)
                .await?,
        ))
    }

    pub(crate) async fn dedup_handle_sql(
        &self,
        ctx: &Context,
        schema: &str,
        sql: &str,
        request_notifiers: Arc<RequestNotifiers<String, Sender<Result<SqlResponse>>>>,
        enable_partition_table_access: bool,
    ) -> Result<SqlResponse> {
        let (tx, mut rx) = mpsc::channel(1);
        let mut guard = match request_notifiers.insert_notifier(sql.to_string(), tx) {
            RequestResult::First => ExecutionGuard::new(|| {
                request_notifiers.take_notifiers(&sql.to_string());
            }),
            RequestResult::Wait => {
                return rx.recv().await.unwrap();
            }
        };

        if let Some(resp) = self
            .maybe_forward_sql_query(ctx.clone(), schema, sql)
            .await?
        {
            match resp {
                ForwardResult::Forwarded(resp) => {
                    let resp = resp?;
                    guard.cancel();
                    let notifiers = request_notifiers.take_notifiers(&sql.to_string()).unwrap();
                    for notifier in &notifiers {
                        if let Err(e) = notifier
                            .send(Ok(SqlResponse::Forwarded(resp.clone())))
                            .await
                        {
                            error!("Failed to send handler result, err:{}.", e);
                        }
                    }
                    GRPC_HANDLER_COUNTER_VEC
                        .dedupped_stream_query
                        .inc_by((notifiers.len() - 1) as u64);
                    return Ok(SqlResponse::Forwarded(resp));
                }
                ForwardResult::Local => (),
            }
        };

        let result = self
            .fetch_sql_query_output(ctx, schema, sql, enable_partition_table_access)
            .await?;

        guard.cancel();
        let notifiers = request_notifiers.take_notifiers(&sql.to_string()).unwrap();
        for notifier in &notifiers {
            if let Err(e) = notifier.send(Ok(SqlResponse::Local(result.clone()))).await {
                error!("Failed to send handler result, err:{}.", e);
            }
        }
        GRPC_HANDLER_COUNTER_VEC
            .dedupped_stream_query
            .inc_by((notifiers.len() - 1) as u64);
        Ok(SqlResponse::Local(result))
    }

    pub(crate) async fn fetch_sql_query_output(
        &self,
        ctx: &Context,
        // TODO: maybe we can put params below input a new ReadRequest struct.
        schema: &str,
        sql: &str,
        enable_partition_table_access: bool,
    ) -> Result<Output> {
        let request_id = ctx.request_id;
        let begin_instant = Instant::now();
        let deadline = ctx.timeout.map(|t| begin_instant + t);
        let catalog = self.instance.catalog_manager.default_catalog_name();

        info!("Handle sql query begin, catalog:{catalog}, schema:{schema}, deadline:{deadline:?}, ctx:{ctx:?}, sql:{sql}");

        let instance = &self.instance;
        // TODO(yingwen): Privilege check, cannot access data of other tenant
        // TODO(yingwen): Maybe move MetaProvider to instance
        let provider = CatalogMetaProvider {
            manager: instance.catalog_manager.clone(),
            default_catalog: catalog,
            default_schema: schema,
            function_registry: &*instance.function_registry,
        };
        let frontend = Frontend::new(provider, instance.dyn_config.fronted.clone());

        let mut sql_ctx = SqlContext::new(request_id, deadline);
        // Parse sql, frontend error of invalid sql already contains sql
        // TODO(yingwen): Maybe move sql from frontend error to outer error
        let mut stmts = frontend
            .parse_sql(&mut sql_ctx, sql)
            .box_err()
            .context(ErrWithCause {
                code: StatusCode::BAD_REQUEST,
                msg: "Failed to parse sql",
            })?;

        // TODO: For simplicity, we only support executing one statement
        let stmts_len = stmts.len();
        ensure!(
            stmts_len == 1,
            ErrNoCause {
                code: StatusCode::BAD_REQUEST,
                msg: format!(
                    "Only support execute one statement now, current num:{stmts_len}, sql:{sql}"
                ),
            }
        );

        // Open partition table if needed.
        let table_name = frontend::parse_table_name(&stmts);
        if let Some(table_name) = &table_name {
            self.maybe_open_partition_table_if_not_exist(catalog, schema, table_name)
                .await?;
        }

        // Create logical plan
        // Note: Remember to store sql in error when creating logical plan
        let plan = frontend
            // TODO(yingwen): Check error, some error may indicate that the sql is invalid. Now we
            // return internal server error in those cases
            .statement_to_plan(&sql_ctx, stmts.remove(0))
            .box_err()
            .with_context(|| ErrWithCause {
                code: StatusCode::INTERNAL_SERVER_ERROR,
                msg: format!("Failed to create plan, query:{sql}"),
            })?;

        let mut plan_maybe_expired = false;
        if let Some(table_name) = &table_name {
            match self.is_plan_expired(&plan, catalog, schema, table_name) {
                Ok(v) => plan_maybe_expired = v,
                Err(err) => {
                    warn!("Plan expire check failed, err:{err}");
                }
            }
        }

        let output = if enable_partition_table_access {
            self.execute_plan_involving_partition_table(request_id, catalog, schema, plan, deadline)
                .await
        } else {
            self.execute_plan(request_id, catalog, schema, plan, deadline)
                .await
        };
        let output = output.box_err().with_context(|| ErrWithCause {
            code: StatusCode::INTERNAL_SERVER_ERROR,
            msg: format!("Failed to execute plan, sql:{sql}"),
        })?;

        let cost = begin_instant.saturating_elapsed();
        info!("Handle sql query successfully, catalog:{catalog}, schema:{schema}, cost:{cost:?}, ctx:{ctx:?}, sql:{sql}");

        match &output {
            Output::AffectedRows(_) => Ok(output),
            Output::Records(v) => {
                if plan_maybe_expired {
                    let num_rows = v
                        .iter()
                        .fold(0_usize, |acc, record_batch| acc + record_batch.num_rows());
                    if num_rows == 0 {
                        warn!("Query time range maybe exceed TTL, sql:{sql}");

                        // TODO: Cannot return this error directly, empty query
                        // should return 200, not 4xx/5xx
                        // All protocols should recognize this error.
                        // return Err(Error::QueryMaybeExceedTTL {
                        //     msg: format!("Query time range maybe exceed TTL,
                        // sql:{sql}"), });
                    }
                }
                Ok(output)
            }
        }
    }

    async fn maybe_forward_sql_query(
        &self,
        ctx: Context,
        schema: &str,
        sql: &str,
    ) -> Result<Option<ForwardResult<SqlQueryResponse, Error>>> {
        let table_name = frontend::parse_table_name_with_sql(sql)
            .box_err()
            .with_context(|| Internal {
                msg: format!("Failed to parse table name with sql, sql:{sql}"),
            })?;
        if table_name.is_none() {
            warn!("Unable to forward sql query without table name, sql:{sql}",);
            return Ok(None);
        }

        let sql_request = SqlQueryRequest {
            context: Some(RequestContext {
                database: schema.to_string(),
            }),
            tables: vec![],
            sql: sql.to_string(),
        };

        let forward_req = ForwardRequest {
            schema: schema.to_string(),
            table: table_name.unwrap(),
            req: sql_request.into_request(),
            forwarded_from: ctx.forwarded_from,
        };
        let do_query = |mut client: StorageServiceClient<Channel>,
                        request: tonic::Request<SqlQueryRequest>,
                        _: &Endpoint| {
            let query = async move {
                client
                    .sql_query(request)
                    .await
                    .map(|resp| resp.into_inner())
                    .box_err()
                    .context(ErrWithCause {
                        code: StatusCode::INTERNAL_SERVER_ERROR,
                        msg: "Forwarded sql query failed",
                    })
            }
            .boxed();

            Box::new(query) as _
        };

        let forward_result = self.forwarder.forward(forward_req, do_query).await;
        Ok(match forward_result {
            Ok(forward_res) => Some(forward_res),
            Err(e) => {
                error!("Failed to forward sql req but the error is ignored, err:{e}");
                None
            }
        })
    }
}
