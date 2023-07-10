// Copyright 2023 CeresDB Project Authors. Licensed under Apache-2.0.

//! Contains common methods used by the read process.

use std::time::Instant;

use ceresdbproto::storage::{
    storage_service_client::StorageServiceClient, RequestContext, SqlQueryRequest, SqlQueryResponse,
};
use common_types::request_id::RequestId;
use common_util::{error::BoxError, time::InstantExt};
use futures::FutureExt;
use http::StatusCode;
use interpreters::interpreter::Output;
use log::{error, info, warn};
use query_engine::executor::Executor as QueryExecutor;
use query_frontend::{
    frontend,
    frontend::{Context as SqlContext, Frontend},
    provider::CatalogMetaProvider,
};
use router::endpoint::Endpoint;
use snafu::{ensure, ResultExt};
use tonic::{transport::Channel, IntoRequest};

use crate::{
    error::{ErrNoCause, ErrWithCause, Error, Internal, Result},
    forward::{ForwardRequest, ForwardResult},
    Context, Proxy,
};

pub enum SqlResponse {
    Forwarded(SqlQueryResponse),
    Local(Output),
}

impl<Q: QueryExecutor + 'static> Proxy<Q> {
    pub(crate) async fn handle_sql(
        &self,
        ctx: Context,
        schema: &str,
        sql: &str,
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
            self.fetch_sql_query_output(ctx, schema, sql).await?,
        ))
    }

    pub(crate) async fn fetch_sql_query_output(
        &self,
        ctx: Context,
        schema: &str,
        sql: &str,
    ) -> Result<Output> {
        let request_id = RequestId::next_id();
        let begin_instant = Instant::now();
        let deadline = ctx.timeout.map(|t| begin_instant + t);
        let catalog = self.instance.catalog_manager.default_catalog_name();

        info!("Handle sql query, request_id:{request_id}, schema:{schema}, sql:{sql}");

        let instance = &self.instance;
        // TODO(yingwen): Privilege check, cannot access data of other tenant
        // TODO(yingwen): Maybe move MetaProvider to instance
        let provider = CatalogMetaProvider {
            manager: instance.catalog_manager.clone(),
            default_catalog: catalog,
            default_schema: schema,
            function_registry: &*instance.function_registry,
        };
        let frontend = Frontend::new(provider);

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

        ensure!(
            !stmts.is_empty(),
            ErrNoCause {
                code: StatusCode::BAD_REQUEST,
                msg: format!("No valid query statement provided, sql:{sql}",),
            }
        );

        // TODO(yingwen): For simplicity, we only support executing one statement now
        // TODO(yingwen): INSERT/UPDATE/DELETE can be batched
        ensure!(
            stmts.len() == 1,
            ErrNoCause {
                code: StatusCode::BAD_REQUEST,
                msg: format!(
                    "Only support execute one statement now, current num:{}, sql:{}",
                    stmts.len(),
                    sql
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
            .statement_to_plan(&mut sql_ctx, stmts.remove(0))
            .box_err()
            .with_context(|| ErrWithCause {
                code: StatusCode::INTERNAL_SERVER_ERROR,
                msg: format!("Failed to create plan, query:{sql}"),
            })?;

        if let Some(table_name) = &table_name {
            match self.valid_ttl_range(&plan, catalog, schema, table_name) {
                Ok((valid, ddl)) => {
                    if !valid {
                        return Err(Error::SqlQueryOverTTL {
                            code: StatusCode::OK,
                            msg: format!(
                                "Time range of sql is over TTL, deadline:{ddl}, sql:{sql}"
                            ),
                        });
                    }
                }
                Err(_) => {
                    return Err(Error::ErrNoCause {
                        code: StatusCode::OK,
                        msg: "Parse duration error".to_string(),
                    });
                }
            }
        }

        let output = if ctx.enable_partition_table_access {
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
        info!("Handle sql query success, catalog:{catalog}, schema:{schema}, request_id:{request_id}, cost:{cost:?}, sql:{sql:?}");

        Ok(output)
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
