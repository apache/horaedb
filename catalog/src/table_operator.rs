// Copyright 2023 CeresDB Project Authors. Licensed under Apache-2.0.

use std::{
    collections::HashMap,
    time::{Duration, Instant},
};

use common_types::table::ShardId;
use common_util::{error::BoxError, time::InstantExt};
use log::{error, info, warn};
use snafu::{OptionExt, ResultExt};
use table_engine::{
    engine,
    table::{TableId, TableRef},
};

use crate::{
    manager::ManagerRef,
    schema::{
        CloseOptions, CloseShardRequest, CloseTableRequest, CreateOptions, CreateTableRequest,
        DropOptions, DropTableRequest, OpenOptions, OpenShardRequest, OpenTableRequest, SchemaRef,
        TableDef,
    },
    Result, TableOperatorNoCause, TableOperatorWithCause,
};

/// Table operator
///
/// Encapsulate all operations about tables, including create/drop, open/close
/// and etc.
#[derive(Clone)]
pub struct TableOperator {
    catalog_manager: ManagerRef,
    retry_limit: usize,
    retry_interval: Duration,
}

enum OpenShardResult {
    Success,
    Retry(HashMap<TableId, TableDef>),
}

struct OpenShardContext<'a> {
    shard_id: ShardId,
    table_defs: HashMap<TableId, TableDef>,
    engine: String,
    retry: &'a mut usize,
    retry_limit: usize,
}

impl TableOperator {
    pub fn new(catalog_manager: ManagerRef, retry_limit: usize, retry_interval: Duration) -> Self {
        Self {
            catalog_manager,
            retry_limit,
            retry_interval,
        }
    }

    pub async fn open_shard(&self, request: OpenShardRequest, opts: OpenOptions) -> Result<()> {
        let mut retry = 0;
        let mut table_defs = request
            .table_defs
            .into_iter()
            .map(|def| (def.id, def))
            .collect::<HashMap<_, _>>();

        loop {
            let context = OpenShardContext {
                shard_id: request.shard_id,
                table_defs,
                engine: request.engine.clone(),
                retry: &mut retry,
                retry_limit: self.retry_limit,
            };

            let once_result = self.open_shard_with_retry(context, opts.clone()).await;
            match once_result {
                Ok(OpenShardResult::Success) => break Ok(()),
                Ok(OpenShardResult::Retry(retry_table_defs)) => {
                    table_defs = retry_table_defs;

                    // Sleep a while before next attempt.
                    tokio::time::sleep(self.retry_interval).await;
                    continue;
                }
                Err(e) => break Err(e),
            }
        }
    }

    async fn open_shard_with_retry(
        &self,
        context: OpenShardContext<'_>,
        opts: OpenOptions,
    ) -> Result<OpenShardResult> {
        info!(
            "TableOperator retry to open shard, retry:{}, retry_limit:{}, shard_id:{}",
            context.retry, context.retry_limit, context.shard_id
        );

        let instant = Instant::now();
        let table_engine = opts.table_engine;
        let shard_id = context.shard_id;

        // Generate open requests.
        let mut related_schemas = Vec::with_capacity(context.table_defs.len());
        let mut engine_table_defs = Vec::with_capacity(context.table_defs.len());
        for open_ctx in context.table_defs.values() {
            let schema = self.schema_by_name(&open_ctx.catalog_name, &open_ctx.schema_name)?;
            let table_id = open_ctx.id;
            engine_table_defs.push(open_ctx.clone().into_engine_table_def(schema.id()));
            related_schemas.push((table_id, schema));
        }

        // Open tables by table engine.
        let engine_open_shard_req = engine::OpenShardRequest {
            shard_id,
            table_defs: engine_table_defs,
            engine: context.engine,
        };
        let mut shard_result = table_engine
            .open_shard(engine_open_shard_req)
            .await
            .box_err()
            .context(TableOperatorWithCause { msg: None })?;

        // Check and register successful opened table into schema.
        let mut success_count = 0_u32;
        let mut missing_table_count = 0_u32;
        let mut open_table_errs = Vec::new();

        // Iter the table results, register the success ones and collect the fail ones.
        let mut table_defs = context.table_defs;
        for (table_id, schema) in related_schemas {
            let table_result = shard_result
                .remove(&table_id)
                .context(TableOperatorNoCause {
                    msg: Some(format!(
                        "table not exist, shard_id:{shard_id}, table_id:{table_id}"
                    )),
                })?;

            match table_result {
                Ok(Some(table)) => {
                    schema.register_table(table);
                    success_count += 1;
                    // Success ones should be remove, we just return the failed ones for retrying.
                    table_defs.remove(&table_id);
                }
                Ok(None) => {
                    error!("TableOperator failed to open a missing table, table_id:{table_id}, schema_id:{:?}, shard_id:{shard_id}", schema.id());
                    missing_table_count += 1;
                    // A special failed case unable to recover by retrying, we just ignore them.
                    table_defs.remove(&table_id);
                }
                Err(e) => {
                    error!("TableOperator failed to open table, table_id:{table_id}, schema_id:{:?}, shard_id:{shard_id}, err:{}", schema.id(), e);
                    open_table_errs.push(e);
                }
            }
        }

        info!(
            "Open shard finish, shard id:{shard_id}, cost:{}ms, success_count:{success_count}, missing_table_count:{missing_table_count}, open_table_errs:{open_table_errs:?}",
            instant.saturating_elapsed().as_millis(),
        );

        let result = if missing_table_count == 0 && open_table_errs.is_empty() {
            Ok(OpenShardResult::Success)
        } else if *context.retry < context.retry_limit && !table_defs.is_empty() {
            Ok(OpenShardResult::Retry(table_defs))
        } else {
            let msg = format!(
                "Failed to open shard, some tables open failed, shard id:{shard_id}, \
                missing_table_count:{missing_table_count}, \
                open_err_count:{}",
                open_table_errs.len()
            );

            TableOperatorNoCause { msg }.fail()
        };

        *context.retry += 1;
        result
    }

    pub async fn close_shard(&self, request: CloseShardRequest, opts: CloseOptions) -> Result<()> {
        let instant = Instant::now();
        let table_engine = opts.table_engine;
        let shard_id = request.shard_id;

        // Generate open requests.
        let mut schemas = Vec::with_capacity(request.table_defs.len());
        let mut engine_table_defs = Vec::with_capacity(request.table_defs.len());
        for table_def in request.table_defs {
            let schema = self.schema_by_name(&table_def.catalog_name, &table_def.schema_name)?;
            engine_table_defs.push(table_def.into_engine_table_def(schema.id()));
            schemas.push(schema);
        }

        //  Close tables by table engine.
        // TODO: add the `close_shard` method into table engine.
        let engine_close_shard_req = engine::CloseShardRequest {
            shard_id: request.shard_id,
            table_defs: engine_table_defs,
            engine: request.engine,
        };
        let close_results = table_engine.close_shard(engine_close_shard_req).await;

        // Check and unregister successful closed table from schema.
        let mut success_count = 0_u32;
        let mut close_table_errs = Vec::new();

        for (schema, close_result) in schemas.into_iter().zip(close_results.into_iter()) {
            match close_result {
                Ok(table_name) => {
                    schema.unregister_table(&table_name);
                    success_count += 1;
                }
                Err(e) => close_table_errs.push(e),
            }
        }

        info!(
            "Close shard finished, shard id:{shard_id}, cost:{}ms, success_count:{success_count}, close_table_errs:{close_table_errs:?}",
            instant.saturating_elapsed().as_millis(),
        );

        if close_table_errs.is_empty() {
            Ok(())
        } else {
            TableOperatorNoCause {
                msg: format!(
                    "Failed to close shard, shard id:{shard_id}, success_count:{success_count}, close_err_count:{}", close_table_errs.len(),
                ),
            }
            .fail()
        }
    }

    pub async fn open_table_on_shard(
        &self,
        request: OpenTableRequest,
        opts: OpenOptions,
    ) -> Result<()> {
        let table_engine = opts.table_engine;
        let schema = self.schema_by_name(&request.catalog_name, &request.schema_name)?;

        let table = table_engine
            .open_table(request.clone().into_engine_open_request(schema.id()))
            .await
            .box_err()
            .context(TableOperatorWithCause {
                msg: format!("failed to open table on shard, request:{request:?}"),
            })?
            .context(TableOperatorNoCause {
                msg: format!("table engine returns none when opening table, request:{request:?}"),
            })?;
        schema.register_table(table);

        Ok(())
    }

    pub async fn close_table_on_shard(
        &self,
        request: CloseTableRequest,
        opts: CloseOptions,
    ) -> Result<()> {
        let table_engine = opts.table_engine;
        let schema = self.schema_by_name(&request.catalog_name, &request.schema_name)?;
        let table_name = request.table_name.clone();

        table_engine
            .close_table(request.clone().into_engine_close_request(schema.id()))
            .await
            .box_err()
            .context(TableOperatorWithCause {
                msg: format!("failed to close table on shard, request:{request:?}"),
            })?;
        schema.unregister_table(&table_name);

        Ok(())
    }

    pub async fn create_table_on_shard(
        &self,
        request: CreateTableRequest,
        opts: CreateOptions,
    ) -> Result<TableRef> {
        let schema = self.schema_by_name(&request.catalog_name, &request.schema_name)?;

        // TODO: we should create table directly by table engine, and register table
        // into schema like opening.
        schema
            .create_table(request.clone(), opts)
            .await
            .box_err()
            .context(TableOperatorWithCause {
                msg: format!("failed to create table on shard, request:{request:?}"),
            })
    }

    pub async fn drop_table_on_shard(
        &self,
        request: DropTableRequest,
        opts: DropOptions,
    ) -> Result<()> {
        let schema = self.schema_by_name(&request.catalog_name, &request.schema_name)?;

        // TODO: we should drop table directly by table engine, and unregister table
        // from schema like closing.
        let has_dropped = schema
            .drop_table(request.clone(), opts)
            .await
            .box_err()
            .context(TableOperatorWithCause {
                msg: format!("failed to create table on shard, request:{request:?}"),
            })?;

        if has_dropped {
            warn!(
                "Table has been dropped already, table_name:{}",
                request.table_name
            );
        }

        Ok(())
    }

    fn schema_by_name(&self, catalog_name: &str, schema_name: &str) -> Result<SchemaRef> {
        let catalog = self
            .catalog_manager
            .catalog_by_name(catalog_name)
            .box_err()
            .context(TableOperatorWithCause {
                msg: format!("failed to find catalog, catalog_name:{catalog_name}"),
            })?
            .context(TableOperatorNoCause {
                msg: format!("catalog not found, catalog_name:{catalog_name}"),
            })?;

        catalog
            .schema_by_name(schema_name)
            .box_err()
            .context(TableOperatorWithCause {
                msg: format!("failed to find schema, schema_name:{schema_name}"),
            })?
            .context(TableOperatorNoCause {
                msg: format!("schema not found, schema_name:{schema_name}"),
            })
    }
}
