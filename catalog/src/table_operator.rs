// Copyright 2023 CeresDB Project Authors. Licensed under Apache-2.0.

use std::time::Instant;

use common_util::{error::BoxError, time::InstantExt};
use log::{error, info, warn};
use snafu::{OptionExt, ResultExt};
use table_engine::{
    engine::{CloseShardRequest, OpenShardRequest, TableEngineRef},
    table::TableRef,
};

use crate::{
    manager::ManagerRef,
    schema::{
        CloseOptions, CloseTableRequest, CreateOptions, CreateTableRequest, DropOptions,
        DropTableRequest, OpenOptions, OpenTableRequest, SchemaRef,
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
}

impl TableOperator {
    pub fn new(catalog_manager: ManagerRef) -> Self {
        Self { catalog_manager }
    }

    pub async fn open_shard(&self, request: OpenShardRequest, opts: OpenOptions) -> Result<()> {
        let instant = Instant::now();
        let table_engine = opts.table_engine;
        let shard_id = request.shard_id;

        // Generate open requests.
        let table_infos = request.table_defs;
        let schemas_and_requests = table_infos
            .into_iter()
            .map(|table| {
                let schema_res = self.schema_by_name(&table.catalog_name, &table.schema_name);

                schema_res.map(|schema| {
                    let request = table_engine::engine::OpenTableRequest {
                        catalog_name: table.catalog_name,
                        schema_name: table.schema_name,
                        schema_id: schema.id(),
                        table_name: table.name.clone(),
                        table_id: table.id,
                        engine: request.engine.clone(),
                        shard_id: request.shard_id,
                    };

                    (schema, request)
                })
            })
            .collect::<Result<Vec<_>>>()?;
        let (schemas, requests): (Vec<_>, Vec<_>) = schemas_and_requests.into_iter().unzip();

        // Open tables by table engine.
        // TODO: add the `open_shard` method into table engine.
        let open_res = open_tables_of_shard(table_engine, requests).await;

        // Check and register successful opened table into schema.
        let mut success_count = 0_u32;
        let mut no_table_count = 0_u32;
        let mut open_err_count = 0_u32;

        for (schema, open_res) in schemas.into_iter().zip(open_res.into_iter()) {
            match open_res {
                Ok(Some(table)) => {
                    schema.register_table(table);
                    success_count += 1;
                }
                Ok(None) => {
                    no_table_count += 1;
                }
                // Has printed error log for it.
                Err(_) => {
                    open_err_count += 1;
                }
            }
        }

        info!(
            "Open shard finish, shard id:{}, cost:{}ms, successful count:{}, no table is opened count:{}, open error count:{}",
            shard_id,
            instant.saturating_elapsed().as_millis(),
            success_count,
            no_table_count,
            open_err_count
        );

        if no_table_count == 0 && open_err_count == 0 {
            Ok(())
        } else {
            TableOperatorNoCause {
                msg:  format!(
                            "Failed to open shard, some tables open failed, shard id:{shard_id}, no table is opened count:{no_table_count}, open error count:{open_err_count}"),
            }.fail()
        }
    }

    pub async fn close_shard(&self, request: CloseShardRequest, opts: CloseOptions) -> Result<()> {
        let instant = Instant::now();
        let table_engine = opts.table_engine;
        let shard_id = request.shard_id;

        // Generate open requests.
        let table_defs = request.table_defs;
        let schemas_and_requests = table_defs
            .into_iter()
            .map(|def| {
                let schema_res = self.schema_by_name(&def.catalog_name, &def.schema_name);

                schema_res.map(|schema| {
                    let request = table_engine::engine::CloseTableRequest {
                        catalog_name: def.catalog_name,
                        schema_name: def.schema_name,
                        schema_id: schema.id(),
                        table_name: def.name.clone(),
                        table_id: def.id,
                        engine: request.engine.clone(),
                    };

                    (schema, request)
                })
            })
            .collect::<Result<Vec<_>>>()?;
        let (schemas, requests): (Vec<_>, Vec<_>) = schemas_and_requests.into_iter().unzip();

        //  Close tables by table engine.
        // TODO: add the `close_shard` method into table engine.
        let results = close_tables_of_shard(table_engine, requests).await;

        // Check and unregister successful closed table from schema.
        let mut success_count = 0_u32;
        let mut close_err_count = 0_u32;

        for (schema, result) in schemas.into_iter().zip(results.into_iter()) {
            match result {
                Ok(table_name) => {
                    schema.unregister_table(&table_name);
                    success_count += 1;
                }
                Err(_) => {
                    close_err_count += 1;
                }
            }
        }

        info!(
            "Close shard finished, shard id:{}, cost:{}ms, success_count:{}, close_err_count:{}",
            shard_id,
            instant.saturating_elapsed().as_millis(),
            success_count,
            close_err_count
        );

        if close_err_count == 0 {
            Ok(())
        } else {
            TableOperatorNoCause {
                msg: format!(
                    "Failed to close shard, shard id:{shard_id}, success_count:{success_count}, close_err_count:{close_err_count}",
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

async fn open_tables_of_shard(
    table_engine: TableEngineRef,
    open_requests: Vec<table_engine::engine::OpenTableRequest>,
) -> Vec<table_engine::engine::Result<Option<TableRef>>> {
    if open_requests.is_empty() {
        return Vec::new();
    }

    let mut open_results = Vec::with_capacity(open_requests.len());
    for request in open_requests {
        let result = table_engine
            .open_table(request.clone())
            .await
            .map_err(|e| {
                error!("Failed to open table, open_request:{request:?}, err:{e}");
                e
            })
            .map(|table_opt| {
                if table_opt.is_none() {
                    error!(
                        "Table engine returns none when opening table, open_request:{request:?}"
                    );
                }
                table_opt
            });

        open_results.push(result);
    }

    open_results
}

async fn close_tables_of_shard(
    table_engine: TableEngineRef,
    close_requests: Vec<table_engine::engine::CloseTableRequest>,
) -> Vec<table_engine::engine::Result<String>> {
    if close_requests.is_empty() {
        return Vec::new();
    }

    let mut close_results = Vec::with_capacity(close_requests.len());
    for request in close_requests {
        let result = table_engine
            .close_table(request.clone())
            .await
            .map_err(|e| {
                error!("Failed to close table, close_request:{request:?}, err:{e}");
                e
            })
            .map(|_| request.table_name);

        close_results.push(result);
    }

    close_results
}
