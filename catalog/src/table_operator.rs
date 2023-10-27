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

use std::time::Instant;

use generic_error::BoxError;
use logger::{error, info, warn};
use snafu::{OptionExt, ResultExt};
use table_engine::{engine, table::TableRef};
use time_ext::InstantExt;

use crate::{
    manager::ManagerRef,
    schema::{
        CloseOptions, CloseShardRequest, CloseTableRequest, CreateOptions, CreateTableRequest,
        DropOptions, DropTableRequest, OpenOptions, OpenShardRequest, OpenTableRequest, SchemaRef,
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
        let mut related_schemas = Vec::with_capacity(request.table_defs.len());
        let mut engine_table_defs = Vec::with_capacity(request.table_defs.len());
        for open_ctx in request.table_defs {
            let schema = self.schema_by_name(&open_ctx.catalog_name, &open_ctx.schema_name)?;
            let table_id = open_ctx.id;
            engine_table_defs.push(open_ctx.into_engine_table_def(schema.id()));
            related_schemas.push((table_id, schema));
        }

        // Open tables by table engine.
        let engine_open_shard_req = engine::OpenShardRequest {
            shard_id,
            table_defs: engine_table_defs,
            engine: request.engine,
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
                }
                Ok(None) => {
                    error!("TableOperator failed to open a missing table, table_id:{table_id}, schema_id:{:?}, shard_id:{shard_id}", schema.id());
                    missing_table_count += 1;
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

        if missing_table_count == 0 && open_table_errs.is_empty() {
            Ok(())
        } else {
            let msg = format!(
                "Failed to open shard, some tables open failed, shard id:{shard_id}, \
                missing_table_count:{missing_table_count}, \
                open_err_count:{}",
                open_table_errs.len()
            );

            TableOperatorNoCause { msg }.fail()
        }
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
        let schema =
            self.schema_by_name(&request.params.catalog_name, &request.params.schema_name)?;

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
