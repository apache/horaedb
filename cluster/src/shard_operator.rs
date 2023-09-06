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

use std::collections::HashMap;

use catalog::{
    schema::{
        CloseOptions, CloseTableRequest, CreateOptions, CreateTableRequest, DropOptions,
        DropTableRequest, OpenOptions, OpenTableRequest, TableDef,
    },
    table_operator::TableOperator,
};
use generic_error::BoxError;
use log::info;
use snafu::ResultExt;
use table_engine::{
    engine::{TableEngineRef, TableState},
    table::TableId,
};

use crate::{
    shard_operation::WalRegionCloserRef,
    shard_set::{ShardDataRef, UpdatedTableInfo},
    CloseShardWithCause, CloseTableWithCause, CreateTableWithCause, DropTableWithCause,
    OpenShardNoCause, OpenShardWithCause, OpenTableWithCause, Result,
};

pub struct OpenContext {
    pub catalog: String,
    pub table_engine: TableEngineRef,
    pub table_operator: TableOperator,
    pub engine: String,
}

impl std::fmt::Debug for OpenContext {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OpenContext")
            .field("catalog", &self.catalog)
            .field("engine", &self.engine)
            .finish()
    }
}

pub struct CloseContext {
    pub catalog: String,
    pub table_engine: TableEngineRef,
    pub table_operator: TableOperator,
    pub wal_region_closer: WalRegionCloserRef,
    pub engine: String,
}

impl std::fmt::Debug for CloseContext {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OpenContext")
            .field("catalog", &self.catalog)
            .field("engine", &self.engine)
            .finish()
    }
}

pub struct CreateTableContext {
    pub catalog: String,
    pub table_engine: TableEngineRef,
    pub table_operator: TableOperator,
    pub partition_table_engine: TableEngineRef,
    pub updated_table_info: UpdatedTableInfo,
    pub table_schema: common_types::schema::Schema,
    pub options: HashMap<String, String>,
    pub create_if_not_exist: bool,
    pub engine: String,
}

impl std::fmt::Debug for CreateTableContext {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CreateTableContext")
            .field("catalog", &self.catalog)
            .field("updated_table_info", &self.updated_table_info)
            .field("table_schema", &self.table_schema)
            .field("options", &self.options)
            .field("engine", &self.engine)
            .field("create_if_not_exist", &self.create_if_not_exist)
            .finish()
    }
}

pub struct AlterContext {
    pub catalog: String,
    pub table_engine: TableEngineRef,
    pub table_operator: TableOperator,
    pub updated_table_info: UpdatedTableInfo,
    pub engine: String,
}

impl std::fmt::Debug for AlterContext {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AlterContext")
            .field("catalog", &self.catalog)
            .field("updated_table_info", &self.updated_table_info)
            .field("engine", &self.engine)
            .finish()
    }
}

pub type DropTableContext = AlterContext;
pub type OpenTableContext = AlterContext;
pub type CloseTableContext = AlterContext;

pub struct ShardOperator {
    pub data: ShardDataRef,
}

impl ShardOperator {
    pub async fn open(&self, mut ctx: OpenContext) -> Result<()> {
        let (shard_info, tables) = {
            let data = self.data.read().unwrap();
            let shard_info = data.shard_info.clone();
            let tables = data.tables.clone();

            (shard_info, tables)
        };

        info!("ShardOperator open sequentially begin, shard_id:{shard_info:?}");

        let table_defs = tables
            .into_iter()
            .map(|info| TableDef {
                catalog_name: ctx.catalog.clone(),
                schema_name: info.schema_name,
                id: TableId::from(info.id),
                name: info.name,
            })
            .collect();

        let open_shard_request = catalog::schema::OpenShardRequest {
            shard_id: shard_info.id,
            table_defs,
            engine: ctx.engine,
        };
        let opts = OpenOptions {
            table_engine: ctx.table_engine.clone(),
        };

        let open_shard_result = Box::new(
            ctx.table_operator
                .open_shard(open_shard_request, opts)
                .await
                .box_err()
                .with_context(|| OpenShardWithCause {
                    msg: format!("shard_info:{shard_info:?}"),
                })?,
        );

        if !open_shard_result.get_open_missing_tables().is_empty()
            || !open_shard_result.get_open_failed_tables().is_empty()
        {
            return OpenShardNoCause {
                msg: format!("shard_info:{shard_info:?}"),
            }
            .fail();
        }

        info!("ShardOperator open sequentially finish, shard_id:{shard_info:?}");

        Ok(())
    }

    pub async fn close(&self, mut ctx: CloseContext) -> Result<()> {
        let (shard_info, tables) = {
            let data = self.data.read().unwrap();
            let shard_info = data.shard_info.clone();
            let tables = data.tables.clone();

            (shard_info, tables)
        };
        info!("ShardOperator close sequentially begin, shard_info:{shard_info:?}");

        {
            let mut data = self.data.write().unwrap();
            data.freeze();
            info!("Shard is frozen before closed, shard_id:{}", shard_info.id);
        }

        let table_defs = tables
            .into_iter()
            .map(|info| TableDef {
                catalog_name: ctx.catalog.clone(),
                schema_name: info.schema_name,
                id: TableId::from(info.id),
                name: info.name,
            })
            .collect();
        let close_shard_request = catalog::schema::CloseShardRequest {
            shard_id: shard_info.id,
            table_defs,
            engine: ctx.engine,
        };
        let opts = CloseOptions {
            table_engine: ctx.table_engine,
        };

        ctx.table_operator
            .close_shard(close_shard_request, opts)
            .await
            .box_err()
            .with_context(|| CloseShardWithCause {
                msg: format!("shard_info:{shard_info:?}"),
            })?;

        // Try to close wal region
        ctx.wal_region_closer
            .close_region(shard_info.id)
            .await
            .with_context(|| CloseShardWithCause {
                msg: format!("shard_info:{shard_info:?}"),
            })?;

        info!("ShardOperator close sequentially finish, shard_info:{shard_info:?}");

        Ok(())
    }

    pub async fn create_table(&self, ctx: CreateTableContext) -> Result<()> {
        let shard_info = ctx.updated_table_info.shard_info.clone();
        let table_info = ctx.updated_table_info.table_info.clone();

        info!(
            "ShardOperator create table sequentially begin, shard_info:{shard_info:?}, table_info:{table_info:?}",
        );

        // FIXME: maybe should insert table from cluster after having created table.
        {
            let mut data = self.data.write().unwrap();
            data.try_insert_table(ctx.updated_table_info)
                .box_err()
                .with_context(|| CreateTableWithCause {
                    msg: format!("shard_info:{shard_info:?}, table_info:{table_info:?}"),
                })?;
        }

        // Create the table by operator afterwards.
        let (table_engine, partition_info) = match table_info.partition_info.clone() {
            Some(v) => (ctx.partition_table_engine.clone(), Some(v)),
            None => (ctx.table_engine.clone(), None),
        };

        // Build create table request and options.
        let create_table_request = CreateTableRequest {
            catalog_name: ctx.catalog,
            schema_name: table_info.schema_name.clone(),
            table_name: table_info.name.clone(),
            table_id: Some(TableId::new(table_info.id)),
            table_schema: ctx.table_schema,
            engine: ctx.engine,
            options: ctx.options,
            state: TableState::Stable,
            shard_id: shard_info.id,
            partition_info,
        };

        let create_opts = CreateOptions {
            table_engine,
            create_if_not_exists: ctx.create_if_not_exist,
        };

        let _ = ctx
            .table_operator
            .create_table_on_shard(create_table_request.clone(), create_opts)
            .await
            .box_err()
            .with_context(|| CreateTableWithCause {
                msg: format!("shard_info:{shard_info:?}, table_info:{table_info:?}"),
            })?;

        info!(
            "ShardOperator create table sequentially finish, shard_info:{shard_info:?}, table_info:{table_info:?}",
        );

        Ok(())
    }

    pub async fn drop_table(&self, ctx: DropTableContext) -> Result<()> {
        let shard_info = ctx.updated_table_info.shard_info.clone();
        let table_info = ctx.updated_table_info.table_info.clone();

        info!(
            "ShardOperator drop table sequentially begin, shard_info:{shard_info:?}, table_info:{table_info:?}",
        );

        // FIXME: maybe should insert table from cluster after having dropped table.
        {
            let mut data = self.data.write().unwrap();
            data.try_remove_table(ctx.updated_table_info)
                .box_err()
                .with_context(|| DropTableWithCause {
                    msg: format!("shard_info:{shard_info:?}, table_info:{table_info:?}"),
                })?;
        }

        // Drop the table by operator afterwards.
        let drop_table_request = DropTableRequest {
            catalog_name: ctx.catalog,
            schema_name: table_info.schema_name.clone(),
            table_name: table_info.name.clone(),
            engine: ctx.engine,
        };
        let drop_opts = DropOptions {
            table_engine: ctx.table_engine,
        };

        ctx.table_operator
            .drop_table_on_shard(drop_table_request.clone(), drop_opts)
            .await
            .box_err()
            .with_context(|| DropTableWithCause {
                msg: format!("shard_info:{shard_info:?}, table_info:{table_info:?}"),
            })?;

        info!(
            "ShardOperator drop table sequentially finish, shard_info:{shard_info:?}, table_info:{table_info:?}",
        );

        Ok(())
    }

    pub async fn open_table(&self, ctx: OpenTableContext) -> Result<()> {
        let shard_info = ctx.updated_table_info.shard_info.clone();
        let table_info = ctx.updated_table_info.table_info.clone();

        info!(
            "ShardOperator open table sequentially begin, shard_info:{shard_info:?}, table_info:{table_info:?}",
        );

        // FIXME: maybe should insert table from cluster after having opened table.
        {
            let mut data = self.data.write().unwrap();
            data.try_insert_table(ctx.updated_table_info)
                .box_err()
                .with_context(|| OpenTableWithCause {
                    msg: format!("shard_info:{shard_info:?}, table_info:{table_info:?}"),
                })?;
        }

        // Open the table by operator afterwards.
        let open_table_request = OpenTableRequest {
            catalog_name: ctx.catalog,
            schema_name: table_info.schema_name.clone(),
            table_name: table_info.name.clone(),
            // FIXME: the engine type should not use the default one.
            engine: ctx.engine,
            shard_id: shard_info.id,
            table_id: TableId::new(table_info.id),
        };
        let open_opts = OpenOptions {
            table_engine: ctx.table_engine,
        };

        ctx.table_operator
            .open_table_on_shard(open_table_request.clone(), open_opts)
            .await
            .box_err()
            .with_context(|| OpenTableWithCause {
                msg: format!("shard_info:{shard_info:?}, table_info:{table_info:?}"),
            })?;

        info!(
            "ShardOperator open table sequentially finish, shard_info:{shard_info:?}, table_info:{table_info:?}",
        );

        Ok(())
    }

    pub async fn close_table(&self, ctx: CloseTableContext) -> Result<()> {
        let shard_info = ctx.updated_table_info.shard_info.clone();
        let table_info = ctx.updated_table_info.table_info.clone();

        info!("ShardOperator close table sequentially begin, shard_info:{shard_info:?}, table_info:{table_info:?}");

        // FIXME: maybe should remove table from cluster after having closed table.
        {
            let mut data = self.data.write().unwrap();
            data.try_remove_table(ctx.updated_table_info)
                .box_err()
                .with_context(|| CloseTableWithCause {
                    msg: format!("shard_info:{shard_info:?}, table_info:{table_info:?}"),
                })?;
        }

        // Close the table by catalog manager afterwards.
        let close_table_request = CloseTableRequest {
            catalog_name: ctx.catalog,
            schema_name: table_info.schema_name.clone(),
            table_name: table_info.name.clone(),
            table_id: TableId::new(table_info.id),
            // FIXME: the engine type should not use the default one.
            engine: ctx.engine,
        };
        let close_opts = CloseOptions {
            table_engine: ctx.table_engine,
        };

        ctx.table_operator
            .close_table_on_shard(close_table_request.clone(), close_opts)
            .await
            .box_err()
            .with_context(|| CloseTableWithCause {
                msg: format!("shard_info:{shard_info:?}, table_info:{table_info:?}"),
            })?;

        info!("ShardOperator close table sequentially finish, shard_info:{shard_info:?}, table_info:{table_info:?}");

        Ok(())
    }

    pub async fn get_open_success_tables(&self) -> Result<Vec<String>> {
        let data = self.data.read().unwrap();
        Ok(data.open_success_tables.clone())
    }

    pub async fn get_open_failed_tables(&self) -> Result<Vec<String>> {
        let data = self.data.read().unwrap();
        Ok(data.open_failed_tables.clone())
    }

    pub async fn get_open_missing_tables(&self) -> Result<Vec<String>> {
        let data = self.data.read().unwrap();
        Ok(data.open_missing_tables.clone())
    }
}
