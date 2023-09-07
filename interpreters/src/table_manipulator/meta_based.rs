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

use async_trait::async_trait;
use common_types::schema::SchemaEncoder;
use generic_error::BoxError;
use log::info;
use meta_client::{
    types::{CreateTableRequest, DropTableRequest, PartitionTableInfo},
    MetaClientRef,
};
use query_frontend::plan::{CreateTablePlan, DropTablePlan};
use snafu::ResultExt;
use table_engine::{
    engine::TableEngineRef,
    partition::{format_sub_partition_table_name, PartitionInfo},
};

use crate::{
    context::Context,
    interpreter::Output,
    table_manipulator::{CreateWithCause, DropWithCause, Result, TableManipulator},
};

pub struct TableManipulatorImpl {
    meta_client: MetaClientRef,
}

impl TableManipulatorImpl {
    pub fn new(meta_client: MetaClientRef) -> Self {
        Self { meta_client }
    }
}

#[async_trait]
impl TableManipulator for TableManipulatorImpl {
    async fn create_table(
        &self,
        ctx: Context,
        plan: CreateTablePlan,
        table_engine: TableEngineRef,
    ) -> Result<Output> {
        {
            let params = table_engine::engine::CreateTableParams {
                catalog_name: ctx.default_catalog().to_string(),
                schema_name: ctx.default_schema().to_string(),
                table_name: plan.table.clone(),
                table_schema: plan.table_schema.clone(),
                engine: plan.engine.clone(),
                table_options: plan.options.clone(),
                partition_info: plan.partition_info.clone(),
            };
            table_engine
                .validate_create_table(&params)
                .await
                .box_err()
                .with_context(|| CreateWithCause {
                    msg: format!("invalid parameters to create table, plan:{plan:?}"),
                })?;
        }

        let encoded_schema = SchemaEncoder::default()
            .encode(&plan.table_schema)
            .box_err()
            .with_context(|| CreateWithCause {
                msg: format!("fail to encode table schema, plan:{plan:?}"),
            })?;

        let partition_table_info = create_partition_table_info(&plan.table, &plan.partition_info);

        let req = CreateTableRequest {
            schema_name: ctx.default_schema().to_string(),
            name: plan.table,
            encoded_schema,
            engine: plan.engine,
            create_if_not_exist: plan.if_not_exists,
            options: plan.options,
            partition_table_info,
        };

        let resp = self
            .meta_client
            .create_table(req.clone())
            .await
            .box_err()
            .with_context(|| CreateWithCause {
                msg: format!("failed to create table by meta client, req:{req:?}"),
            })?;

        info!(
            "Create table by meta successfully, req:{:?}, resp:{:?}",
            req, resp
        );

        Ok(Output::AffectedRows(0))
    }

    async fn drop_table(
        &self,
        ctx: Context,
        plan: DropTablePlan,
        _table_engine: TableEngineRef,
    ) -> Result<Output> {
        let partition_table_info = create_partition_table_info(&plan.table, &plan.partition_info);

        let req = DropTableRequest {
            schema_name: ctx.default_schema().to_string(),
            name: plan.table,
            partition_table_info,
        };

        let resp = self
            .meta_client
            .drop_table(req.clone())
            .await
            .box_err()
            .context(DropWithCause {
                msg: format!("failed to create table by meta client, req:{req:?}"),
            })?;

        info!(
            "Drop table by meta successfully, req:{:?}, resp:{:?}",
            req, resp
        );

        Ok(Output::AffectedRows(0))
    }
}

fn create_partition_table_info(
    table_name: &str,
    partition_info: &Option<PartitionInfo>,
) -> Option<PartitionTableInfo> {
    if let Some(info) = partition_info {
        let sub_table_names = info
            .get_definitions()
            .iter()
            .map(|def| format_sub_partition_table_name(table_name, &def.name))
            .collect::<Vec<String>>();
        Some(PartitionTableInfo {
            sub_table_names,
            partition_info: info.clone(),
        })
    } else {
        None
    }
}
