// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

use async_trait::async_trait;
use common_types::schema::SchemaEncoder;
use log::info;
use meta_client::{
    types::{CreateTableRequest, DropTableRequest, PartitionTableInfo},
    MetaClientRef,
};
use snafu::ResultExt;
use sql::plan::{CreateTablePlan, DropTablePlan};
use table_engine::{
    engine::TableEngineRef,
    partition::{format_sub_partition_table_name, PartitionInfo, PartitionInfoEncoder},
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
        _table_engine: TableEngineRef,
    ) -> Result<Output> {
        let encoded_schema = SchemaEncoder::default()
            .encode(&plan.table_schema)
            .map_err(|e| Box::new(e) as _)
            .with_context(|| CreateWithCause {
                msg: format!(
                    "fail to encode table schema, ctx:{:?}, plan:{:?}",
                    ctx, plan
                ),
            })?;

        let partition_table_info =
            convert_to_partition_table_info(&plan.table, &plan.partition_info);

        let encoder = PartitionInfoEncoder::default();

        let encoded_partition_info = match plan.partition_info.clone() {
            None => Vec::new(),
            Some(v) => {
                encoder
                    .encode(v)
                    .map_err(|e| Box::new(e) as _)
                    .context(CreateWithCause {
                        msg: format!(
                            "fail to encode partition info, ctx:{:?}, plan:{:?}",
                            ctx, plan
                        ),
                    })?
            }
        };
        let req = CreateTableRequest {
            schema_name: ctx.default_schema().to_string(),
            name: plan.table,
            encoded_schema,
            engine: plan.engine,
            create_if_not_exist: plan.if_not_exists,
            options: plan.options,
            partition_table_info,
            encoded_partition_info,
        };

        let resp = self
            .meta_client
            .create_table(req.clone())
            .await
            .map_err(|e| Box::new(e) as _)
            .with_context(|| CreateWithCause {
                msg: format!("failed to create table by meta client, req:{:?}", req),
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
        let partition_table_info =
            convert_to_partition_table_info(&plan.table, &plan.partition_info);

        let req = DropTableRequest {
            schema_name: ctx.default_schema().to_string(),
            name: plan.table,
            partition_table_info,
        };

        let resp = self
            .meta_client
            .drop_table(req.clone())
            .await
            .map_err(|e| Box::new(e) as _)
            .context(DropWithCause {
                msg: format!("failed to create table by meta client, req:{:?}", req),
            })?;

        info!(
            "Drop table by meta successfully, req:{:?}, resp:{:?}",
            req, resp
        );

        Ok(Output::AffectedRows(0))
    }
}

fn convert_to_partition_table_info(
    table_name: &str,
    partition_info: &Option<PartitionInfo>,
) -> Option<PartitionTableInfo> {
    let sub_table_names = partition_info.as_ref().map(|v| {
        v.get_definitions()
            .iter()
            .map(|def| format_sub_partition_table_name(table_name, &def.name))
            .collect::<Vec<String>>()
    });

    sub_table_names.map(|v| PartitionTableInfo { sub_table_names: v })
}
