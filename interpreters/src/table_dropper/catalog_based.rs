// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

use async_trait::async_trait;
use catalog::{
    manager::ManagerRef,
    schema::{DropOptions, DropTableRequest},
};
use log::warn;
use snafu::{OptionExt, ResultExt};
use sql::plan::DropTablePlan;
use table_engine::engine::TableEngineRef;

use crate::{
    context::Context,
    drop::{CatalogNotExists, FindCatalog, FindSchema, Result, SchemaDropTable, SchemaNotExists},
    interpreter::Output,
    table_dropper::TableDropper,
};

pub struct TableDropperImpl {
    catalog_manager: ManagerRef,
}

impl TableDropperImpl {
    pub fn new(catalog_manager: ManagerRef) -> Self {
        Self { catalog_manager }
    }
}

#[async_trait]
impl TableDropper for TableDropperImpl {
    async fn drop_table(
        &self,
        ctx: Context,
        plan: DropTablePlan,
        table_engine: TableEngineRef,
    ) -> Result<Output> {
        let default_catalog = ctx.default_catalog();
        let catalog = self
            .catalog_manager
            .catalog_by_name(default_catalog)
            .context(FindCatalog {
                name: default_catalog,
            })?
            .context(CatalogNotExists {
                name: default_catalog,
            })?;

        let default_schema = ctx.default_schema();
        let schema = catalog
            .schema_by_name(default_schema)
            .context(FindSchema {
                name: default_schema,
            })?
            .context(SchemaNotExists {
                name: default_schema,
            })?;

        let table = plan.table;
        let request = DropTableRequest {
            catalog_name: catalog.name().to_string(),
            schema_name: schema.name().to_string(),
            schema_id: schema.id(),
            table_name: table.clone(),
            engine: plan.engine,
        };

        let opts = DropOptions { table_engine };

        if schema
            .drop_table(request, opts)
            .await
            .context(SchemaDropTable { table: &table })?
        {
            warn!("Table {} has been dropped already", &table);
        }

        Ok(Output::AffectedRows(0))
    }
}
