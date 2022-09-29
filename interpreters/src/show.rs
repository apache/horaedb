// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

use std::{convert::TryInto, sync::Arc};

use arrow::{
    array::StringArray,
    datatypes::{DataType, Field, Schema as DataSchema},
    record_batch::RecordBatch,
};
use async_trait::async_trait;
use catalog::{manager::ManagerRef, schema::Schema, Catalog};
use snafu::{Backtrace, OptionExt, ResultExt, Snafu};
use sql::{
    ast::ShowCreateObject,
    plan::{ShowCreatePlan, ShowPlan},
};

use crate::{
    context::Context,
    create::{CatalogNotExists, FindCatalog, FindSchema, SchemaNotExists},
    interpreter::{
        Interpreter, InterpreterPtr, Output, Result as InterpreterResult, ShowCreateTable,
        ShowDatabases, ShowTables,
    },
    show_create::ShowCreateInterpreter,
};

const SHOW_TABLES_COLUMN_SCHEMA: &str = "Tables";
const SHOW_DATABASES_COLUMN_SCHEMA: &str = "Schemas";

#[derive(Debug, Snafu)]
#[snafu(visibility(pub(crate)))]
pub enum Error {
    #[snafu(display(
        "Unsupported show create type, type: {:?}, err:{}",
        obj_type,
        backtrace
    ))]
    UnsupportedType {
        obj_type: ShowCreateObject,
        backtrace: Backtrace,
    },

    #[snafu(display("Failed to create a new arrow RecordBatch, err:{}", source))]
    CreateRecordBatch { source: arrow::error::ArrowError },

    #[snafu(display(
        "Failed to convert arrow::RecordBatch to common_types::RecordBatch, err:{}",
        source
    ))]
    ToCommonRecordType {
        source: common_types::record_batch::Error,
    },

    #[snafu(display("Failed to fetch tables, err:{}", source))]
    FetchTables { source: catalog::schema::Error },

    #[snafu(display("Failed to fetch databases, err:{}", source))]
    FetchDatabases { source: catalog::Error },

    #[snafu(display("Failed to fetch catalog, err:{}", source))]
    FetchCatalog { source: crate::create::Error },

    #[snafu(display("Failed to fetch schema, err:{}", source))]
    FetchSchema { source: crate::create::Error },

    #[snafu(display("From create::Error, err:{}", source))]
    FromCreateError { source: crate::create::Error },
}

define_result!(Error);

impl From<crate::create::Error> for Error {
    fn from(error: crate::create::Error) -> Self {
        use crate::create::Error::*;
        match error {
            FindCatalog { .. } | CatalogNotExists { .. } => Error::FetchCatalog { source: error },
            FindSchema { .. } | SchemaNotExists { .. } => Error::FetchSchema { source: error },
            other => Error::FromCreateError { source: other },
        }
    }
}

pub struct ShowInterpreter {
    ctx: Context,
    plan: ShowPlan,
    catalog_manager: ManagerRef,
}

impl ShowInterpreter {
    pub fn create(ctx: Context, plan: ShowPlan, catalog_manager: ManagerRef) -> InterpreterPtr {
        Box::new(Self {
            ctx,
            plan,
            catalog_manager,
        })
    }
}

impl ShowInterpreter {
    fn show_create(plan: ShowCreatePlan) -> Result<Output> {
        let show_create = ShowCreateInterpreter::create(plan);
        show_create.execute_show_create()
    }

    fn show_tables(ctx: Context, catalog_manager: ManagerRef) -> Result<Output> {
        let schema = get_default_schema(&ctx, &catalog_manager)?;

        let tables_names = schema
            .all_tables()
            .context(FetchTables)?
            .iter()
            .map(|t| t.name().to_string())
            .collect::<Vec<_>>();

        let schema = DataSchema::new(vec![Field::new(
            SHOW_TABLES_COLUMN_SCHEMA,
            DataType::Utf8,
            false,
        )]);
        let record_batch = RecordBatch::try_new(
            Arc::new(schema),
            vec![Arc::new(StringArray::from(tables_names))],
        )
        .context(CreateRecordBatch)?;

        let record_batch = record_batch.try_into().context(ToCommonRecordType)?;

        Ok(Output::Records(vec![record_batch]))
    }

    fn show_databases(ctx: Context, catalog_manager: ManagerRef) -> Result<Output> {
        let catalog = get_default_catalog(&ctx, &catalog_manager)?;
        let schema_names = catalog
            .all_schemas()
            .context(FetchDatabases)?
            .iter()
            .map(|t| t.name().to_string())
            .collect::<Vec<_>>();

        let schema = DataSchema::new(vec![Field::new(
            SHOW_DATABASES_COLUMN_SCHEMA,
            DataType::Utf8,
            false,
        )]);
        let record_batch = RecordBatch::try_new(
            Arc::new(schema),
            vec![Arc::new(StringArray::from(schema_names))],
        )
        .context(CreateRecordBatch)?;

        let record_batch = record_batch.try_into().context(ToCommonRecordType)?;

        Ok(Output::Records(vec![record_batch]))
    }
}

#[async_trait]
impl Interpreter for ShowInterpreter {
    async fn execute(self: Box<Self>) -> InterpreterResult<Output> {
        match self.plan {
            ShowPlan::ShowCreatePlan(t) => Self::show_create(t).context(ShowCreateTable),
            ShowPlan::ShowTables => {
                Self::show_tables(self.ctx, self.catalog_manager).context(ShowTables)
            }
            ShowPlan::ShowDatabase => {
                Self::show_databases(self.ctx, self.catalog_manager).context(ShowDatabases)
            }
        }
    }
}

fn get_default_catalog(
    ctx: &Context,
    catalog_manager: &ManagerRef,
) -> Result<Arc<dyn Catalog + Send + Sync>> {
    let default_catalog = ctx.default_catalog();
    let catalog = catalog_manager
        .catalog_by_name(default_catalog)
        .context(FindCatalog {
            name: default_catalog,
        })?
        .context(CatalogNotExists {
            name: default_catalog,
        })?;
    Ok(catalog)
}

fn get_default_schema(
    ctx: &Context,
    catalog_manager: &ManagerRef,
) -> Result<Arc<dyn Schema + Send + Sync>> {
    let catalog = get_default_catalog(ctx, catalog_manager)?;

    let default_schema = ctx.default_schema();
    let schema = catalog
        .schema_by_name(default_schema)
        .context(FindSchema {
            name: default_schema,
        })?
        .context(SchemaNotExists {
            name: default_schema,
        })?;
    Ok(schema)
}
