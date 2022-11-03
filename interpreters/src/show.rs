// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

use std::{convert::TryInto, sync::Arc};

use arrow::{
    array::StringArray,
    datatypes::{DataType, Field, Schema as DataSchema},
    record_batch::RecordBatch,
};
use async_trait::async_trait;
use catalog::{manager::ManagerRef, schema::Schema, Catalog};
use regex::Regex;
use snafu::{Backtrace, OptionExt, ResultExt, Snafu};
use sql::{
    ast::ShowCreateObject,
    plan::{ShowCreatePlan, ShowPlan, ShowTablesPlan},
};

use crate::{
    context::Context,
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
        "Unsupported show create type, type:{:?}.\nBacktrace:{}",
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

    #[snafu(display("Catalog does not exist, catalog:{}.\nBacktrace\n:{}", name, backtrace))]
    CatalogNotExists { name: String, backtrace: Backtrace },

    #[snafu(display("Schema does not exist, schema:{}.\nBacktrace\n:{}", name, backtrace))]
    SchemaNotExists { name: String, backtrace: Backtrace },

    #[snafu(display("Failed to fetch catalog, err:{}", source))]
    FetchCatalog { source: catalog::manager::Error },

    #[snafu(display("Failed to fetch schema, err:{}", source))]
    FetchSchema { source: catalog::Error },
}

define_result!(Error);

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

    fn show_tables(
        ctx: Context,
        catalog_manager: ManagerRef,
        plan: ShowTablesPlan,
    ) -> Result<Output> {
        let schema = get_default_schema(&ctx, &catalog_manager)?;
        let tables_names = match plan.pattern {
            Some(sc) => schema
                .all_tables()
                .context(FetchTables)?
                .iter()
                .filter(|t| is_table_matched(t.name(), &sc).unwrap())
                .map(|t| t.name().to_string())
                .collect::<Vec<_>>(),
            None => schema
                .all_tables()
                .context(FetchTables)?
                .iter()
                .map(|t| t.name().to_string())
                .collect::<Vec<_>>(),
        };
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

fn is_table_matched(str: &str, search_re: &str) -> Result<bool> {
    let regex_str = search_re
        .replace('\'', "")
        .replace('_', ".")
        .replace('%', ".*");
    let re = Regex::new(&regex_str).unwrap();
    Ok(re.is_match(str))
}

#[async_trait]
impl Interpreter for ShowInterpreter {
    async fn execute(self: Box<Self>) -> InterpreterResult<Output> {
        match self.plan {
            ShowPlan::ShowCreatePlan(t) => Self::show_create(t).context(ShowCreateTable),
            ShowPlan::ShowTablesPlan(t) => {
                Self::show_tables(self.ctx, self.catalog_manager, t).context(ShowTables)
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
    catalog_manager
        .catalog_by_name(default_catalog)
        .context(FetchCatalog)?
        .context(CatalogNotExists {
            name: default_catalog,
        })
}

fn get_default_schema(
    ctx: &Context,
    catalog_manager: &ManagerRef,
) -> Result<Arc<dyn Schema + Send + Sync>> {
    let catalog = get_default_catalog(ctx, catalog_manager)?;

    let default_schema = ctx.default_schema();
    catalog
        .schema_by_name(default_schema)
        .context(FetchSchema)?
        .context(SchemaNotExists {
            name: default_schema,
        })
}

#[cfg(test)]
mod tests {
    use crate::show::is_table_matched;
    #[test]
    fn test_is_table_matched() {
        assert!(is_table_matched("01_system_table1", "01%").is_ok());
        assert!(is_table_matched("01_system_table1", "01_%").is_ok());
    }
}
