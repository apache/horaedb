// Copyright 2022-2023 CeresDB Project Authors. Licensed under Apache-2.0.

use std::sync::Arc;

use arrow::{
    array::StringArray,
    datatypes::{DataType, Field, Schema as DataSchema},
    record_batch::RecordBatch,
};
use async_trait::async_trait;
use catalog::{manager::ManagerRef, schema::Schema, Catalog};
use query_frontend::{
    ast::ShowCreateObject,
    plan::{QueryType, ShowCreatePlan, ShowPlan, ShowTablesPlan},
};
use regex::Regex;
use snafu::{Backtrace, OptionExt, ResultExt, Snafu};

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
    FetchTables { source: Box<catalog::schema::Error> },

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

    #[snafu(display("Invalid regexp, err:{}.\nBacktrace\n:{}", source, backtrace))]
    InvalidRegexp {
        source: regex::Error,
        backtrace: Backtrace,
    },
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
            Some(pattern) => {
                let pattern_re = to_pattern_re(&pattern)?;
                schema
                    .all_tables()
                    .map_err(Box::new)
                    .context(FetchTables)?
                    .iter()
                    .map(|t| t.name().to_string())
                    .filter(|table_name| pattern_re.is_match(table_name))
                    .collect::<Vec<_>>()
            }
            None => schema
                .all_tables()
                .map_err(Box::new)
                .context(FetchTables)?
                .iter()
                .map(|t| t.name().to_string())
                .collect::<Vec<_>>(),
        };

        let arrow_record_batch = match plan.query_type {
            QueryType::Sql => {
                let schema = DataSchema::new(vec![Field::new(
                    SHOW_TABLES_COLUMN_SCHEMA,
                    DataType::Utf8,
                    false,
                )]);

                RecordBatch::try_new(
                    Arc::new(schema),
                    vec![Arc::new(StringArray::from(tables_names))],
                )
                .context(CreateRecordBatch)?
            }
            QueryType::InfluxQL => {
                // TODO: refactor those constants
                let schema = DataSchema::new(vec![
                    Field::new("iox::measurement", DataType::Utf8, false),
                    Field::new("name", DataType::Utf8, false),
                ]);

                let measurements = vec!["measurements".to_string(); tables_names.len()];
                let measurements = Arc::new(StringArray::from(measurements));
                RecordBatch::try_new(
                    Arc::new(schema),
                    vec![measurements, Arc::new(StringArray::from(tables_names))],
                )
                .context(CreateRecordBatch)?
            }
        };
        let record_batch = arrow_record_batch.try_into().context(ToCommonRecordType)?;

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
        let arrow_record_batch = RecordBatch::try_new(
            Arc::new(schema),
            vec![Arc::new(StringArray::from(schema_names))],
        )
        .context(CreateRecordBatch)?;

        let record_batch = arrow_record_batch.try_into().context(ToCommonRecordType)?;

        Ok(Output::Records(vec![record_batch]))
    }
}

fn to_pattern_re(pattern: &str) -> Result<Regex> {
    // In MySQL
    // `_` match any single character
    // `% ` match an arbitrary number of characters (including zero characters).
    // so replace those meta character to regexp syntax
    // TODO: support escape char to match exact those two chars
    let pattern = pattern.replace('_', ".").replace('%', ".*");
    let pattern = format!("^{pattern}$");
    Regex::new(&pattern).context(InvalidRegexp)
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
    use crate::show::to_pattern_re;
    #[test]

    fn test_is_table_matched() {
        let testcases = vec![
            // table, pattern, matched
            ("abc", "abc", true),
            ("abc", "abcd", false),
            ("abc", "ab%", true),
            ("abc", "%b%", true),
            ("abc", "_b_", true),
            ("aabcc", "%b%", true),
            ("aabcc", "_b_", false),
        ];

        for (table_name, pattern, matched) in testcases {
            let pattern = to_pattern_re(pattern).unwrap();
            assert_eq!(matched, pattern.is_match(table_name));
        }
    }
}
