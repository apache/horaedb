// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

use std::{convert::TryInto, sync::Arc};

use arrow_deps::arrow::{
    array::StringArray,
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use async_trait::async_trait;
use catalog::manager::Manager;
use snafu::{Backtrace, ResultExt, Snafu};
use sql::{
    ast::ShowCreateObject,
    plan::{ShowCreatePlan, ShowPlan},
};

use crate::{
    context::Context,
    create::{self, get_catalog, get_schema},
    interpreter::{
        Interpreter, InterpreterPtr, Output, Result as InterpreterResult, ShowCreateTable,
        ShowDatabases, ShowTables,
    },
    show_create::ShowCreateInterpreter,
};

const SHOW_TABLES_SCHEMA: &str = "Tables";
const SHOW_DATABASES_SCHEMA: &str = "Schemas";

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

    #[snafu(display("Create a new arrow RecordBatch fail, err:{}", source))]
    NewRecordBatch {
        source: arrow_deps::arrow::error::ArrowError,
    },

    #[snafu(display("Arrow RecordBatch to common_types::RecordBatch fail, err:{}", source))]
    ToCommonRecordType {
        source: common_types::record_batch::Error,
    },

    #[snafu(display("Fetch tables fail, err:{}", source))]
    FetchTables { source: catalog::schema::Error },

    #[snafu(display("Fetch databases fail, err:{}", source))]
    FetchDatabases { source: catalog::Error },

    #[snafu(display("Fetch catalog fail, err:{}", source))]
    FetchCatalog { source: create::Error },

    #[snafu(display("Fetch schema fail, err:{}", source))]
    FetchSchema { source: create::Error },
}

define_result!(Error);

pub struct ShowInterpreter<C> {
    ctx: Context,
    plan: ShowPlan,
    catalog_manager: C,
}

impl<C: Manager + 'static> ShowInterpreter<C> {
    pub fn create(ctx: Context, plan: ShowPlan, catalog_manager: C) -> InterpreterPtr {
        Box::new(Self {
            ctx,
            plan,
            catalog_manager,
        })
    }
}

impl<C: Manager> ShowInterpreter<C> {
    fn show_create(plan: ShowCreatePlan) -> Result<Output> {
        let show_create = ShowCreateInterpreter::create(plan);
        show_create.execute_show_create()
    }

    fn show_tables(ctx: Context, catalog_manager: C) -> Result<Output> {
        let schema = get_schema(&ctx, &catalog_manager).context(FetchSchema)?;

        let tables_names = schema
            .all_tables()
            .context(FetchTables)?
            .iter()
            .map(|t| t.name().to_string())
            .collect::<Vec<_>>();

        let schema = Schema::new(vec![Field::new(SHOW_TABLES_SCHEMA, DataType::Utf8, false)]);
        let record_batch = RecordBatch::try_new(
            Arc::new(schema),
            vec![Arc::new(StringArray::from(tables_names))],
        )
        .context(NewRecordBatch)?;

        let record_batch = record_batch.try_into().context(ToCommonRecordType)?;

        Ok(Output::Records(vec![record_batch]))
    }

    fn show_database(ctx: Context, catalog_manager: C) -> Result<Output> {
        let catalog = get_catalog(&ctx, &catalog_manager).context(FetchCatalog)?;
        let schema_names = catalog
            .all_schemas()
            .context(FetchDatabases)?
            .iter()
            .map(|t| t.name().to_string())
            .collect::<Vec<_>>();

        let schema = Schema::new(vec![Field::new(
            SHOW_DATABASES_SCHEMA,
            DataType::Utf8,
            false,
        )]);
        let record_batch = RecordBatch::try_new(
            Arc::new(schema),
            vec![Arc::new(StringArray::from(schema_names))],
        )
        .context(NewRecordBatch)?;

        let record_batch = record_batch.try_into().context(ToCommonRecordType)?;

        Ok(Output::Records(vec![record_batch]))
    }
}

#[async_trait]
impl<C: Manager> Interpreter for ShowInterpreter<C> {
    async fn execute(self: Box<Self>) -> InterpreterResult<Output> {
        match self.plan {
            ShowPlan::ShowCreatePlan(t) => Self::show_create(t).context(ShowCreateTable),
            ShowPlan::ShowTables => {
                Self::show_tables(self.ctx, self.catalog_manager).context(ShowTables)
            }
            ShowPlan::ShowDatabase => {
                Self::show_database(self.ctx, self.catalog_manager).context(ShowDatabases)
            }
        }
    }
}
