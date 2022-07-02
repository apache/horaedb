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
    create::{get_catalog, get_schema},
    interpreter::{Interpreter, InterpreterPtr, Output, Result as InterpreterResult, ShowCreate},
    show_create::ShowCreateInInterpreter,
};

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

    #[snafu(display("Create schema fail, err:{}", source))]
    CreateSchema { source: crate::create::Error },

    #[snafu(display("Show tables fail, err:{}", source))]
    ShowTables { source: catalog::schema::Error },

    #[snafu(display("Show database fail, err:{}", source))]
    ShowDatabase { source: catalog::Error },

    #[snafu(display("Arrow Record batch fail, err:{}", source))]
    ArrowRecord {
        source: arrow_deps::arrow::error::ArrowError,
    },

    #[snafu(display("Arrow Record batch into fail, err:{}", source))]
    ArrowInto {
        source: common_types::record_batch::Error,
    },
}

define_result!(Error);

pub struct ShowInInterpreter<C> {
    ctx: Context,
    plan: ShowPlan,
    catalog_manager: C,
}

impl<C: Manager + 'static> ShowInInterpreter<C> {
    pub fn create(ctx: Context, plan: ShowPlan, catalog_manager: C) -> InterpreterPtr {
        Box::new(Self {
            ctx,
            plan,
            catalog_manager,
        })
    }
}

impl<C: Manager> ShowInInterpreter<C> {
    async fn execute_show(self: Box<Self>) -> Result<Output> {
        match self.plan {
            ShowPlan::ShowCreatePlan(t) => Self::show_create(t),
            ShowPlan::ShowTables => Self::show_tables(self.ctx, self.catalog_manager),
            ShowPlan::ShowDatabase => Self::show_database(self.ctx, self.catalog_manager),
        }
    }

    fn show_create(plan: ShowCreatePlan) -> Result<Output> {
        let show_create = ShowCreateInInterpreter::create(plan);
        show_create.execute_show_create()
    }

    fn show_tables(ctx: Context, catalog_manager: C) -> Result<Output> {
        let schema = get_schema(&ctx, &catalog_manager).context(CreateSchema)?;
        let tables_names = schema
            .all_tables()
            .context(ShowTables)?
            .iter()
            .map(|t| t.name().to_string())
            .collect::<Vec<_>>();

        let schema = Schema::new(vec![Field::new("Tables", DataType::Utf8, false)]);
        let record_batch = RecordBatch::try_new(
            Arc::new(schema),
            vec![Arc::new(StringArray::from(tables_names))],
        )
        .context(ArrowRecord)?;

        let record_batch = record_batch.try_into().context(ArrowInto)?;

        Ok(Output::Records(vec![record_batch]))
    }

    fn show_database(ctx: Context, catalog_manager: C) -> Result<Output> {
        let catalog = get_catalog(&ctx, &catalog_manager).context(CreateSchema)?;
        let schema_names = catalog
            .all_schemas()
            .context(ShowDatabase)?
            .iter()
            .map(|t| t.name().to_string())
            .collect::<Vec<_>>();

        let schema = Schema::new(vec![Field::new("Schemas", DataType::Utf8, false)]);
        let record_batch = RecordBatch::try_new(
            Arc::new(schema),
            vec![Arc::new(StringArray::from(schema_names))],
        )
        .context(ArrowRecord)?;

        let record_batch = record_batch.try_into().context(ArrowInto)?;

        Ok(Output::Records(vec![record_batch]))
    }
}

#[async_trait]
impl<C: Manager> Interpreter for ShowInInterpreter<C> {
    async fn execute(self: Box<Self>) -> InterpreterResult<Output> {
        self.execute_show().await.context(ShowCreate)
    }
}
