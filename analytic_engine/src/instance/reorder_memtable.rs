use std::{any::Any, sync::Arc};

pub use arrow::datatypes::{DataType, Field, Schema as ArrowSchema, SchemaRef as ArrowSchemaRef};
use common_types::{record_batch::RecordBatchWithKey, schema::Schema};
use datafusion::{
    datasource::TableProvider,
    error::DataFusionError,
    logical_expr::{LogicalPlan, LogicalPlanBuilder, TableSource, Values},
    prelude::col,
};
use generic_error::BoxError;
use macros::define_result;
use snafu::{ResultExt, Snafu};

use crate::memtable::ColumnarIterPtr;

const DUMMY_CATALOG_NAME: &str = "catalog";
const DUMMY_SCHEMA_NAME: &str = "schema";
const DUMMY_TABLE_NAME: &str = "memtable";

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Failed to build plan, source:{source}"))]
    MemtableIterError { source: crate::memtable::Error },

    #[snafu(display("Failed to build plan, source:{source}"))]
    BuildPlanError { source: DataFusionError },
}

define_result!(Error);
impl From<DataFusionError> for Error {
    fn from(df_err: DataFusionError) -> Self {
        Error::BuildPlanError { source: df_err }
    }
}
pub struct Reorder {
    iter: ColumnarIterPtr,
    schema: Schema,
    sort_by_col_idx: Vec<usize>,
}

struct MemtableSource {
    arrow_schema: ArrowSchemaRef,
}

impl TableSource for MemtableSource {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> ArrowSchemaRef {
        self.arrow_schema.clone()
    }
}

impl Reorder {
    fn as_df_values(self) -> Result<Values> {
        for batch in self.iter {
            let batch: RecordBatchWithKey = batch.context(MemtableIterError)?;
            batch.into_record_batch().into_arrow_record_batch();
        }

        todo!()
    }

    fn build_logical_plan(&self) -> Result<LogicalPlan> {
        let source = MemtableSource {
            arrow_schema: self.schema.to_arrow_schema_ref(),
        };
        let source = Arc::new(source);

        let columns = self.schema.columns();
        let sort_exprs = self
            .sort_by_col_idx
            .iter()
            .map(|i| col(&columns[*i].name).sort(true, true))
            .collect::<Vec<_>>();
        let df_plan = LogicalPlanBuilder::scan(DUMMY_TABLE_NAME, source, None)?
            .sort(sort_exprs)?
            .build()
            .context(BuildPlanError)?;

        Ok(df_plan)
    }
}
