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

use std::{
    any::Any,
    fmt,
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context, Poll},
};

pub use arrow::{
    datatypes::{DataType, Field, Schema as ArrowSchema, SchemaRef as ArrowSchemaRef},
    record_batch::RecordBatch as ArrowRecordBatch,
};
use async_trait::async_trait;
use common_types::{
    record_batch::{RecordBatchData, RecordBatchWithKey},
    schema::Schema,
};
use datafusion::{
    datasource::{DefaultTableSource, TableProvider},
    error::DataFusionError,
    execution::{context::SessionState, runtime_env::RuntimeEnv, TaskContext},
    logical_expr::{LogicalPlan, LogicalPlanBuilder, TableType},
    physical_expr::PhysicalSortExpr,
    physical_plan::{
        execute_stream, DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning,
        RecordBatchStream as DfRecordBatchStream, SendableRecordBatchStream, Statistics,
    },
    prelude::{col, Expr, SessionConfig, SessionContext},
    sql::TableReference,
};
use futures::{Stream, StreamExt};
use macros::define_result;
use snafu::{ResultExt, Snafu};

use crate::memtable::ColumnarIterPtr;

const DUMMY_TABLE_NAME: &str = "memtable";

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Failed to build plan, source:{source}"))]
    MemtableIter { source: crate::memtable::Error },

    #[snafu(display("Failed to build plan, source:{source}"))]
    BuildPlan { source: DataFusionError },

    #[snafu(display("Failed to fetch record batch, source:{source}"))]
    FetchRecordBatch { source: DataFusionError },

    #[snafu(display("Failed to convert to record batch data, source:{source}"))]
    ConvertRecordBatchData {
        source: common_types::record_batch::Error,
    },
}

define_result!(Error);

pub type DfResult<T> = std::result::Result<T, DataFusionError>;

impl From<DataFusionError> for Error {
    fn from(df_err: DataFusionError) -> Self {
        Error::BuildPlan { source: df_err }
    }
}

/// Reorder will sort `iter` by given indexes.
/// Currently leverage DataFusion to do the sort, we will build a plan like
/// this:
///
/// ```plaintext
/// Sort: (given columns) asc
///   Project:
///     TableScan (based on memtable's iter)
/// ```
pub struct Reorder {
    pub(crate) iter: ColumnarIterPtr,
    pub(crate) schema: Schema,
    pub(crate) order_by_col_indexes: Vec<usize>,
}

struct MemtableExecutionPlan {
    arrow_schema: ArrowSchemaRef,
    iter: Mutex<Option<ColumnarIterPtr>>,
}

impl ExecutionPlan for MemtableExecutionPlan {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> ArrowSchemaRef {
        self.arrow_schema.clone()
    }

    fn output_partitioning(&self) -> Partitioning {
        Partitioning::UnknownPartitioning(1)
    }

    fn output_ordering(&self) -> Option<&[PhysicalSortExpr]> {
        None
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        // this is a leaf node and has no children
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        Err(DataFusionError::Internal(format!(
            "Children cannot be replaced in {self:?}"
        )))
    }

    fn execute(
        &self,
        _partition: usize,
        _context: Arc<TaskContext>,
    ) -> DfResult<SendableRecordBatchStream> {
        let mut iter = self.iter.lock().unwrap();
        let iter = iter.take().expect("only can execute once");

        Ok(Box::pin(MemtableStream {
            iter,
            arrow_schema: self.arrow_schema.clone(),
        }))
    }

    fn statistics(&self) -> Statistics {
        Statistics::default()
    }
}

struct MemtableStream {
    iter: ColumnarIterPtr,
    arrow_schema: ArrowSchemaRef,
}

impl DfRecordBatchStream for MemtableStream {
    fn schema(&self) -> ArrowSchemaRef {
        self.arrow_schema.clone()
    }
}

impl Stream for MemtableStream {
    type Item = DfResult<ArrowRecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, _ctx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut this = self.as_mut();
        Poll::Ready(this.iter.next().map(|batch| {
            batch
                .map(|batch| batch.into_arrow_record_batch())
                .map_err(|e| DataFusionError::External(Box::new(e)))
        }))
    }
}

impl DisplayAs for MemtableExecutionPlan {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "MemtableExecutionPlan: table={}", DUMMY_TABLE_NAME)
    }
}

impl fmt::Debug for MemtableExecutionPlan {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MemtableExecutionPlan")
            .field("schema", &self.arrow_schema)
            .finish()
    }
}

struct MemtableProvider {
    arrow_schema: ArrowSchemaRef,
    iter: Mutex<Option<ColumnarIterPtr>>,
}

#[async_trait]
impl TableProvider for MemtableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> ArrowSchemaRef {
        self.arrow_schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Temporary
    }

    // TODO: export stats
    fn statistics(&self) -> Option<Statistics> {
        None
    }

    async fn scan(
        &self,
        _state: &SessionState,
        _projection: Option<&Vec<usize>>,
        // filters and limit can be used here to inject some push-down operations if needed
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        let mut iter = self.iter.lock().unwrap();
        let iter = iter.take().expect("only can scan once");

        let plan = MemtableExecutionPlan {
            arrow_schema: self.arrow_schema.clone(),
            iter: Mutex::new(Some(iter)),
        };
        Ok(Arc::new(plan))
    }
}

impl Reorder {
    fn build_logical_plan(
        schema: &Schema,
        sort_by_col_idx: &[usize],
        table_provider: Arc<dyn TableProvider>,
    ) -> Result<LogicalPlan> {
        let source = Arc::new(DefaultTableSource::new(table_provider));

        let columns = schema.columns();
        let sort_exprs = sort_by_col_idx
            .iter()
            .map(|i| col(&columns[*i].name).sort(true, true))
            .collect::<Vec<_>>();
        let df_plan = LogicalPlanBuilder::scan(DUMMY_TABLE_NAME, source, None)?
            .sort(sort_exprs)?
            .build()
            .context(BuildPlan)?;

        Ok(df_plan)
    }

    pub async fn reorder(self) -> Result<Vec<Result<RecordBatchWithKey>>> {
        // 1. Init datafusion context
        let runtime = Arc::new(RuntimeEnv::default());
        let state = SessionState::with_config_rt(SessionConfig::new(), runtime);
        let ctx = SessionContext::with_state(state);
        let table_provider = Arc::new(MemtableProvider {
            arrow_schema: self.schema.to_arrow_schema_ref(),
            iter: Mutex::new(Some(self.iter)),
        });
        ctx.register_table(
            TableReference::from(DUMMY_TABLE_NAME),
            table_provider.clone(),
        )
        .context(BuildPlan)?;

        // 2. Build plan
        let logical_plan =
            Self::build_logical_plan(&self.schema, &self.order_by_col_indexes, table_provider)?;
        let physical_plan = ctx.state().create_physical_plan(&logical_plan).await?;

        // 3. Execute stream
        let stream = execute_stream(physical_plan, ctx.task_ctx())?;
        let batches = stream
            .map(|batch| {
                let batch = batch.context(FetchRecordBatch)?;
                let data = RecordBatchData::try_from(batch).context(ConvertRecordBatchData)?;
                let schema_with_key = self.schema.to_record_schema_with_key();

                Ok(RecordBatchWithKey::new(schema_with_key, data))
            })
            .collect::<Vec<_>>()
            .await;

        Ok(batches)
    }
}
