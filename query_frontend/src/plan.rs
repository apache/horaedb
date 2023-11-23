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

//! Logical plans such as select/insert/update/delete

use std::{
    collections::{BTreeMap, HashMap},
    fmt,
    fmt::{Debug, Formatter},
    ops::Bound,
    sync::Arc,
};

use common_types::{column_schema::ColumnSchema, row::RowGroup, schema::Schema, time::TimeRange};
use datafusion::{
    logical_expr::{
        expr::Expr as DfLogicalExpr, logical_plan::LogicalPlan as DataFusionLogicalPlan,
    },
    prelude::Column,
    scalar::ScalarValue,
};
use logger::warn;
use macros::define_result;
use snafu::Snafu;
use table_engine::{partition::PartitionInfo, table::TableRef};

use crate::{ast::ShowCreateObject, container::TableContainer};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Unsupported alter table operation."))]
    UnsupportedOperation,

    #[snafu(display("Unsupported column data type, err:{}.", source))]
    UnsupportedDataType { source: common_types::datum::Error },

    #[snafu(display("Unsupported column option:{}.", name))]
    UnsupportedColumnOption { name: String },

    #[snafu(display("Alter primary key is not allowed."))]
    AlterPrimaryKey,
}

define_result!(Error);

// TODO(yingwen): Custom Debug format
/// Logical plan to be processed by interpreters
#[derive(Debug)]
pub enum Plan {
    /// A SQL SELECT plan or other plans related to query
    Query(QueryPlan),
    // TODO(yingwen): Other sql command
    Insert(InsertPlan),
    /// Create table plan
    Create(CreateTablePlan),
    /// Drop table plan
    Drop(DropTablePlan),
    /// Describe table plan
    Describe(DescribeTablePlan),
    /// Alter table plan
    AlterTable(AlterTablePlan),
    /// Show plan
    Show(ShowPlan),
    /// Exists table
    Exists(ExistsTablePlan),
}

pub struct QueryPlan {
    pub df_plan: DataFusionLogicalPlan,
    pub table_name: Option<String>,
    // Contains the TableProviders so we can register the them to ExecutionContext later.
    // Use TableProviderAdapter here so we can get the underlying TableRef and also be
    // able to cast to Arc<dyn TableProvider + Send + Sync>
    pub tables: Arc<TableContainer>,
}

impl QueryPlan {
    fn find_timestamp_column(&self) -> Option<Column> {
        let table_name = self.table_name.as_ref()?;
        let table_ref = self.tables.get(table_name.into())?;
        let schema = table_ref.table.schema();
        let timestamp_name = schema.timestamp_name();
        Some(Column::from_name(timestamp_name))
    }

    pub fn extract_time_range(&self) -> Option<TimeRange> {
        let ts_column = self.find_timestamp_column()?;
        let time_range = match influxql_query::logical_optimizer::range_predicate::find_time_range(
            &self.df_plan,
            &ts_column,
        ) {
            Ok(v) => v,
            Err(e) => {
                warn!(
                    "Couldn't find time range, plan:{:?}, err:{}",
                    self.df_plan, e
                );
                return None;
            }
        };

        let mut start = i64::MIN;
        match time_range.start {
            Bound::Included(inclusive_start) => {
                if let DfLogicalExpr::Literal(ScalarValue::Int64(Some(x))) = inclusive_start {
                    start = start.max(x);
                }
            }
            Bound::Excluded(exclusive_start) => {
                if let DfLogicalExpr::Literal(ScalarValue::Int64(Some(x))) = exclusive_start {
                    start = start.max(x - 1);
                }
            }
            Bound::Unbounded => {}
        }
        let mut end = i64::MAX;
        match time_range.end {
            Bound::Included(inclusive_end) => {
                if let DfLogicalExpr::Literal(ScalarValue::Int64(Some(x))) = inclusive_end {
                    end = end.min(x + 1);
                }
            }
            Bound::Excluded(exclusive_start) => {
                if let DfLogicalExpr::Literal(ScalarValue::Int64(Some(x))) = exclusive_start {
                    start = start.min(x);
                }
            }
            Bound::Unbounded => {}
        }

        TimeRange::new(start.into(), end.into())
    }
}

impl Debug for QueryPlan {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("QueryPlan")
            .field("df_plan", &self.df_plan)
            .finish()
    }
}

pub struct CreateTablePlan {
    /// Engine
    pub engine: String,
    /// Create table if not exists
    pub if_not_exists: bool,
    /// Table name
    pub table: String,
    /// Table schema
    pub table_schema: Schema,
    /// Table options
    pub options: HashMap<String, String>,
    /// Table partition info
    pub partition_info: Option<PartitionInfo>,
}

impl Debug for CreateTablePlan {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("CreateTablePlan")
            .field("engine", &self.engine)
            .field("if_not_exists", &self.if_not_exists)
            .field("table", &self.table)
            .field("table_schema", &self.table_schema)
            .field(
                "options",
                &self
                    .options
                    .iter()
                    .map(|(k, v)| (k.clone(), v.clone()))
                    .collect::<BTreeMap<String, String>>(),
            )
            .finish()
    }
}

#[derive(Debug)]
pub struct DropTablePlan {
    /// Engine
    pub engine: String,
    /// If exists
    pub if_exists: bool,
    /// Table name
    pub table: String,
    /// Table partition info
    pub partition_info: Option<PartitionInfo>,
}

/// Insert logical plan
#[derive(Debug)]
pub struct InsertPlan {
    /// The table to insert
    pub table: TableRef,
    /// RowGroup to insert
    pub rows: RowGroup,
    /// Column indexes in schema to its default-value-expr which is used to fill
    /// values
    pub default_value_map: BTreeMap<usize, DfLogicalExpr>,
}

#[derive(Debug)]
pub struct DescribeTablePlan {
    /// The table to describe
    pub table: TableRef,
}

#[derive(Debug)]
pub enum AlterTableOperation {
    /// Add a new column, the column id will be ignored.
    AddColumn(Vec<ColumnSchema>),
    ModifySetting(HashMap<String, String>),
}

#[derive(Debug)]
pub struct AlterTablePlan {
    /// The table to alter.
    pub table: TableRef,
    // TODO(yingwen): Maybe use smallvec.
    pub operations: AlterTableOperation,
}

#[derive(Debug)]
pub struct ShowCreatePlan {
    /// The table to show.
    pub table: TableRef,
    /// The type to show
    pub obj_type: ShowCreateObject,
}

#[derive(Debug, PartialEq, Eq)]
pub enum QueryType {
    Sql,
    InfluxQL,
}

#[derive(Debug, PartialEq, Eq)]
pub struct ShowTablesPlan {
    /// Like pattern
    pub pattern: Option<String>,
    pub query_type: QueryType,
}

#[derive(Debug)]
pub enum ShowPlan {
    /// show create table
    ShowCreatePlan(ShowCreatePlan),
    /// show tables
    ShowTablesPlan(ShowTablesPlan),
    /// show database
    ShowDatabase,
}

#[derive(Debug)]
pub struct ExistsTablePlan {
    pub exists: bool,
}
