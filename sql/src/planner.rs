// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

//! Planner converts a SQL AST into logical plans

use std::{
    collections::{BTreeMap, HashMap},
    convert::TryFrom,
    mem,
    sync::Arc,
};

use arrow_deps::datafusion::{error::DataFusionError, sql::planner::SqlToRel};
use common_types::{
    column_schema::{self, ColumnSchema},
    datum::{Datum, DatumKind},
    request_id::RequestId,
    row::{RowGroup, RowGroupBuilder},
    schema::{self, Schema, TSID_COLUMN},
};
use log::debug;
use snafu::{ensure, Backtrace, OptionExt, ResultExt, Snafu};
use sqlparser::ast::{
    ColumnDef, ColumnOption, Expr, ObjectName, Query, SetExpr, SqlOption,
    Statement as SqlStatement, TableConstraint, Value, Values,
};
use table_engine::table::TableRef;

use crate::{
    ast::{
        AlterAddColumn, AlterModifySetting, CreateTable, DescribeTable, DropTable, ExistsTable,
        ShowCreate, Statement, TableName,
    },
    container::TableReference,
    parser,
    plan::{
        AlterTableOperation, AlterTablePlan, CreateTablePlan, DescribeTablePlan, DropTablePlan,
        ExistsTablePlan, InsertPlan, Plan, QueryPlan, ShowCreatePlan, ShowPlan,
    },
    promql::{ColumnNames, Expr as PromExpr},
    provider::{ContextProviderAdapter, MetaProvider},
};

// We do not carry backtrace in sql error because it is mainly used in server
// handler and the error is usually caused by invalid/unsupported sql, which
// should be easy to find out the reason.
#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("DataFusion Failed to plan, err:{}", source))]
    DataFusionPlan { source: DataFusionError },

    // Statement is too large and complicate to carry in Error, so we
    // only return error here, so the caller should attach sql to its
    // error context
    #[snafu(display("Unsupported SQL statement"))]
    UnsupportedStatement,

    #[snafu(display("Create table name is empty"))]
    CreateTableNameEmpty,

    #[snafu(display("Table must contain timestamp constraint"))]
    RequireTimestamp,

    #[snafu(display(
        "Table must contain only one timestamp key and it's data type must be TIMESTAMP"
    ))]
    InvalidTimetampKey,

    #[snafu(display("Invalid unsign type: {}.\nBacktrace:\n{}", kind, backtrace))]
    InvalidUnsignType {
        kind: DatumKind,
        backtrace: Backtrace,
    },

    #[snafu(display("Primary key not found, column name:{}", name))]
    PrimaryKeyNotFound { name: String },

    #[snafu(display("Tag column not found, name:{}", name))]
    TagColumnNotFound { name: String },

    #[snafu(display("Timestamp column not found, name:{}", name))]
    TimestampColumnNotFound { name: String },

    #[snafu(display("{} is a reserved column name", name))]
    ColumnNameReserved { name: String },

    #[snafu(display("Invalid create table name, err:{}", source))]
    InvalidCreateTableName { source: DataFusionError },

    #[snafu(display("Failed to build schema, err:{}", source))]
    BuildTableSchema { source: common_types::schema::Error },

    #[snafu(display("Unsupported SQL data type, err:{}", source))]
    UnsupportedDataType { source: common_types::datum::Error },

    #[snafu(display("Invalid column schema, column_name:{}, err:{}", column_name, source))]
    InvalidColumnSchema {
        column_name: String,
        source: column_schema::Error,
    },

    #[snafu(display("Invalid table name, err:{}", source))]
    InvalidTableName { source: DataFusionError },

    #[snafu(display("Table not found, table:{}", name))]
    TableNotFound { name: String },

    #[snafu(display("Column is not null, table:{}, column:{}", table, column))]
    InsertMissingColumn { table: String, column: String },

    #[snafu(display("Column is reserved, table:{}, column:{}", table, column))]
    InsertReservedColumn { table: String, column: String },

    #[snafu(display("Unknown insert column, name:{}", name))]
    UnknownInsertColumn { name: String },

    #[snafu(display("Insert values not enough, len:{}, index:{}", len, index))]
    InsertValuesNotEnough { len: usize, index: usize },

    #[snafu(display("Invalid insert stmt, contains duplicate columns"))]
    InsertDuplicateColumns,

    #[snafu(display("Invalid insert stmt, source should be a set"))]
    InsertSourceBodyNotSet,

    #[snafu(display("Invalid insert stmt, source expr is not value"))]
    InsertExprNotValue,

    #[snafu(display("Insert Failed to convert value, err:{}", source))]
    InsertConvertValue { source: common_types::datum::Error },

    #[snafu(display("Failed to build row, err:{}", source))]
    BuildRow { source: common_types::row::Error },

    #[snafu(display("MetaProvider Failed to find table, err:{}", source))]
    MetaProviderFindTable { source: crate::provider::Error },

    #[snafu(display("Failed to find meta during planning, err:{}", source))]
    FindMeta { source: crate::provider::Error },

    #[snafu(display("Invalid alter table operation, err:{}", source))]
    InvalidAlterTableOperation { source: crate::plan::Error },

    #[snafu(display("Unsupported sql option, value:{}", value))]
    UnsupportedOption { value: String },

    #[snafu(display("Failed to build plan from promql, error:{}", source))]
    BuildPromPlanError { source: crate::promql::Error },
}

define_result!(Error);

/// Planner produces logical plans from SQL AST
// TODO(yingwen): Rewrite Planner instead of using datafusion's planner
pub struct Planner<'a, P: MetaProvider> {
    provider: &'a P,
    request_id: RequestId,
    read_parallelism: usize,
}

impl<'a, P: MetaProvider> Planner<'a, P> {
    /// Create a new logical planner
    pub fn new(provider: &'a P, request_id: RequestId, read_parallelism: usize) -> Self {
        Self {
            provider,
            request_id,
            read_parallelism,
        }
    }

    /// Create a logical plan from Statement
    ///
    /// Takes the ownership of statement because some statements like INSERT
    /// statements contains lots of data
    pub fn statement_to_plan(&self, statement: Statement) -> Result<Plan> {
        let adapter =
            ContextProviderAdapter::new(self.provider, self.request_id, self.read_parallelism);
        // SqlToRel needs to hold the reference to adapter, thus we can't both holds the
        // adapter and the SqlToRel in Planner, which is a self-referential
        // case. We wrap a PlannerDelegate to workaround this and avoid the usage of
        // pin.
        let planner = PlannerDelegate::new(adapter);

        match statement {
            Statement::Standard(s) => planner.sql_statement_to_plan(*s),
            Statement::Create(s) => planner.create_table_to_plan(s),
            Statement::Drop(s) => planner.drop_table_to_plan(s),
            Statement::Describe(s) => planner.describe_table_to_plan(s),
            Statement::AlterModifySetting(s) => planner.alter_modify_setting_to_plan(s),
            Statement::AlterAddColumn(s) => planner.alter_add_column_to_plan(s),
            Statement::ShowCreate(s) => planner.show_create_to_plan(s),
            Statement::ShowTables => planner.show_tables_to_plan(),
            Statement::ShowDatabases => planner.show_databases_to_plan(),
            Statement::Exists(s) => planner.exists_table_to_plan(s),
        }
    }

    pub fn promql_expr_to_plan(&self, expr: PromExpr) -> Result<(Plan, Arc<ColumnNames>)> {
        let adapter =
            ContextProviderAdapter::new(self.provider, self.request_id, self.read_parallelism);
        // SqlToRel needs to hold the reference to adapter, thus we can't both holds the
        // adapter and the SqlToRel in Planner, which is a self-referential
        // case. We wrap a PlannerDelegate to workaround this and avoid the usage of
        // pin.
        let planner = PlannerDelegate::new(adapter);

        expr.to_plan(planner.meta_provider, self.read_parallelism)
            .context(BuildPromPlanError)
    }
}

/// A planner wraps the datafusion's logical planner, and delegate sql like
/// select/explain to datafusion's planner.
struct PlannerDelegate<'a, P: MetaProvider> {
    meta_provider: ContextProviderAdapter<'a, P>,
}

impl<'a, P: MetaProvider> PlannerDelegate<'a, P> {
    fn new(meta_provider: ContextProviderAdapter<'a, P>) -> Self {
        Self { meta_provider }
    }

    fn sql_statement_to_plan(self, sql_stmt: SqlStatement) -> Result<Plan> {
        match sql_stmt {
            // Query statement use datafusion planner
            SqlStatement::Explain { .. } | SqlStatement::Query(_) => {
                self.sql_statement_to_datafusion_plan(sql_stmt)
            }
            SqlStatement::Insert { .. } => self.insert_to_plan(sql_stmt),
            _ => UnsupportedStatement.fail(),
        }
    }

    fn sql_statement_to_datafusion_plan(self, sql_stmt: SqlStatement) -> Result<Plan> {
        let df_planner = SqlToRel::new(&self.meta_provider);

        let df_plan = df_planner
            .sql_statement_to_plan(sql_stmt)
            .context(DataFusionPlan)?;

        debug!("Sql statement to datafusion plan, df_plan:\n{:#?}", df_plan);

        // Get all tables needed in the plan
        let tables = self.meta_provider.try_into_container().context(FindMeta)?;

        Ok(Plan::Query(QueryPlan {
            df_plan,
            tables: Arc::new(tables),
        }))
    }

    fn create_table_to_plan(&self, stmt: CreateTable) -> Result<Plan> {
        ensure!(!stmt.table_name.is_empty(), CreateTableNameEmpty);

        debug!("Create table to plan, stmt:{:?}", stmt);

        // TODO(yingwen): Maybe support create table on other schema?
        let table_name = stmt.table_name.to_string();
        let table_ref = TableReference::from(table_name.as_str());

        // Now we only takes the table name and ignore the schema and catalog name
        let table = table_ref.table().to_string();

        let mut schema_builder =
            schema::Builder::with_capacity(stmt.columns.len()).auto_increment_column_id(true);
        let mut name_column_map = BTreeMap::new();

        // Build all column schemas.
        for col in &stmt.columns {
            name_column_map.insert(col.name.value.as_str(), parse_column(col)?);
        }

        // Tsid column is a reserved column.
        ensure!(
            !name_column_map.contains_key(TSID_COLUMN),
            ColumnNameReserved {
                name: TSID_COLUMN.to_string(),
            }
        );

        // Find timestamp key and primary key contraint
        let mut primary_key_constraint_idx = None;
        let mut timestamp_name = None;
        for (idx, constraint) in stmt.constraints.iter().enumerate() {
            if let TableConstraint::Unique {
                columns,
                is_primary,
                ..
            } = constraint
            {
                if *is_primary {
                    primary_key_constraint_idx = Some(idx);
                } else if parser::is_timestamp_key_constraint(constraint) {
                    // Only one timestamp key constraint
                    ensure!(timestamp_name.is_none(), InvalidTimetampKey);
                    // Only one column in constraint
                    ensure!(columns.len() == 1, InvalidTimetampKey);

                    let name = &columns[0].value;
                    let timestamp_column = name_column_map
                        .get(name as &str)
                        .context(TimestampColumnNotFound { name })?;
                    // Ensure type is timestamp
                    ensure!(
                        timestamp_column.data_type == DatumKind::Timestamp,
                        InvalidTimetampKey
                    );

                    timestamp_name = Some(name.clone());
                }
            }
        }

        // Timestamp column must be provided.
        let timestamp_name = timestamp_name.context(RequireTimestamp)?;

        // Build primary key, the builder will check timestamp column is in primary key.
        if let Some(idx) = primary_key_constraint_idx {
            // If primary key is already provided, use that primary key.
            if let TableConstraint::Unique { columns, .. } = &stmt.constraints[idx] {
                for col in columns {
                    let key_column = name_column_map.remove(&*col.value).with_context(|| {
                        PrimaryKeyNotFound {
                            name: col.value.clone(),
                        }
                    })?;
                    // The schema builder will checks there is only one timestamp column in primary
                    // key.
                    schema_builder = schema_builder
                        .add_key_column(key_column)
                        .context(BuildTableSchema)?;
                }
            }
        } else {
            // If primary key is not set, Use (timestamp, tsid) as primary key.
            let timestamp_column = name_column_map.remove(timestamp_name.as_str()).context(
                TimestampColumnNotFound {
                    name: &timestamp_name,
                },
            )?;
            let column_schema =
                column_schema::Builder::new(TSID_COLUMN.to_string(), DatumKind::UInt64)
                    .is_nullable(false)
                    .build()
                    .context(InvalidColumnSchema {
                        column_name: TSID_COLUMN,
                    })?;
            schema_builder = schema_builder
                .enable_tsid_primary_key(true)
                .add_key_column(timestamp_column)
                .context(BuildTableSchema)?
                .add_key_column(column_schema)
                .context(BuildTableSchema)?;
        }

        // The key columns have been consumed.
        for col in name_column_map.into_values() {
            schema_builder = schema_builder
                .add_normal_column(col)
                .context(BuildTableSchema)?;
        }

        let table_schema = schema_builder.build().context(BuildTableSchema)?;

        let options = parse_options(stmt.options)?;

        let plan = CreateTablePlan {
            engine: stmt.engine,
            if_not_exists: stmt.if_not_exists,
            table,
            table_schema,
            options,
        };

        debug!("Create table to plan, plan:{:?}", plan);

        Ok(Plan::Create(plan))
    }

    fn drop_table_to_plan(&self, stmt: DropTable) -> Result<Plan> {
        let table = if stmt.if_exists {
            stmt.table_name.to_string()
        } else {
            self.find_table(stmt.table_name)?.name().to_string()
        };

        Ok(Plan::Drop(DropTablePlan {
            engine: stmt.engine,
            if_exists: stmt.if_exists,
            table,
        }))
    }

    fn describe_table_to_plan(&self, stmt: DescribeTable) -> Result<Plan> {
        let table = self.find_table(stmt.table_name)?;

        Ok(Plan::Describe(DescribeTablePlan { table }))
    }

    // REQUIRE: SqlStatement must be INSERT stmt
    fn insert_to_plan(&self, sql_stmt: SqlStatement) -> Result<Plan> {
        match sql_stmt {
            SqlStatement::Insert {
                table_name,
                columns,
                source,
                ..
            } => {
                let table = self.find_table(ObjectName(table_name.0).into())?;

                let schema = table.schema();
                // Column name and its index in insert stmt: {column name} => index
                let column_names_idx: HashMap<_, _> = columns
                    .iter()
                    .enumerate()
                    .map(|(idx, ident)| (&ident.value, idx))
                    .collect();
                ensure!(
                    column_names_idx.len() == columns.len(),
                    InsertDuplicateColumns
                );

                validate_insert_stmt(table.name(), &schema, &column_names_idx)?;

                // Index in insert values stmt of each column in table schema
                let mut column_index_in_insert = Vec::with_capacity(schema.num_columns());

                // Check all not null columns are provided in stmt, also init
                // `column_index_in_insert`
                for (idx, column) in schema.columns().iter().enumerate() {
                    if let Some(tsid_idx) = schema.index_of_tsid() {
                        if idx == tsid_idx {
                            // This is a tsid column.
                            column_index_in_insert.push(InsertMode::Auto);
                            continue;
                        }
                    }
                    match column_names_idx.get(&column.name) {
                        Some(idx_in_insert) => {
                            // This column in schema is also in insert stmt
                            column_index_in_insert.push(InsertMode::Direct(*idx_in_insert));
                        }
                        None => {
                            // This column in schema is not in insert stmt
                            if column.is_nullable {
                                column_index_in_insert.push(InsertMode::Null);
                            } else {
                                // Column is not null and input does not contains that column
                                return InsertMissingColumn {
                                    table: table.name(),
                                    column: &column.name,
                                }
                                .fail();
                            }
                        }
                    }
                }

                let rows = build_row_group(schema, source, column_index_in_insert)?;

                Ok(Plan::Insert(InsertPlan { table, rows }))
            }
            // We already known this stmt is a INSERT stmt
            _ => unreachable!(),
        }
    }

    fn alter_modify_setting_to_plan(&self, stmt: AlterModifySetting) -> Result<Plan> {
        let table = self.find_table(stmt.table_name)?;
        let plan = AlterTablePlan {
            table,
            operations: AlterTableOperation::ModifySetting(parse_options(stmt.options)?),
        };
        Ok(Plan::AlterTable(plan))
    }

    fn alter_add_column_to_plan(&self, stmt: AlterAddColumn) -> Result<Plan> {
        let table = self.find_table(stmt.table_name)?;
        let plan = AlterTablePlan {
            table,
            operations: AlterTableOperation::AddColumn(parse_columns(stmt.columns)?),
        };
        Ok(Plan::AlterTable(plan))
    }

    fn exists_table_to_plan(&self, stmt: ExistsTable) -> Result<Plan> {
        let table = self.find_table(stmt.table_name);
        match table {
            Ok(_) => Ok(Plan::Exists(ExistsTablePlan { exists: true })),
            Err(_) => Ok(Plan::Exists(ExistsTablePlan { exists: false })),
        }
    }

    fn show_create_to_plan(&self, show_create: ShowCreate) -> Result<Plan> {
        let table = self.find_table(show_create.table_name)?;
        let plan = ShowCreatePlan {
            table,
            obj_type: show_create.obj_type,
        };
        Ok(Plan::Show(ShowPlan::ShowCreatePlan(plan)))
    }

    fn show_tables_to_plan(&self) -> Result<Plan> {
        Ok(Plan::Show(ShowPlan::ShowTables))
    }

    fn show_databases_to_plan(&self) -> Result<Plan> {
        Ok(Plan::Show(ShowPlan::ShowDatabase))
    }

    fn find_table(&self, table_name: TableName) -> Result<TableRef> {
        let table_name = table_name.to_string();
        let table_ref = TableReference::from(table_name.as_str());

        self.meta_provider
            .table(table_ref)
            .context(MetaProviderFindTable)?
            .with_context(|| TableNotFound { name: table_name })
    }
}

#[derive(Debug)]
enum InsertMode {
    // Insert the value in expr with given index directly.
    Direct(usize),
    // No value provided, insert a null.
    Null,
    // Auto generated column, just temporary fill by default value, the real value will
    // be filled by interpreter.
    Auto,
}

/// Build RowGroup
fn build_row_group(
    schema: Schema,
    source: Box<Query>,
    column_index_in_insert: Vec<InsertMode>,
) -> Result<RowGroup> {
    // Build row group by schema
    match *source.body {
        SetExpr::Values(Values(values)) => {
            let mut row_group_builder =
                RowGroupBuilder::with_capacity(schema.clone(), values.len());
            for mut exprs in values {
                // Try to build row
                let mut row_builder = row_group_builder.row_builder();

                // For each column in schema, append datum into row builder
                for (index_opt, column_schema) in
                    column_index_in_insert.iter().zip(schema.columns())
                {
                    match index_opt {
                        InsertMode::Direct(index) => {
                            let exprs_len = exprs.len();
                            let expr = exprs.get_mut(*index).context(InsertValuesNotEnough {
                                len: exprs_len,
                                index: *index,
                            })?;

                            match expr {
                                Expr::Value(value) => {
                                    let datum = Datum::try_from_sql_value(
                                        &column_schema.data_type,
                                        mem::replace(value, Value::Null),
                                    )
                                    .context(InsertConvertValue)?;
                                    row_builder =
                                        row_builder.append_datum(datum).context(BuildRow)?;
                                }
                                _ => {
                                    InsertExprNotValue.fail()?;
                                }
                            }
                        }
                        InsertMode::Null => {
                            // This is a null column
                            row_builder =
                                row_builder.append_datum(Datum::Null).context(BuildRow)?;
                        }
                        InsertMode::Auto => {
                            // This is an auto generated column, fill by default value.
                            let kind = &column_schema.data_type;
                            row_builder = row_builder
                                .append_datum(Datum::empty(kind))
                                .context(BuildRow)?;
                        }
                    }
                }

                // Finish this row and append into row group
                row_builder.finish().context(BuildRow)?;
            }

            // Build the whole row group
            Ok(row_group_builder.build())
        }
        _ => InsertSourceBodyNotSet.fail(),
    }
}

#[inline]
fn is_tsid_column(name: &str) -> bool {
    name == TSID_COLUMN
}

fn validate_insert_stmt(
    table_name: &str,
    schema: &Schema,
    column_name_idx: &HashMap<&String, usize>,
) -> Result<()> {
    for name in column_name_idx.keys() {
        if is_tsid_column(name.as_str()) {
            return Err(Error::InsertReservedColumn {
                table: table_name.to_string(),
                column: name.to_string(),
            });
        }
        schema.column_with_name(name).context(UnknownInsertColumn {
            name: name.to_string(),
        })?;
    }

    Ok(())
}

fn parse_options(options: Vec<SqlOption>) -> Result<HashMap<String, String>> {
    let mut parsed_options = HashMap::with_capacity(options.len());

    for option in options {
        let key = option.name.value;
        if let Some(value) = parse_for_option(option.value)? {
            parsed_options.insert(key, value);
        };
    }

    Ok(parsed_options)
}

/// Parse value for sql option.
pub fn parse_for_option(value: Value) -> Result<Option<String>> {
    let value_opt = match value {
        Value::Number(n, _long) => Some(n),
        Value::SingleQuotedString(v) | Value::DoubleQuotedString(v) => Some(v),
        Value::NationalStringLiteral(v) | Value::HexStringLiteral(v) => {
            return UnsupportedOption { value: v }.fail();
        }
        Value::Boolean(v) => Some(v.to_string()),
        Value::Interval { value, .. } => {
            return UnsupportedOption {
                value: value.to_string(),
            }
            .fail();
        }
        // Ignore this option if value is null.
        Value::Null | Value::Placeholder(_) | Value::EscapedStringLiteral(_) => None,
    };

    Ok(value_opt)
}

fn parse_columns(cols: Vec<ColumnDef>) -> Result<Vec<ColumnSchema>> {
    let mut parsed_columns = Vec::with_capacity(cols.len());

    // Build all column schemas.
    for col in &cols {
        parsed_columns.push(parse_column(col)?);
    }

    Ok(parsed_columns)
}

fn parse_column(col: &ColumnDef) -> Result<ColumnSchema> {
    let mut data_type = DatumKind::try_from(&col.data_type).context(UnsupportedDataType)?;

    // Process column options
    let mut is_nullable = true; // A column is nullable by default.
    let mut is_tag = false;
    let mut is_unsign = false;
    let mut comment = String::new();
    for option_def in &col.options {
        if matches!(option_def.option, ColumnOption::NotNull) {
            is_nullable = false;
        } else if parser::is_tag_column(&option_def.option) {
            is_tag = true;
        } else if parser::is_unsign_column(&option_def.option) {
            is_unsign = true;
        } else if let Some(v) = parser::get_column_comment(&option_def.option) {
            comment = v;
        }
    }

    if is_unsign {
        data_type = data_type
            .unsign_kind()
            .context(InvalidUnsignType { kind: data_type })?;
    }

    let builder = column_schema::Builder::new(col.name.value.clone(), data_type)
        .is_nullable(is_nullable)
        .is_tag(is_tag)
        .comment(comment);

    builder.build().context(InvalidColumnSchema {
        column_name: &col.name.value,
    })
}

#[cfg(test)]
mod tests {
    use sqlparser::ast::{Ident, Value};

    use super::*;
    use crate::{
        parser::Parser,
        planner::{parse_for_option, Planner},
        tests::MockMetaProvider,
    };

    fn quick_test(sql: &str, expected: &str) -> Result<()> {
        let mock = MockMetaProvider::default();
        let planner = build_planner(&mock);
        let mut statements = Parser::parse_sql(sql).unwrap();
        assert_eq!(statements.len(), 1);
        let plan = planner.statement_to_plan(statements.remove(0))?;
        assert_eq!(format!("{:#?}", plan), expected);
        Ok(())
    }

    fn build_planner(provider: &MockMetaProvider) -> Planner<MockMetaProvider> {
        Planner::new(provider, RequestId::next_id(), 1)
    }

    #[test]
    pub fn test_parse_for_option() {
        let test_string = "aa".to_string();
        // input is_err expected
        let test_cases = vec![
            (
                Value::Number("1000".to_string(), false),
                false,
                Some("1000".to_string()),
            ),
            (
                Value::SingleQuotedString(test_string.clone()),
                false,
                Some(test_string.clone()),
            ),
            (
                Value::DoubleQuotedString(test_string.clone()),
                false,
                Some(test_string.clone()),
            ),
            (
                Value::NationalStringLiteral(test_string.clone()),
                true,
                None,
            ),
            (Value::HexStringLiteral(test_string.clone()), true, None),
            (Value::Boolean(true), false, Some("true".to_string())),
            (
                Value::Interval {
                    value: Box::new(Expr::Identifier(Ident {
                        value: test_string,
                        quote_style: None,
                    })),
                    leading_field: None,
                    leading_precision: None,
                    last_field: None,
                    fractional_seconds_precision: None,
                },
                true,
                None,
            ),
            (Value::Null, false, None),
        ];

        for (input, is_err, expected) in test_cases {
            let ret = parse_for_option(input);
            assert_eq!(ret.is_err(), is_err);
            if !is_err {
                assert_eq!(ret.unwrap(), expected);
            }
        }
    }

    #[test]
    fn test_create_statement_to_plan() {
        let sql = "CREATE TABLE IF NOT EXISTS t(c1 string tag not null,ts timestamp not null, c3 string, timestamp key(ts),primary key(c1, ts)) \
        ENGINE=Analytic WITH (ttl='70d',update_mode='overwrite',arena_block_size='1KB')";
        quick_test(
            sql,
            r#"Create(
    CreateTablePlan {
        engine: "Analytic",
        if_not_exists: true,
        table: "t",
        table_schema: Schema {
            num_key_columns: 2,
            timestamp_index: 1,
            tsid_index: None,
            enable_tsid_primary_key: false,
            column_schemas: ColumnSchemas {
                columns: [
                    ColumnSchema {
                        id: 1,
                        name: "c1",
                        data_type: String,
                        is_nullable: false,
                        is_tag: true,
                        comment: "",
                        escaped_name: "c1",
                    },
                    ColumnSchema {
                        id: 2,
                        name: "ts",
                        data_type: Timestamp,
                        is_nullable: false,
                        is_tag: false,
                        comment: "",
                        escaped_name: "ts",
                    },
                    ColumnSchema {
                        id: 3,
                        name: "c3",
                        data_type: String,
                        is_nullable: true,
                        is_tag: false,
                        comment: "",
                        escaped_name: "c3",
                    },
                ],
            },
            version: 1,
        },
        options: {
            "arena_block_size": "1KB",
            "ttl": "70d",
            "update_mode": "overwrite",
        },
    },
)"#,
        )
        .unwrap();
    }

    #[test]
    fn test_query_statement_to_plan() {
        let sql = "select * from test_tablex;";
        assert!(quick_test(sql, "").is_err());

        let sql = "select * from test_table;";
        quick_test(sql, "Query(
    QueryPlan {
        df_plan: Projection: #test_table.key1, #test_table.key2, #test_table.field1, #test_table.field2
          TableScan: test_table projection=None,
    },
)").unwrap();
    }

    #[test]
    fn test_insert_statement_to_plan() {
        let sql = "INSERT INTO test_tablex(key1, key2, field1,field2) VALUES('tagk', 1638428434000,100, 'hello3');";
        assert!(quick_test(sql, "").is_err());

        let sql = "INSERT INTO test_table(key1, key2, field1,field2) VALUES('tagk', 1638428434000,100, 'hello3');";
        quick_test(
            sql,
            r#"Insert(
    InsertPlan {
        table: MemoryTable {
            name: "test_table",
            id: TableId(100),
            schema: Schema {
                num_key_columns: 2,
                timestamp_index: 1,
                tsid_index: None,
                enable_tsid_primary_key: false,
                column_schemas: ColumnSchemas {
                    columns: [
                        ColumnSchema {
                            id: 1,
                            name: "key1",
                            data_type: Varbinary,
                            is_nullable: false,
                            is_tag: false,
                            comment: "",
                            escaped_name: "key1",
                        },
                        ColumnSchema {
                            id: 2,
                            name: "key2",
                            data_type: Timestamp,
                            is_nullable: false,
                            is_tag: false,
                            comment: "",
                            escaped_name: "key2",
                        },
                        ColumnSchema {
                            id: 3,
                            name: "field1",
                            data_type: Double,
                            is_nullable: false,
                            is_tag: false,
                            comment: "",
                            escaped_name: "field1",
                        },
                        ColumnSchema {
                            id: 4,
                            name: "field2",
                            data_type: String,
                            is_nullable: false,
                            is_tag: false,
                            comment: "",
                            escaped_name: "field2",
                        },
                    ],
                },
                version: 1,
            },
        },
        rows: RowGroup {
            schema: Schema {
                num_key_columns: 2,
                timestamp_index: 1,
                tsid_index: None,
                enable_tsid_primary_key: false,
                column_schemas: ColumnSchemas {
                    columns: [
                        ColumnSchema {
                            id: 1,
                            name: "key1",
                            data_type: Varbinary,
                            is_nullable: false,
                            is_tag: false,
                            comment: "",
                            escaped_name: "key1",
                        },
                        ColumnSchema {
                            id: 2,
                            name: "key2",
                            data_type: Timestamp,
                            is_nullable: false,
                            is_tag: false,
                            comment: "",
                            escaped_name: "key2",
                        },
                        ColumnSchema {
                            id: 3,
                            name: "field1",
                            data_type: Double,
                            is_nullable: false,
                            is_tag: false,
                            comment: "",
                            escaped_name: "field1",
                        },
                        ColumnSchema {
                            id: 4,
                            name: "field2",
                            data_type: String,
                            is_nullable: false,
                            is_tag: false,
                            comment: "",
                            escaped_name: "field2",
                        },
                    ],
                },
                version: 1,
            },
            rows: [
                Row {
                    cols: [
                        Varbinary(
                            b"tagk",
                        ),
                        Timestamp(
                            Timestamp(
                                1638428434000,
                            ),
                        ),
                        Double(
                            100.0,
                        ),
                        String(
                            StringBytes(
                                b"hello3",
                            ),
                        ),
                    ],
                },
            ],
            min_timestamp: Timestamp(
                1638428434000,
            ),
            max_timestamp: Timestamp(
                1638428434000,
            ),
        },
    },
)"#,
        )
        .unwrap();
    }

    #[test]
    fn test_drop_statement_to_plan() {
        let sql = "drop table test_table;";
        quick_test(
            sql,
            r#"Drop(
    DropTablePlan {
        engine: "Analytic",
        if_exists: false,
        table: "test_table",
    },
)"#,
        )
        .unwrap();

        let sql = "drop table test_tablex;";
        assert!(quick_test(sql, "",).is_err());

        let sql = "drop table if exists test_tablex;";
        quick_test(
            sql,
            r#"Drop(
    DropTablePlan {
        engine: "Analytic",
        if_exists: true,
        table: "test_tablex",
    },
)"#,
        )
        .unwrap();
    }

    #[test]
    fn test_desc_statement_to_plan() {
        let sql = "desc test_tablex;";
        assert!(quick_test(sql, "").is_err());

        let sql = "desc test_table;";
        quick_test(
            sql,
            r#"Describe(
    DescribeTablePlan {
        table: MemoryTable {
            name: "test_table",
            id: TableId(100),
            schema: Schema {
                num_key_columns: 2,
                timestamp_index: 1,
                tsid_index: None,
                enable_tsid_primary_key: false,
                column_schemas: ColumnSchemas {
                    columns: [
                        ColumnSchema {
                            id: 1,
                            name: "key1",
                            data_type: Varbinary,
                            is_nullable: false,
                            is_tag: false,
                            comment: "",
                            escaped_name: "key1",
                        },
                        ColumnSchema {
                            id: 2,
                            name: "key2",
                            data_type: Timestamp,
                            is_nullable: false,
                            is_tag: false,
                            comment: "",
                            escaped_name: "key2",
                        },
                        ColumnSchema {
                            id: 3,
                            name: "field1",
                            data_type: Double,
                            is_nullable: false,
                            is_tag: false,
                            comment: "",
                            escaped_name: "field1",
                        },
                        ColumnSchema {
                            id: 4,
                            name: "field2",
                            data_type: String,
                            is_nullable: false,
                            is_tag: false,
                            comment: "",
                            escaped_name: "field2",
                        },
                    ],
                },
                version: 1,
            },
        },
    },
)"#,
        )
        .unwrap();
    }

    #[test]
    fn test_alter_column_statement_to_plan() {
        let sql = "ALTER TABLE test_tablex ADD column add_col string;";
        assert!(quick_test(sql, "").is_err());

        let sql = "ALTER TABLE test_table ADD column add_col string;";
        quick_test(
            sql,
            r#"AlterTable(
    AlterTablePlan {
        table: MemoryTable {
            name: "test_table",
            id: TableId(100),
            schema: Schema {
                num_key_columns: 2,
                timestamp_index: 1,
                tsid_index: None,
                enable_tsid_primary_key: false,
                column_schemas: ColumnSchemas {
                    columns: [
                        ColumnSchema {
                            id: 1,
                            name: "key1",
                            data_type: Varbinary,
                            is_nullable: false,
                            is_tag: false,
                            comment: "",
                            escaped_name: "key1",
                        },
                        ColumnSchema {
                            id: 2,
                            name: "key2",
                            data_type: Timestamp,
                            is_nullable: false,
                            is_tag: false,
                            comment: "",
                            escaped_name: "key2",
                        },
                        ColumnSchema {
                            id: 3,
                            name: "field1",
                            data_type: Double,
                            is_nullable: false,
                            is_tag: false,
                            comment: "",
                            escaped_name: "field1",
                        },
                        ColumnSchema {
                            id: 4,
                            name: "field2",
                            data_type: String,
                            is_nullable: false,
                            is_tag: false,
                            comment: "",
                            escaped_name: "field2",
                        },
                    ],
                },
                version: 1,
            },
        },
        operations: AddColumn(
            [
                ColumnSchema {
                    id: 0,
                    name: "add_col",
                    data_type: String,
                    is_nullable: true,
                    is_tag: false,
                    comment: "",
                    escaped_name: "add_col",
                },
            ],
        ),
    },
)"#,
        )
        .unwrap();
    }

    #[test]
    fn test_alter_option_statement_to_plan() {
        let sql = "ALTER TABLE test_tablex modify SETTING ttl='9d';";
        assert!(quick_test(sql, "").is_err());

        let sql = "ALTER TABLE test_table modify SETTING ttl='9d';";
        quick_test(
            sql,
            r#"AlterTable(
    AlterTablePlan {
        table: MemoryTable {
            name: "test_table",
            id: TableId(100),
            schema: Schema {
                num_key_columns: 2,
                timestamp_index: 1,
                tsid_index: None,
                enable_tsid_primary_key: false,
                column_schemas: ColumnSchemas {
                    columns: [
                        ColumnSchema {
                            id: 1,
                            name: "key1",
                            data_type: Varbinary,
                            is_nullable: false,
                            is_tag: false,
                            comment: "",
                            escaped_name: "key1",
                        },
                        ColumnSchema {
                            id: 2,
                            name: "key2",
                            data_type: Timestamp,
                            is_nullable: false,
                            is_tag: false,
                            comment: "",
                            escaped_name: "key2",
                        },
                        ColumnSchema {
                            id: 3,
                            name: "field1",
                            data_type: Double,
                            is_nullable: false,
                            is_tag: false,
                            comment: "",
                            escaped_name: "field1",
                        },
                        ColumnSchema {
                            id: 4,
                            name: "field2",
                            data_type: String,
                            is_nullable: false,
                            is_tag: false,
                            comment: "",
                            escaped_name: "field2",
                        },
                    ],
                },
                version: 1,
            },
        },
        operations: ModifySetting(
            {
                "ttl": "9d",
            },
        ),
    },
)"#,
        )
        .unwrap();
    }

    #[test]
    fn test_show_create_statement_to_plan() {
        let sql = "show create table test_tablex;";
        assert!(quick_test(sql, "").is_err());

        let sql = "show create table test_table;";
        quick_test(
            sql,
            r#"Show(
    ShowCreatePlan(
        ShowCreatePlan {
            table: MemoryTable {
                name: "test_table",
                id: TableId(100),
                schema: Schema {
                    num_key_columns: 2,
                    timestamp_index: 1,
                    tsid_index: None,
                    enable_tsid_primary_key: false,
                    column_schemas: ColumnSchemas {
                        columns: [
                            ColumnSchema {
                                id: 1,
                                name: "key1",
                                data_type: Varbinary,
                                is_nullable: false,
                                is_tag: false,
                                comment: "",
                                escaped_name: "key1",
                            },
                            ColumnSchema {
                                id: 2,
                                name: "key2",
                                data_type: Timestamp,
                                is_nullable: false,
                                is_tag: false,
                                comment: "",
                                escaped_name: "key2",
                            },
                            ColumnSchema {
                                id: 3,
                                name: "field1",
                                data_type: Double,
                                is_nullable: false,
                                is_tag: false,
                                comment: "",
                                escaped_name: "field1",
                            },
                            ColumnSchema {
                                id: 4,
                                name: "field2",
                                data_type: String,
                                is_nullable: false,
                                is_tag: false,
                                comment: "",
                                escaped_name: "field2",
                            },
                        ],
                    },
                    version: 1,
                },
            },
            obj_type: Table,
        },
    ),
)"#,
        )
        .unwrap();
    }

    #[test]
    fn test_show_tables_statement_to_plan() {
        let sql = "SHOW TABLES;";
        quick_test(
            sql,
            r#"Show(
    ShowTables,
)"#,
        )
        .unwrap();
    }

    #[test]
    fn test_show_databases_statement_to_plan() {
        let sql = "SHOW DATABASES;";
        quick_test(
            sql,
            r#"Show(
    ShowDatabase,
)"#,
        )
        .unwrap();
    }
}
