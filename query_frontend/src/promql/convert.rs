// Copyright 2022-2023 CeresDB Project Authors. Licensed under Apache-2.0.

use std::{convert::TryFrom, sync::Arc};

use ceresdbproto::prometheus::{
    self, operand, sub_expr::OperatorType, Expr as ExprPb, Filter as FilterPb,
    FilterType as FilterPbType, Operand as OperandPb, Selector as PbSelector, SubExpr as PbSubExpr,
};
use common_types::{
    schema::{Schema, TSID_COLUMN},
    time::{TimeRange, Timestamp},
};
use datafusion::{
    error::DataFusionError,
    logical_expr::{
        avg, col, count, lit,
        logical_plan::{Extension, LogicalPlan, LogicalPlanBuilder},
        max, min, sum, Expr as DataFusionExpr,
    },
    optimizer::utils::conjunction,
    sql::planner::ContextProvider,
};
use snafu::{ensure, Backtrace, OptionExt, ResultExt, Snafu};

use crate::{
    plan::{Plan, QueryPlan},
    promql::{
        datafusion_util::{default_sort_exprs, timerange_to_expr},
        pushdown::{AlignParameter, Func},
        udf::{create_unique_id, regex_match_expr},
        ColumnNames, PromAlignNode,
    },
    provider::{ContextProviderAdapter, MetaProvider},
};

const INIT_LEVEL: usize = 1;
const DEFAULT_LOOKBACK: i64 = 300_000;

#[allow(clippy::large_enum_variant)]
#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Invalid expr, expected: {}, actual:{:?}", expected, actual))]
    UnexpectedExpr { expected: String, actual: String },

    #[snafu(display("Expr pushdown not implemented, expr_type:{:?}", expr_type))]
    NotImplemented { expr_type: OperatorType },

    #[snafu(display("MetaProvider {}, err:{}", msg, source))]
    MetaProviderError {
        msg: String,
        source: crate::provider::Error,
    },

    #[snafu(display("Table not found, table:{}", name))]
    TableNotFound { name: String },

    #[snafu(display("Table provider not found, table:{}, err:{}", name, source))]
    TableProviderNotFound {
        name: String,
        source: DataFusionError,
    },

    #[snafu(display("Failed to build schema, err:{}", source))]
    BuildTableSchema { source: common_types::schema::Error },

    #[snafu(display("Failed to build plan, source:{}", source,))]
    BuildPlanError { source: DataFusionError },

    #[snafu(display("Invalid expr, msg:{}\nBacktrace:\n{}", msg, backtrace))]
    InvalidExpr { msg: String, backtrace: Backtrace },

    #[snafu(display("Failed to pushdown, source:{}", source))]
    PushdownError {
        source: crate::promql::pushdown::Error,
    },
}

define_result!(Error);

impl From<DataFusionError> for Error {
    fn from(df_err: DataFusionError) -> Self {
        Error::BuildPlanError { source: df_err }
    }
}

#[derive(Debug, Clone)]
pub enum Expr {
    SimpleExpr(Operand),
    RecursiveExpr(SubExpr),
}

impl TryFrom<OperandPb> for Expr {
    type Error = Error;

    fn try_from(pb_operand: OperandPb) -> Result<Self> {
        if let Some(v) = pb_operand.value {
            let op = match v {
                operand::Value::Selector(selector) => {
                    let PbSelector {
                        measurement: table,
                        start,
                        end,
                        align_start,
                        align_end,
                        filters,
                        range,
                        field,
                        offset,
                        step,
                        ..
                    } = selector;
                    let filters = filters.into_iter().map(Filter::from).collect::<Vec<_>>();
                    Operand::Selector(Selector {
                        table,
                        filters,
                        field,
                        query_range: TimeRange::new_unchecked(
                            Timestamp::new(start),
                            Timestamp::new(end + 1),
                        ), /* [start, end] */
                        align_range: TimeRange::new_unchecked(
                            Timestamp::new(align_start),
                            Timestamp::new(align_end + 1),
                        ), /* [align_start, align_end] */
                        step,
                        range,
                        offset,
                    })
                }
                operand::Value::FloatVal(val) => Operand::Float(val),
                operand::Value::StringVal(val) => Operand::String(val),
            };
            Ok(Expr::SimpleExpr(op))
        } else {
            InvalidExpr {
                msg: "unknown operand".to_string(),
            }
            .fail()
        }
    }
}

impl TryFrom<ExprPb> for Expr {
    type Error = Error;

    fn try_from(expr: ExprPb) -> Result<Self> {
        if let Some(expr_node) = expr.node {
            match expr_node {
                prometheus::expr::Node::Operand(v) => Expr::try_from(v),
                prometheus::expr::Node::SubExpr(v) => Expr::try_from(v),
            }
        } else {
            InvalidExpr {
                msg: "unknown expr".to_string(),
            }
            .fail()
        }
    }
}

impl Expr {
    pub fn selector(&self) -> &Selector {
        match self {
            Expr::SimpleExpr(op) => match op {
                Operand::Selector(sel) => sel,
                _ => unreachable!(),
            },
            Expr::RecursiveExpr(sub_expr) => sub_expr.selector(),
        }
    }

    pub fn is_selector(&self) -> bool {
        matches!(self, Expr::SimpleExpr(e) if matches!(e, Operand::Selector(_)))
    }

    /// For now, only filters and timestamp are pushdown, we translate it
    /// into plan like:
    /// Aggregate: (when needed)
    ///   PromAlign:
    ///     Sort: (tsid, timestamp) asc
    ///       Project:
    ///         Filter:
    ///           TableScan
    pub fn to_plan<P: MetaProvider>(
        self,
        meta_provider: ContextProviderAdapter<'_, P>,
        read_parallelism: usize,
    ) -> Result<(Plan, Arc<ColumnNames>)> {
        let (logic_plan, column_name, _) =
            self.build_plan_iter(&meta_provider, INIT_LEVEL, read_parallelism)?;
        let tables = Arc::new(
            meta_provider
                .try_into_container()
                .context(MetaProviderError {
                    msg: "failed to find meta",
                })?,
        );
        Ok((
            Plan::Query(QueryPlan {
                df_plan: logic_plan,
                tables,
            }),
            column_name,
        ))
    }

    fn build_plan_iter<P: MetaProvider>(
        self,
        meta_provider: &ContextProviderAdapter<'_, P>,
        level: usize,
        read_parallelism: usize,
    ) -> Result<(LogicalPlan, Arc<ColumnNames>, String)> {
        match self {
            Expr::SimpleExpr(simple_expr) => match simple_expr {
                Operand::Selector(selector) => {
                    let (sub_plan, column_name, table_name) =
                        selector.clone().into_scan_plan(meta_provider)?;
                    if level == INIT_LEVEL {
                        // when only selector is pushdown, align is done in Prometheus itself
                        // since maybe there are subquery inside one query which require complex
                        // align logic.
                        return Ok((sub_plan, column_name, table_name));
                    }
                    // insert PromAlignNode into plan with Func::Instant
                    let Selector {
                        align_range,
                        step,
                        offset,
                        ..
                    } = selector;
                    let align_param = AlignParameter {
                        align_range,
                        step: step.into(),
                        offset: offset.into(),
                        lookback_delta: DEFAULT_LOOKBACK.into(),
                    };
                    let align_plan = LogicalPlan::Extension(Extension {
                        node: Arc::new(PromAlignNode {
                            input: sub_plan,
                            func: Func::Instant,
                            table_name: table_name.clone(),
                            align_param,
                            column_name: column_name.clone(),
                            read_parallelism,
                        }),
                    });
                    Ok((align_plan, column_name, table_name))
                }
                Operand::Float(_) | Operand::String(_) => InvalidExpr {
                    msg: "scalar value not allowed in plan node",
                }
                .fail(),
            },
            // New plan like:
            // PromAlign:
            //   SubPlan
            Expr::RecursiveExpr(recursive_expr) => match recursive_expr {
                SubExpr::Func(FuncExpr { op, operands }) => {
                    assert!(!operands.is_empty());
                    let func = Func::try_from(op.as_str()).context(PushdownError {})?;
                    let first_arg = &operands[0];
                    if first_arg.is_selector() {
                        let selector = first_arg.selector();
                        let (sub_plan, column_name, table_name) =
                            selector.clone().into_scan_plan(meta_provider)?;
                        let Selector {
                            align_range,
                            step,
                            range,
                            offset,
                            ..
                        } = selector;
                        let align_param = AlignParameter {
                            align_range: *align_range,
                            step: step.into(),
                            offset: offset.into(),
                            lookback_delta: range.into(),
                        };
                        let align_plan = LogicalPlan::Extension(Extension {
                            node: Arc::new(PromAlignNode {
                                input: sub_plan,
                                table_name: table_name.clone(),
                                func,
                                align_param,
                                column_name: column_name.clone(),
                                read_parallelism,
                            }),
                        });
                        return Ok((align_plan, column_name, table_name));
                    }
                    InvalidExpr {
                        msg: "first arg of func must be selector",
                    }
                    .fail()
                }

                // New plan like:
                // Sort:
                //   Projection
                //     Aggregate
                //       SubPlan
                SubExpr::Aggr(AggrExpr {
                    op,
                    operands,
                    group_by,
                    without,
                }) => {
                    assert!(!operands.is_empty());
                    let next_level = level + 1;
                    // aggregators don't have args, only need to deal with sub_node now.
                    let sub_node = operands.into_iter().next().unwrap();
                    let (sub_plan, column_name, table_name) =
                        sub_node.build_plan_iter(meta_provider, next_level, read_parallelism)?;
                    // filter out nonexistent tags
                    let group_by = group_by
                        .into_iter()
                        .filter(|by| column_name.tag_keys.contains(by))
                        .collect::<Vec<_>>();
                    let groupby_columns = if without {
                        column_name
                            .tag_keys
                            .iter()
                            .filter_map(|tag_key| {
                                if group_by.contains(tag_key) {
                                    None
                                } else {
                                    Some(tag_key.as_str())
                                }
                            })
                            .collect::<Vec<_>>()
                    } else {
                        group_by.iter().map(|s| (s.as_str())).collect::<Vec<_>>()
                    };
                    let aggr_expr =
                        Self::aggr_op_expr(&op, &column_name.field, column_name.field.clone())?;
                    let tag_exprs = groupby_columns.iter().map(|v| col(*v)).collect::<Vec<_>>();
                    let udf_args = tag_exprs.clone();
                    let mut groupby_expr = vec![col(&column_name.timestamp)];
                    groupby_expr.extend(udf_args);
                    let unique_id_expr =
                        // TSID is lost after aggregate, but PromAlignNode need a unique id, so
                        // mock UUID as tsid based on groupby keys
                        DataFusionExpr::Alias(
                            Box::new(DataFusionExpr::ScalarUDF {
                                fun: Arc::new(create_unique_id(tag_exprs.len())),
                                args: tag_exprs.clone(),
                            }),
                            TSID_COLUMN.to_string(),
                        );
                    let mut projection = tag_exprs.clone();
                    projection.extend(vec![
                        col(&column_name.timestamp),
                        col(&column_name.field),
                        unique_id_expr.clone(),
                    ]);
                    let sort_exprs = if tag_exprs.is_empty() {
                        vec![col(&column_name.timestamp).sort(true, true)]
                    } else {
                        vec![
                            unique_id_expr.sort(true, true),
                            col(&column_name.timestamp).sort(true, true),
                        ]
                    };
                    let builder = LogicalPlanBuilder::from(sub_plan);
                    let plan = builder
                        .aggregate(groupby_expr, vec![aggr_expr])?
                        .project(projection)?
                        .sort(sort_exprs)?
                        .build()?;

                    Ok((plan, column_name, table_name))
                }
                SubExpr::Binary(_) => InvalidExpr {
                    msg: "binary Expr not supported",
                }
                .fail(),
            },
        }
    }

    fn aggr_op_expr(aggr_op: &str, field: &str, alias: String) -> Result<DataFusionExpr> {
        let expr = match aggr_op {
            "sum" => sum(col(field)),
            "max" => max(col(field)),
            "min" => min(col(field)),
            "count" => count(col(field)),
            "avg" => avg(col(field)),
            _ => {
                return InvalidExpr {
                    msg: format!("aggr {aggr_op} not supported now"),
                }
                .fail()
            }
        };

        Ok(DataFusionExpr::Alias(Box::new(expr), alias))
    }
}

#[derive(Debug, Clone)]
pub enum Operand {
    String(String),
    Float(f64),
    Selector(Selector),
}

#[derive(Debug, Clone)]
pub enum SubExpr {
    Aggr(AggrExpr),
    Func(FuncExpr),
    Binary(BinaryExpr),
}

impl TryFrom<PbSubExpr> for Expr {
    type Error = Error;

    fn try_from(pb_sub_expr: PbSubExpr) -> Result<Self> {
        let op_type = pb_sub_expr.op_type();

        let operator = pb_sub_expr.operator;
        let operands = pb_sub_expr
            .operands
            .into_iter()
            .map(Expr::try_from)
            .collect::<Result<Vec<_>>>()?;
        let sub_expr = match op_type {
            OperatorType::Aggr => SubExpr::Aggr(AggrExpr {
                op: operator,
                operands,
                group_by: pb_sub_expr.group,
                without: pb_sub_expr.without,
            }),
            OperatorType::Func => SubExpr::Func(FuncExpr {
                op: operator,
                operands,
            }),
            OperatorType::Binary => {
                return NotImplemented {
                    expr_type: OperatorType::Binary,
                }
                .fail()
            }
        };

        Ok(Expr::RecursiveExpr(sub_expr))
    }
}

impl SubExpr {
    pub fn selector(&self) -> &Selector {
        match self {
            SubExpr::Aggr(AggrExpr { operands, .. }) => operands[0].selector(),
            SubExpr::Func(FuncExpr { operands, .. }) => operands[0].selector(),
            SubExpr::Binary(BinaryExpr { operands, .. }) => operands[0].selector(),
        }
    }

    pub fn is_range_fn(&self) -> bool {
        match self {
            Self::Func(FuncExpr { operands, .. }) => match &operands[0] {
                Expr::SimpleExpr(Operand::Selector(sel)) => sel.range > 0,
                _ => false,
            },
            _ => false,
        }
    }
}

#[derive(Debug, Clone)]
pub struct AggrExpr {
    op: String,
    operands: Vec<Expr>,
    group_by: Vec<String>,
    without: bool,
}

#[derive(Debug, Clone)]
pub struct FuncExpr {
    op: String,
    operands: Vec<Expr>,
}

#[derive(Debug, Clone)]
pub struct BinaryExpr {
    _op: String,
    operands: Vec<Expr>,
    _return_bool: bool,
}

#[derive(Debug, Clone)]
pub enum FilterType {
    LiteralOr,
    NotLiteralOr,
    Regexp,
    NotRegexpMatch,
}

impl From<FilterPbType> for FilterType {
    fn from(pb_type: FilterPbType) -> Self {
        match pb_type {
            FilterPbType::LiteralOr => FilterType::LiteralOr,
            FilterPbType::NotLiteralOr => FilterType::NotLiteralOr,
            FilterPbType::Regexp => FilterType::Regexp,
            FilterPbType::NotRegexpMatch => FilterType::NotRegexpMatch,
        }
    }
}

#[derive(Debug, Clone)]
pub struct FilterOperator {
    typ: FilterType,
    params: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct Filter {
    tag_key: String,
    operators: Vec<FilterOperator>,
}

impl From<Filter> for DataFusionExpr {
    fn from(mut f: Filter) -> DataFusionExpr {
        let tag_key = col(&f.tag_key);
        // TODO(chenxiang): only compare first op now
        let mut first_op = f.operators.remove(0);
        match first_op.typ {
            // regexp filter only have one param
            FilterType::Regexp => regex_match_expr(tag_key, first_op.params.remove(0), true),
            FilterType::NotRegexpMatch => {
                regex_match_expr(tag_key, first_op.params.remove(0), false)
            }
            FilterType::LiteralOr => tag_key.in_list(
                first_op
                    .params
                    .iter()
                    .map(|v| lit(v.as_str()))
                    .collect::<Vec<_>>(),
                false,
            ),
            FilterType::NotLiteralOr => tag_key.in_list(
                first_op
                    .params
                    .iter()
                    .map(|v| lit(v.as_str()))
                    .collect::<Vec<_>>(),
                true,
            ),
        }
    }
}

impl From<FilterPb> for Filter {
    fn from(pb_filter: FilterPb) -> Self {
        Self {
            tag_key: pb_filter.tag_key,
            operators: pb_filter
                .operators
                .into_iter()
                .map(|f| FilterOperator {
                    typ: f.filter_type().into(),
                    params: f.params,
                })
                .collect::<Vec<_>>(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct Selector {
    // query params
    pub query_range: TimeRange,
    pub table: String,
    pub filters: Vec<Filter>,
    pub field: String,

    // align params
    pub align_range: TimeRange,
    pub step: i64,
    pub range: i64,
    pub offset: i64,
}

impl Selector {
    fn into_scan_plan<P: MetaProvider>(
        self,
        meta_provider: &ContextProviderAdapter<'_, P>,
    ) -> Result<(LogicalPlan, Arc<ColumnNames>, String)> {
        let Selector {
            query_range,
            field,
            filters,
            table,
            ..
        } = self;
        let table_ref = meta_provider
            .table(table.as_str().into())
            .context(MetaProviderError {
                msg: "failed to find table".to_string(),
            })?
            .context(TableNotFound { name: &table })?;

        let table_provider = meta_provider
            .get_table_provider(table_ref.name().into())
            .context(TableProviderNotFound { name: &table })?;
        let schema = Schema::try_from(table_provider.schema()).context(BuildTableSchema)?;
        let timestamp_column_name = schema.timestamp_name().to_string();
        let (projection, tag_keys) = Self::build_projection_tag_keys(&schema, &field)?;
        let mut filter_exprs = filters
            .iter()
            .filter_map(|f| {
                // drop non_exist filter
                if tag_keys.contains(&f.tag_key) {
                    Some(DataFusionExpr::from(f.clone()))
                } else {
                    None
                }
            })
            .collect::<Vec<_>>();
        filter_exprs.push(timerange_to_expr(query_range, &timestamp_column_name));

        let builder = LogicalPlanBuilder::scan(table.clone(), table_provider, None)?
            .filter(conjunction(filter_exprs).expect("at least one filter(timestamp)"))?
            .project(projection)?
            .sort(default_sort_exprs(&timestamp_column_name))?;
        let column_name = Arc::new(ColumnNames {
            timestamp: timestamp_column_name,
            tag_keys,
            field,
        });
        let scan_plan = builder.build().context(BuildPlanError)?;
        Ok((scan_plan, column_name, table))
    }

    fn build_projection_tag_keys(
        schema: &Schema,
        field: &str,
    ) -> Result<(Vec<DataFusionExpr>, Vec<String>)> {
        if let Some(f) = schema.column_with_name(field) {
            ensure!(
                f.data_type.is_f64_castable(),
                InvalidExpr {
                    msg: "field type must be f64-compatibile type",
                }
            );
        } else {
            return InvalidExpr {
                msg: format!("field:{field} not found"),
            }
            .fail();
        };
        let mut tag_keys = Vec::new();
        let mut projection = schema
            .columns()
            .iter()
            .filter_map(|column| {
                if column.is_tag {
                    tag_keys.push(column.name.clone());
                    Some(col(&column.name))
                } else {
                    None
                }
            })
            .collect::<Vec<_>>();

        let timestamp_expr = col(&schema.column(schema.timestamp_index()).name);
        let tsid_expr = schema
            .tsid_column()
            .map(|c| col(&c.name))
            .context(InvalidExpr {
                msg: format!("{TSID_COLUMN} not found"),
            })?;
        let field_expr = col(field);
        projection.extend(vec![timestamp_expr, tsid_expr, field_expr]);

        Ok((projection, tag_keys))
    }
}
