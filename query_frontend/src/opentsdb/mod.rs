use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

use common_types::{
    schema::{Schema, TSID_COLUMN},
    time::{TimeRange, Timestamp},
};
use datafusion::{
    error::DataFusionError,
    logical_expr::LogicalPlanBuilder,
    optimizer::utils::conjunction,
    prelude::{avg, col, count, ident, lit, max, min, stddev, sum, Expr},
    sql::{planner::ContextProvider, TableReference},
};
use macros::define_result;
use snafu::{OptionExt, ResultExt, Snafu};

use self::types::{Filter, OpentsdbQueryPlan, OpentsdbSubPlan, QueryRequest, SubQuery};
use crate::{
    datafusion_util::{default_sort_exprs, timerange_to_expr},
    plan::{Plan, QueryPlan},
    provider::{ContextProviderAdapter, MetaProvider},
};

pub mod types;

const DEFAULT_FIELD: &str = "value";

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Table provider not found, table:{name}, err:{source}"))]
    TableProviderNotFound {
        name: String,
        source: DataFusionError,
    },

    #[snafu(display("Failed to build schema, err:{source}"))]
    BuildTableSchema { source: common_types::schema::Error },

    #[snafu(display("Query end should larger than start, start:{start}, end:{end}"))]
    InvalidRange { start: i64, end: i64 },

    #[snafu(display("Invalid filter, value:{filter_type}"))]
    InvalidFilter { filter_type: String },

    #[snafu(display("Invalid aggregator, value:{aggr}"))]
    InvalidAggregator { aggr: String },

    #[snafu(display("Failed to build plan, source:{source}"))]
    BuildPlanError { source: DataFusionError },

    #[snafu(display("MetaProvider {msg}, err:{source}"))]
    MetaProviderError {
        msg: String,
        source: crate::provider::Error,
    },
}

impl From<DataFusionError> for Error {
    fn from(df_err: DataFusionError) -> Self {
        Error::BuildPlanError { source: df_err }
    }
}

define_result!(Error);

fn normalize_filters(
    tags: HashMap<String, String>,
    filters: Vec<Filter>,
) -> Result<(Vec<String>, Vec<Expr>)> {
    let mut groupby_col_names = Vec::new();
    let mut exprs = Vec::with_capacity(tags.len() + filters.len());
    for (tagk, tagv) in tags {
        exprs.push(ident(tagk).eq(lit(tagv)));
        groupby_col_names.push(tagk);
    }

    for filter in filters {
        if filter.group_by {
            groupby_col_names.push(filter.tagk);
        }

        let col_name = ident(filter.tagk);
        // http://opentsdb.net/docs/build/html/user_guide/query/filters.html
        let value = match filter.r#type.as_str() {
            "literal_or" => {
                let vs = filter.filter.split('|').map(lit).collect();
                col_name.in_list(vs, false)
            }
            "not_literal_or" => {
                let vs = filter.filter.split('|').map(lit).collect();
                col_name.in_list(vs, true)
            }
            filter_type => return InvalidFilter { filter_type }.fail(),
        };
    }

    Ok((groupby_col_names, exprs))
}

fn build_projection(
    timestamp_col_name: &str,
    tags: &HashMap<String, String>,
    filters: &[Filter],
) -> Vec<Expr> {
    let mut projections = tags.keys().map(ident).collect::<HashSet<_>>();
    projections.extend(filters.iter().map(|f| ident(f.tagk)));
    projections.insert(ident(timestamp_col_name));
    projections.insert(ident(TSID_COLUMN));

    projections.into_iter().collect()
}

fn build_aggr_expr(aggr: &str) -> Result<Option<Expr>> {
    // http://opentsdb.net/docs/build/html/user_guide/query/aggregators.html
    let aggr = match aggr {
        "sum" => sum(col(DEFAULT_FIELD)),
        "count" => count(col(DEFAULT_FIELD)),
        "avg" => avg(col(DEFAULT_FIELD)),
        "min" => min(col(DEFAULT_FIELD)),
        "max" => max(col(DEFAULT_FIELD)),
        "dev" => stddev(col(DEFAULT_FIELD)),
        "none" => return Ok(None),
        v => return InvalidAggregator { aggr }.fail(),
    };

    Ok(Some(aggr))
}

pub fn subquery_to_plan<P: MetaProvider>(
    meta_provider: &ContextProviderAdapter<'_, P>,
    query_range: &TimeRange,
    sub_query: SubQuery,
) -> Result<OpentsdbSubPlan> {
    let metric = sub_query.metric;
    let table_provider = meta_provider
        .get_table_provider(TableReference::bare(&metric))
        .context(TableProviderNotFound { name: &metric })?;
    let schema = Schema::try_from(table_provider.schema()).context(BuildTableSchema)?;
    let timestamp_col_name = schema.timestamp_name();
    let projection_exprs =
        build_projection(timestamp_col_name, &sub_query.tags, &sub_query.filters);
    let (groupby_col_names, filter_exprs) = {
        let (groupby, mut filters) = normalize_filters(sub_query.tags, sub_query.filters)?;
        filters.push(timerange_to_expr(query_range, timestamp_col_name));
        let anded_filters = conjunction(filters).expect("at least one filter(timestamp)");
        (groupby, anded_filters)
    };

    let sort_exprs = default_sort_exprs(timestamp_col_name);
    let mut builder = LogicalPlanBuilder::scan(metric.clone(), table_provider, None)?
        .filter(filter_exprs)?
        .project(projection_exprs)?
        .sort(sort_exprs)?;

    if let Some(aggr_expr) = build_aggr_expr(&sub_query.aggregator)? {
        let group_expr = groupby_col_names.iter().map(ident).collect::<Vec<_>>();
        builder = builder.aggregate(group_expr, [aggr_expr])?;
    }

    let df_plan = builder.build().context(BuildPlanError)?;
    let tables = Arc::new(
        meta_provider
            .try_into_container()
            .context(MetaProviderError {
                msg: "Failed to find meta",
            })?,
    );

    Ok(OpentsdbSubPlan {
        plan: Plan::Query(QueryPlan { df_plan, tables }),
        timestamp_col_name: timestamp_col_name.to_string(),
        field_col_name: DEFAULT_FIELD.to_string(),
    })
}

pub fn opentsdb_query_to_plan<P: MetaProvider>(
    query: QueryRequest,
    meta_provider: ContextProviderAdapter<'_, P>,
) -> Result<OpentsdbQueryPlan> {
    let range = TimeRange::new(Timestamp::new(query.start), Timestamp::new(query.end + 1))
        .context(InvalidRange {
            start: query.start,
            end: query.end,
        })?;

    let plans = query
        .queries
        .into_iter()
        .map(|sub_query| subquery_to_plan(&meta_provider, &range, sub_query))
        .collect::<Result<Vec<_>>>()?;

    Ok(OpentsdbQueryPlan { plans })
}
