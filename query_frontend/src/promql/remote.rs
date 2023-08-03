// Copyright 2023 CeresDB Project Authors. Licensed under Apache-2.0.

//! This module convert Prometheus remote query to datafusion plan.

use std::sync::Arc;

use common_types::{schema::Schema, time::TimeRange};
use datafusion::{
    logical_expr::{LogicalPlanBuilder, Operator},
    optimizer::utils::conjunction,
    prelude::{ident, lit, regexp_match, Expr},
    sql::{planner::ContextProvider, TableReference},
};
use prom_remote_api::types::{label_matcher, LabelMatcher, Query};
use snafu::{OptionExt, ResultExt};

use crate::{
    plan::{Plan, QueryPlan},
    promql::{
        convert::{Selector},
        datafusion_util::{default_sort_exprs, timerange_to_expr},
        error::*,
    },
    provider::{ContextProviderAdapter, MetaProvider},
};

pub const NAME_LABEL: &str = "__name__";
pub const DEFAULT_FIELD_COLUMN: &str = "value";
const FIELD_LABEL: &str = "__ceresdb_field__";

pub struct RemoteQueryPlan {
    pub plan: Plan,
    pub field_col_name: String,
    pub timestamp_col_name: String,
}
/// Generate a plan like this
/// ```plaintext
/// Sort: (tsid, timestamp) asc
///   Project:
///     Filter:
///       TableScan
/// ```
pub fn remote_query_to_plan<P: MetaProvider>(
    query: Query,
    meta_provider: ContextProviderAdapter<'_, P>,
) -> Result<RemoteQueryPlan> {
    let (metric, field, mut filters) = normalize_matchers(query.matchers)?;

    let table_provider = meta_provider
        .get_table_provider(TableReference::bare(&metric))
        .context(TableProviderNotFound { name: &metric })?;
    let schema = Schema::try_from(table_provider.schema()).context(BuildTableSchema)?;
    let timestamp_col_name = schema.timestamp_name();

    // Build datafusion plan
    let filter_exprs = {
        let query_range = TimeRange::new_unchecked(
            query.start_timestamp_ms.into(),
            (query.end_timestamp_ms + 1).into(), // end is inclusive
        );
        filters.push(timerange_to_expr(query_range, timestamp_col_name));
        conjunction(filters).expect("at least one filter(timestamp)")
    };
    let (projection_exprs, _) = Selector::build_projection_tag_keys(&schema, &field)?;
    let sort_exprs = default_sort_exprs(timestamp_col_name);
    let df_plan = LogicalPlanBuilder::scan(metric.clone(), table_provider, None)?
        .filter(filter_exprs)?
        .project(projection_exprs)?
        .sort(sort_exprs)?
        .build()
        .context(BuildPlanError)?;

    let tables = Arc::new(
        meta_provider
            .try_into_container()
            .context(MetaProviderError {
                msg: "Failed to find meta",
            })?,
    );
    Ok(RemoteQueryPlan {
        plan: Plan::Query(QueryPlan { df_plan, tables }),
        field_col_name: field,
        timestamp_col_name: timestamp_col_name.to_string(),
    })
}

/// Extract metric, field from matchers, and convert remaining matchers to
/// datafusion exprs
fn normalize_matchers(matchers: Vec<LabelMatcher>) -> Result<(String, String, Vec<Expr>)> {
    let mut metric = None;
    let mut field = None;
    let mut filters = Vec::with_capacity(matchers.len());
    for m in matchers {
        match m.name.as_str() {
            NAME_LABEL => metric = Some(m.value),
            FIELD_LABEL => field = Some(m.value),
            _ => {
                let col_name = ident(&m.name);
                let expr = match m.r#type() {
                    label_matcher::Type::Eq => col_name.eq(lit(m.value)),
                    label_matcher::Type::Neq => col_name.not_eq(lit(m.value)),
                    // https://github.com/prometheus/prometheus/blob/2ce94ac19673a3f7faf164e9e078a79d4d52b767/model/labels/regexp.go#L29
                    label_matcher::Type::Re => {
                        let tmp = datafusion::logical_expr::BinaryExpr::new(
                            Box::new(col_name),
                            Operator::RegexMatch,
                            Box::new(lit(format!("^(?:{})$", m.value))),
                        );
                        Expr::BinaryExpr(tmp)
                        // regexp_match(vec![col_name, lit(format!("^(?:{})$",
                        // m.value))]) .is_not_null()
                    }
                    label_matcher::Type::Nre => {
                        regexp_match(vec![col_name, lit(format!("^(?:{})$", m.value))]).is_null()
                    }
                };
                filters.push(expr);
            }
        }
    }

    Ok((
        metric.context(InvalidExpr {
            msg: "Metric not found",
        })?,
        field.unwrap_or_else(|| DEFAULT_FIELD_COLUMN.to_string()),
        filters,
    ))
}

#[cfg(test)]
mod tests {
    use prom_remote_api::types::{label_matcher::Type, LabelMatcher};

    use super::*;
    use crate::{promql::remote::NAME_LABEL, tests::MockMetaProvider};

    fn make_matchers(tuples: Vec<(&str, &str, Type)>) -> Vec<LabelMatcher> {
        tuples
            .into_iter()
            .map(|(name, value, matcher_type)| LabelMatcher {
                name: name.to_string(),
                value: value.to_string(),
                r#type: matcher_type as i32,
            })
            .collect()
    }

    #[test]
    fn test_remote_query_to_plan() {
        let meta_provider = MockMetaProvider::default();
        // default value
        {
            let ctx_provider = ContextProviderAdapter::new(&meta_provider, 1);
            let query = Query {
                start_timestamp_ms: 1000,
                end_timestamp_ms: 2000,
                matchers: make_matchers(vec![
                    ("tag1", "some-value", Type::Eq),
                    (NAME_LABEL, "cpu", Type::Eq),
                ]),
                hints: None,
            };
            let RemoteQueryPlan {
                plan,
                field_col_name,
                timestamp_col_name,
            } = remote_query_to_plan(query, ctx_provider).unwrap();
            assert_eq!(
                format!("\n{plan:?}"),
                r#"
Query(QueryPlan { df_plan: Sort: cpu.tsid ASC NULLS FIRST, cpu.time ASC NULLS FIRST
  Projection: cpu.tag1, cpu.tag2, cpu.time, cpu.tsid, cpu.value
    Filter: cpu.tag1 = Utf8("some-value") AND cpu.time BETWEEN Int64(1000) AND Int64(2000)
      TableScan: cpu })"#
                    .to_string()
            );
            assert_eq!(&field_col_name, "value");
            assert_eq!(&timestamp_col_name, "time");
        }

        // field2 value
        {
            let ctx_provider = ContextProviderAdapter::new(&meta_provider, 1);
            let query = Query {
                start_timestamp_ms: 1000,
                end_timestamp_ms: 2000,
                matchers: make_matchers(vec![
                    ("tag1", "some-value", Type::Eq),
                    (NAME_LABEL, "cpu", Type::Eq),
                    (FIELD_LABEL, "field2", Type::Eq),
                ]),
                hints: None,
            };
            let RemoteQueryPlan {
                plan,
                field_col_name,
                timestamp_col_name,
            } = remote_query_to_plan(query, ctx_provider).unwrap();
            assert_eq!(
                format!("\n{plan:?}"),
                r#"
Query(QueryPlan { df_plan: Sort: cpu.tsid ASC NULLS FIRST, cpu.time ASC NULLS FIRST
  Projection: cpu.tag1, cpu.tag2, cpu.time, cpu.tsid, cpu.field2
    Filter: cpu.tag1 = Utf8("some-value") AND cpu.time BETWEEN Int64(1000) AND Int64(2000)
      TableScan: cpu })"#
                    .to_string()
            );
            assert_eq!(&field_col_name, "field2");
            assert_eq!(&timestamp_col_name, "time");
        }
    }

    #[test]
    fn test_normailze_matchers() {
        // no metric
        assert!(normalize_matchers(vec![]).is_err());

        {
            let matchers = make_matchers(vec![
                ("a", "1", Type::Eq),
                ("b", "2", Type::Neq),
                ("c", "3", Type::Re),
                ("D", "4", Type::Nre),
                (NAME_LABEL, "cpu", Type::Eq),
            ]);

            let (metric, field, filters) = normalize_matchers(matchers).unwrap();
            assert_eq!("cpu", metric);
            assert_eq!("value", field);
            assert_eq!(
                r#"a = Utf8("1")
b != Utf8("2")
c ~ Utf8("^(?:3)$")
regexp_match(D, Utf8("^(?:4)$")) IS NULL"#,
                filters
                    .iter()
                    .map(|f| f.to_string())
                    .collect::<Vec<_>>()
                    .join("\n")
            );
        }

        {
            let matchers = make_matchers(vec![
                (NAME_LABEL, "cpu", Type::Eq),
                (FIELD_LABEL, "another-field", Type::Eq),
            ]);

            let (metric, field, filters) = normalize_matchers(matchers).unwrap();
            assert_eq!("cpu", metric);
            assert_eq!("another-field", field);
            assert!(filters.is_empty());
        }
    }
}
