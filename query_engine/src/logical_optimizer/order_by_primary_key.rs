// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

use std::{convert::TryFrom, sync::Arc};

use common_types::schema::Schema;
use datafusion::{
    logical_plan::{
        plan::{Extension, Filter, Projection, Sort},
        DFSchemaRef, Expr, Limit, LogicalPlan, TableScan,
    },
    optimizer::{optimizer::OptimizerRule, OptimizerConfig},
};
use log::info;

use crate::df_planner_extension::table_scan_by_primary_key::TableScanByPrimaryKey;

/// The optimizer rule applies to the example plan:
/// Limit: 1
///   Sort: #test.id ASC NULLS FIRST, #test.t ASC NULLS FIRST
///     Projection: #test.tsid, #test.t, #test.id, #test.tag1, #test.tag2
///       TableScan: test projection=None
pub struct OrderByPrimaryKeyRule;

impl OrderByPrimaryKeyRule {
    /// Optimize the plan if it is the pattern:
    /// Limit:
    ///   Sort:
    ///     Project:
    ///       (Filter): (Filer node is allowed to be not exist)
    ///         TableScan
    fn do_optimize(&self, plan: &LogicalPlan) -> datafusion::error::Result<Option<LogicalPlan>> {
        if let LogicalPlan::Limit(Limit {
            skip,
            fetch,
            input: sort_plan,
        }) = plan
        {
            if let LogicalPlan::Sort(Sort {
                expr: sort_exprs,
                input: projection_plan,
            }) = sort_plan.as_ref()
            {
                if let LogicalPlan::Projection(Projection {
                    expr: projection_exprs,
                    input: scan_or_filter_plan,
                    schema: projection_schema,
                    alias,
                }) = projection_plan.as_ref()
                {
                    let (scan_plan, filter_predicate) = if let LogicalPlan::Filter(Filter {
                        predicate,
                        input: scan_plan,
                    }) = scan_or_filter_plan.as_ref()
                    {
                        (scan_plan, Some(predicate))
                    } else {
                        (scan_or_filter_plan, None)
                    };

                    if let LogicalPlan::TableScan(TableScan {
                        table_name, source, ..
                    }) = scan_plan.as_ref()
                    {
                        let schema = Schema::try_from(source.schema()).map_err(|e| {
                            let err_msg = format!(
                                "fail to convert arrow schema to schema, table:{}, err:{:?}",
                                table_name, e
                            );
                            datafusion::error::DataFusionError::Plan(err_msg)
                        })?;
                        if let Some(sort_in_asc_order) =
                            Self::detect_primary_key_order(&schema, sort_exprs.as_slice())
                        {
                            let new_plan = Self::rewrite_plan(RewriteContext {
                                projection: projection_exprs.clone(),
                                filter_predicate: filter_predicate.cloned(),
                                schema: projection_schema.clone(),
                                alias: alias.clone(),
                                scan_plan: scan_plan.clone(),
                                sort_exprs: sort_exprs.clone(),
                                sort_in_asc_order,
                                skip: *skip,
                                fetch: *fetch,
                            });
                            return Ok(Some(new_plan));
                        }
                    }
                }
            }
        }

        Ok(None)
    }

    /// Check:
    ///  - Whether `timestamp` is the first column in the primary key.
    ///  - Whether `sort_exprs` is equals the any prefix of primary key.
    ///  - Whether `sort_exprs` is in the same order.
    ///
    /// Returns: Some(sort_order) if the two rules above are true.
    fn detect_primary_key_order(schema: &Schema, sort_exprs: &[Expr]) -> Option<bool> {
        if schema.timestamp_index() != 0 {
            return None;
        }

        let key_cols = schema.key_columns();
        if sort_exprs.len() > key_cols.len() {
            return None;
        }
        let sub_key_cols = &key_cols[..sort_exprs.len()];

        let mut in_asc_order = None;
        for (sort_expr, key_col) in sort_exprs.iter().zip(sub_key_cols.iter()) {
            if let Expr::Sort { expr, asc, .. } = sort_expr {
                if let Some(in_asc_order) = in_asc_order.as_mut() {
                    if in_asc_order != asc {
                        return None;
                    }
                }
                in_asc_order = Some(*asc);

                if let Expr::Column(col) = expr.as_ref() {
                    if col.name == key_col.name {
                        continue;
                    }
                }
            }
            return None;
        }

        in_asc_order
    }

    // TODO(xikai): The topmost limit and sort plan node of the rewritten plan is
    //  not necessary now  because now the rewrite requires the timestamp key is
    //  the first column in the primary key  and that means the output of
    //  TableScanByPrimaryKey is in the correct order. And topmost two
    //  plan nodes is used to optimize the normal cases where the timestamp key is
    //  any column.
    /// Rewrite the plan:
    /// Limit:
    ///   Sort:
    ///     Project:
    ///       Filter:
    ///         TableScan
    ///
    /// Rewritten plan:
    /// Limit:
    ///   Sort:
    ///     Limit:
    ///       Project:
    ///         Filter:
    ///           TableScanByPrimaryKey
    fn rewrite_plan(rewrite_ctx: RewriteContext) -> LogicalPlan {
        let order_by_primary_key_scan = Arc::new(LogicalPlan::Extension(Extension {
            node: Arc::new(TableScanByPrimaryKey::new_from_scan_plan(
                rewrite_ctx.sort_in_asc_order,
                rewrite_ctx.scan_plan,
            )),
        }));

        let filter_plan = if let Some(predicate) = rewrite_ctx.filter_predicate {
            Arc::new(LogicalPlan::Filter(Filter {
                predicate,
                input: order_by_primary_key_scan,
            }))
        } else {
            order_by_primary_key_scan
        };

        let new_project_plan = Arc::new(LogicalPlan::Projection(Projection {
            expr: rewrite_ctx.projection,
            input: filter_plan,
            schema: rewrite_ctx.schema,
            alias: rewrite_ctx.alias,
        }));

        let new_limit_plan = Arc::new(LogicalPlan::Limit(Limit {
            skip: rewrite_ctx.skip,
            fetch: rewrite_ctx.fetch,
            input: new_project_plan,
        }));

        let new_sort_plan = Arc::new(LogicalPlan::Sort(Sort {
            expr: rewrite_ctx.sort_exprs,
            input: new_limit_plan,
        }));
        LogicalPlan::Limit(Limit {
            skip: rewrite_ctx.skip,
            fetch: rewrite_ctx.fetch,
            input: new_sort_plan,
        })
    }
}

impl OptimizerRule for OrderByPrimaryKeyRule {
    fn optimize(
        &self,
        plan: &LogicalPlan,
        _optimizer_config: &mut OptimizerConfig,
    ) -> datafusion::error::Result<LogicalPlan> {
        match self.do_optimize(plan)? {
            Some(new_plan) => {
                info!(
                     "optimize plan by OrderByPrimaryKeyRule, original plan:\n{:?}\n optimized plan:\n{:?}",
                     plan, new_plan
                 );
                Ok(new_plan)
            }
            None => Ok(plan.clone()),
        }
    }

    fn name(&self) -> &str {
        "order_by_primary_key"
    }
}

struct RewriteContext {
    projection: Vec<Expr>,
    filter_predicate: Option<Expr>,
    schema: DFSchemaRef,
    alias: Option<String>,
    scan_plan: Arc<LogicalPlan>,
    sort_exprs: Vec<Expr>,
    sort_in_asc_order: bool,
    skip: usize,
    fetch: Option<usize>,
}

#[cfg(test)]
mod tests {
    use common_types::{column_schema, datum::DatumKind, schema};
    use datafusion::{logical_plan::Column, scalar::ScalarValue};

    use super::*;
    use crate::logical_optimizer::tests::LogicalPlanNodeBuilder;

    const TEST_TABLE_NAME: &str = "order_by_primary_key_test_table";

    fn build_no_optimized_schema() -> Schema {
        schema::Builder::new()
            .auto_increment_column_id(true)
            .add_key_column(
                column_schema::Builder::new("key".to_string(), DatumKind::Varbinary)
                    .build()
                    .expect("Build column schema"),
            )
            .unwrap()
            .add_key_column(
                column_schema::Builder::new("t".to_string(), DatumKind::Timestamp)
                    .build()
                    .expect("Build column schema"),
            )
            .unwrap()
            .add_normal_column(
                column_schema::Builder::new("field".to_string(), DatumKind::Double)
                    .build()
                    .expect("Build column schema"),
            )
            .unwrap()
            .build()
            .expect("Build schema")
    }

    fn build_optimized_schema() -> Schema {
        schema::Builder::new()
            .auto_increment_column_id(true)
            .add_key_column(
                column_schema::Builder::new("t".to_string(), DatumKind::Timestamp)
                    .build()
                    .expect("Build column schema"),
            )
            .unwrap()
            .add_key_column(
                column_schema::Builder::new("key".to_string(), DatumKind::Varbinary)
                    .build()
                    .expect("Build column schema"),
            )
            .unwrap()
            .add_normal_column(
                column_schema::Builder::new("field".to_string(), DatumKind::Double)
                    .build()
                    .expect("Build column schema"),
            )
            .unwrap()
            .build()
            .expect("Build schema")
    }

    fn build_sort_expr(sort_col: &str, asc: bool) -> Expr {
        let col_expr = Expr::Column(Column::from(sort_col));
        Expr::Sort {
            expr: Box::new(col_expr),
            asc,
            nulls_first: false,
        }
    }

    fn build_primary_key_sort_exprs(schema: &Schema, asc: bool) -> Vec<Expr> {
        schema
            .key_columns()
            .iter()
            .map(|col| build_sort_expr(&col.name, asc))
            .collect()
    }

    fn check_optimization_works(
        schema: Schema,
        sort_exprs: Vec<Expr>,
        filter_expr: Option<Expr>,
        asc: bool,
    ) {
        let builder = LogicalPlanNodeBuilder::new(TEST_TABLE_NAME.to_string(), schema);

        let plan = {
            let mut builder = builder.clone().table_scan();
            if let Some(filter) = &filter_expr {
                builder = builder.filter(filter.clone());
            }
            builder
                .projection(vec![])
                .sort(sort_exprs.clone())
                .limit(0, Some(10))
                .take_plan()
        };

        let rule = OrderByPrimaryKeyRule;
        let optimized_plan = rule
            .do_optimize(&plan)
            .expect("Optimize plan")
            .expect("Succeed to optimize plan");
        let expected_plan = {
            let mut builder = builder.table_scan().table_scan_in_primary_key_order(asc);
            if let Some(filter) = filter_expr {
                builder = builder.filter(filter);
            }
            builder
                .projection(vec![])
                .limit(0, Some(10))
                .sort(sort_exprs)
                .limit(0, Some(10))
                .take_plan()
        };

        crate::logical_optimizer::tests::assert_logical_plan_eq(
            &optimized_plan,
            expected_plan.as_ref(),
        );
    }

    #[test]
    fn test_optimize_applied_with_no_filter() {
        let schema = build_optimized_schema();
        let sort_in_asc_order = true;
        let sort_exprs = build_primary_key_sort_exprs(&schema, sort_in_asc_order);
        check_optimization_works(schema, sort_exprs, None, sort_in_asc_order);
    }

    #[test]
    fn test_optimize_applied_with_prefix_sort_exprs() {
        let schema = build_optimized_schema();
        let sort_in_asc_order = true;
        let sort_exprs = build_primary_key_sort_exprs(&schema, sort_in_asc_order);
        let prefix_sort_exprs = sort_exprs[..1].to_vec();
        check_optimization_works(schema, prefix_sort_exprs, None, sort_in_asc_order);
    }

    #[test]
    fn test_optimize_applied_with_filter() {
        let schema = build_optimized_schema();
        let filter_expr = Expr::Literal(ScalarValue::Int8(None));
        let sort_in_asc_order = false;
        let sort_exprs = build_primary_key_sort_exprs(&schema, sort_in_asc_order);

        check_optimization_works(schema, sort_exprs, Some(filter_expr), sort_in_asc_order);
    }

    #[test]
    fn test_optimize_fail_with_wrong_schema() {
        let plan = {
            let schema = build_no_optimized_schema();
            let sort_exprs = build_primary_key_sort_exprs(&schema, true);
            let builder = LogicalPlanNodeBuilder::new(TEST_TABLE_NAME.to_string(), schema);
            builder
                .table_scan()
                .projection(vec![])
                .sort(sort_exprs)
                .limit(0, Some(10))
                .take_plan()
        };

        let rule = OrderByPrimaryKeyRule;
        let optimized_plan = rule.do_optimize(&plan).expect("Optimize plan");
        assert!(optimized_plan.is_none());
    }

    #[test]
    fn test_optimize_with_wrong_plan() {
        let plan = {
            let schema = build_optimized_schema();
            let builder = LogicalPlanNodeBuilder::new(TEST_TABLE_NAME.to_string(), schema);
            builder
                .table_scan()
                .projection(vec![])
                .limit(0, Some(10))
                .take_plan()
        };

        let rule = OrderByPrimaryKeyRule;
        let optimized_plan = rule.do_optimize(&plan).expect("Optimize plan");
        assert!(optimized_plan.is_none());
    }
}
