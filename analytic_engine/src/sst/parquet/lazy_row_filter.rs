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

use std::{collections::BTreeSet, sync::Arc};

use arrow::{
    array::BooleanArray, datatypes::Schema, error::Result as ArrowResult, record_batch::RecordBatch,
};
use datafusion::{
    common::{
        cast::as_boolean_array,
        tree_node::{TreeNode, TreeNodeVisitor, VisitRecursion},
        ToDFSchema,
    },
    logical_expr::Operator,
    physical_expr::{create_physical_expr, execution_props::ExecutionProps},
    physical_plan::PhysicalExpr,
    prelude::Expr,
};
use parquet::arrow::{
    arrow_reader::{ArrowPredicate, RowFilter},
    ProjectionMask,
};
use parquet_ext::ParquetMetaData;
use snafu::ResultExt;

use crate::sst::{
    metrics::MaybeTableLevelMetrics,
    reader::{error::Result, ArrowError, DataFusionError},
};

// Inspired by datafusion:
// https://github.com/apache/arrow-datafusion/blob/9f25634414a2650ee8dd2b263f0e29900b13a370/datafusion/core/src/datasource/physical_plan/parquet/row_filter.rs

/// Row filter(for late materialization) builder
// TODO: DataFusion already change predicates to `PhyscialExpr`, we should keep
// up with upstream.
// https://github.com/apache/arrow-datafusion/issues/4695
pub(crate) struct LazyRowFilterBuilder {
    arrow_predicates: Vec<DatafusionArrowPredicate>,
}

impl LazyRowFilterBuilder {
    /// Try to new lazy row filter builder, if not any filter candidate is
    /// selected at all, it may return `None`.
    pub fn try_new(
        exprs: &[Expr],
        schema: &Schema,
        meta_data: &ParquetMetaData,
        maybe_table_level_metrics: &Arc<MaybeTableLevelMetrics>,
    ) -> Result<Option<Self>> {
        let filter_candidates = FilterCandidatesSelector::new(exprs, schema, meta_data).select()?;
        if filter_candidates.is_empty() {
            return Ok(None);
        }

        let sorted_candidates = FilterCandidatesSorter(filter_candidates).sort();

        let arrow_predicates = sorted_candidates
            .into_iter()
            .map(|candidate| {
                DatafusionArrowPredicate::try_new(
                    candidate,
                    schema,
                    meta_data,
                    maybe_table_level_metrics.clone(),
                )
            })
            .collect::<Result<Vec<_>>>()?;

        Ok(Some(Self { arrow_predicates }))
    }

    pub fn build(&self) -> RowFilter {
        let row_filter = RowFilter::new(
            self.arrow_predicates
                .iter()
                .cloned()
                .map(|predicate| Box::new(predicate) as _)
                .collect(),
        );

        row_filter
    }
}

/// The selected filter with contexts used in later
// TODO: make a builder trait for it if necessary in later.
pub(crate) struct FilterCandidate {
    /// Logical expr
    expr: Expr,

    /// Column idxs in parquet file
    /// (we just push filter with a single column now)
    projection: BTreeSet<usize>,

    /// Encoded size of column in parquet file
    required_bytes: usize,
}

impl FilterCandidate {
    fn try_new(
        expr: &Expr,
        schema: &Schema,
        meta_data: &ParquetMetaData,
    ) -> Result<Option<FilterCandidate>> {
        let stats = FilterStatsBuilder::new(expr, schema).build()?;
        if stats.selected() {
            let required_column_indices = stats.required_column_indices;
            let required_bytes = size_of_columns(&required_column_indices, meta_data);

            Ok(Some(FilterCandidate {
                expr: expr.clone(),
                projection: required_column_indices,
                required_bytes,
            }))
        } else {
            Ok(None)
        }
    }
}

/// Selector for the candidate filter
/// Some([FilterCandidate]) will be returned if filter is selected and `None`
/// when unselected.
///
/// For simplicity and consideration of parquet's related
/// implementation (pushed filter must be highly selective, otherwise it will
/// even add up the cost of decoding...), the selected filter should satisfy:
///
///   - Has just one column(and not nested).
///   - Highly selective(now just `=`, `in`).
///
/// Detailed selection logic can be seen in [FilterStats].
struct FilterCandidatesSelector<'a> {
    exprs: &'a [Expr],
    schema: &'a Schema,
    meta_data: &'a ParquetMetaData,
}

impl<'a> FilterCandidatesSelector<'a> {
    fn new(exprs: &'a [Expr], schema: &'a Schema, meta_data: &'a ParquetMetaData) -> Self {
        Self {
            exprs,
            schema,
            meta_data,
        }
    }

    fn select(self) -> Result<Vec<FilterCandidate>> {
        self.exprs
            .iter()
            .filter_map(|expr| {
                FilterCandidate::try_new(expr, self.schema, self.meta_data).transpose()
            })
            .collect()
    }
}

/// Sorter for filter candidates
// TODO: make it a trait if extensible required in later.
struct FilterCandidatesSorter(Vec<FilterCandidate>);

impl FilterCandidatesSorter {
    fn sort(mut self) -> Vec<FilterCandidate> {
        self.0
            .sort_unstable_by_key(|candidate| candidate.required_bytes);
        self.0
    }
}

/// Calculate the total compressed size of all `Column's required for
/// predicate `Expr`. This should represent the total amount of file IO
/// required to evaluate the predicate.
fn size_of_columns(columns: &BTreeSet<usize>, metadata: &ParquetMetaData) -> usize {
    let mut total_size = 0;
    let row_groups = metadata.row_groups();
    for idx in columns {
        for rg in row_groups.iter() {
            total_size += rg.column(*idx).compressed_size() as usize;
        }
    }

    total_size
}

/// Filter stats used for selecting filter candidate
struct FilterStats {
    required_column_indices: BTreeSet<usize>,
    non_primitive_columns: bool,
    highly_selective: bool,
}

impl FilterStats {
    fn new() -> Self {
        Self {
            required_column_indices: BTreeSet::new(),
            non_primitive_columns: true,
            highly_selective: false,
        }
    }

    fn selected(&self) -> bool {
        self.required_column_indices.len() == 1
            && self.non_primitive_columns
            && self.highly_selective
    }
}

struct FilterStatsBuilder<'a> {
    expr: &'a Expr,
    schema: &'a Schema,
    stats: FilterStats,
}

impl<'a> FilterStatsBuilder<'a> {
    fn new(expr: &'a Expr, schema: &'a Schema) -> Self {
        Self {
            expr,
            schema,
            stats: FilterStats::new(),
        }
    }

    fn build(mut self) -> Result<FilterStats> {
        self.expr.visit(&mut self).context(DataFusionError)?;

        self.stats.highly_selective = match self.expr {
            Expr::BinaryExpr(expr) => matches!(&expr.op, Operator::Eq),
            Expr::InList(_) => true,
            _ => false,
        };

        Ok(self.stats)
    }
}

impl<'a> TreeNodeVisitor for FilterStatsBuilder<'a> {
    type N = Expr;

    fn pre_visit(&mut self, expr: &Expr) -> datafusion::error::Result<VisitRecursion> {
        if let Expr::Column(column) = expr {
            let idx = self.schema.index_of(&column.name)?;
            self.stats.required_column_indices.insert(idx);

            if self.schema.field(idx).data_type().is_nested() {
                self.stats.non_primitive_columns = false;
            }
        }

        Ok(VisitRecursion::Continue)
    }
}

/// A predicate which can be passed to `ParquetRecordBatchStream` to perform
/// row-level filtering during parquet decoding.
#[derive(Debug, Clone)]
struct DatafusionArrowPredicate {
    physical_expr: Arc<dyn PhysicalExpr>,
    projection_mask: ProjectionMask,
    maybe_table_level_metrics: Arc<MaybeTableLevelMetrics>,
}

impl DatafusionArrowPredicate {
    pub fn try_new(
        candidate: FilterCandidate,
        schema: &Schema,
        metadata: &ParquetMetaData,
        maybe_table_level_metrics: Arc<MaybeTableLevelMetrics>,
    ) -> Result<Self> {
        let projection = candidate.projection.iter().cloned().collect::<Vec<_>>();
        let schema = schema.project(&projection).context(ArrowError)?;
        let df_schema = schema.clone().to_dfschema().context(DataFusionError)?;

        let physical_expr = create_physical_expr(
            &candidate.expr,
            &df_schema,
            &schema,
            &ExecutionProps::default(),
        )
        .context(DataFusionError)?;

        Ok(Self {
            physical_expr,
            projection_mask: ProjectionMask::roots(
                metadata.file_metadata().schema_descr(),
                projection,
            ),
            maybe_table_level_metrics,
        })
    }
}

impl ArrowPredicate for DatafusionArrowPredicate {
    fn projection(&self) -> &ProjectionMask {
        &self.projection_mask
    }

    fn evaluate(&mut self, batch: RecordBatch) -> ArrowResult<BooleanArray> {
        match self
            .physical_expr
            .evaluate(&batch)
            .map(|v| v.into_array(batch.num_rows()))
        {
            Ok(array) => {
                let bool_arr = as_boolean_array(&array)?.clone();
                self.maybe_table_level_metrics
                    .record_batch_before_filter_counter
                    .inc_by(bool_arr.len() as u64);
                self.maybe_table_level_metrics
                    .record_batch_after_filter_counter
                    .inc_by(bool_arr.true_count() as u64);
                Ok(bool_arr)
            }
            Err(e) => Err(arrow::error::ArrowError::ComputeError(format!(
                "Error evaluating filter predicate: {:?}",
                e
            ))),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::BTreeSet, sync::Arc};

    use arrow::datatypes::{DataType, Field, Fields, Schema};
    use datafusion::{
        prelude::{col, lit, Expr},
        scalar::ScalarValue,
    };

    use crate::sst::parquet::lazy_row_filter::{
        FilterCandidate, FilterCandidatesSorter, FilterStatsBuilder,
    };

    struct TestUtil {
        schema: Schema,
        filters: Vec<Expr>,
    }

    impl TestUtil {
        fn new() -> Self {
            Self {
                schema: Self::test_schema(),
                filters: Self::test_filters(),
            }
        }

        fn test_schema() -> Schema {
            Schema::new(vec![
                Field::new("a", DataType::Int32, true),
                Field::new("b", DataType::Utf8, true),
                Field::new(
                    "c",
                    DataType::List(Arc::new(Field::new("field", DataType::Int32, true))),
                    true,
                ),
            ])
        }

        fn test_filters() -> Vec<Expr> {
            // Single column + eq
            let good_1 = col("a").eq(lit(42));
            // Single column + in
            let good_2 = col("b").in_list(vec![lit("a"), lit("b"), lit("c")], false);

            // multiple columns
            let bad_1 = (col("a").eq(lit(42))).or(col("b").eq(lit("d")));
            // Single column + compound data type
            let bad_2 = col("c").eq(lit(ScalarValue::Struct(
                Some(vec![
                    ScalarValue::Int32(Some(1)),
                    ScalarValue::Int32(Some(2)),
                ]),
                Fields::from(vec![
                    Field::new("ca", DataType::Int32, true),
                    Field::new("cb", DataType::Int32, true),
                ]),
            )));
            // Single column but not so selective
            let bad_3 = col("a").lt(lit(42));

            vec![good_1, good_2, bad_1, bad_2, bad_3]
        }
    }

    fn check_filter(expr: &Expr, schema: &Schema, expected: bool) {
        let stats = FilterStatsBuilder::new(expr, schema).build().unwrap();
        assert_eq!(stats.selected(), expected, "filter:{}", expr);
    }

    #[test]
    fn test_select_filter_candidates() {
        let test_util = TestUtil::new();
        let expecteds = [true, true, false, false, false];
        let cases_and_expecteds = test_util
            .filters
            .clone()
            .into_iter()
            .zip(expecteds)
            .collect::<Vec<_>>();

        // Check them.
        for (case, expected) in cases_and_expecteds {
            check_filter(&case, &test_util.schema, expected)
        }
    }

    #[test]
    fn test_sort_filter_candidates() {
        let test_util = TestUtil::new();
        let candidates = vec![
            FilterCandidate {
                expr: test_util.filters[0].clone(),
                projection: BTreeSet::from([0]),
                required_bytes: 3,
            },
            FilterCandidate {
                expr: test_util.filters[1].clone(),
                projection: BTreeSet::from([1]),
                required_bytes: 1,
            },
        ];

        let sorted_candidates = FilterCandidatesSorter(candidates).sort();
        let sorted_exprs = sorted_candidates
            .iter()
            .map(|candidate| candidate.expr.clone())
            .collect::<Vec<_>>();
        let expected = vec![test_util.filters[1].clone(), test_util.filters[0].clone()];
        assert_eq!(sorted_exprs, expected);
    }
}
