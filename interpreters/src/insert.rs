// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

//! Interpreter for insert statement

use std::{
    collections::{BTreeMap, HashMap},
    ops::IndexMut,
    sync::Arc,
};

use arrow::{array::ArrayRef, error::ArrowError, record_batch::RecordBatch};
use async_trait::async_trait;
use common_types::{
    column::{ColumnBlock, ColumnBlockBuilder},
    column_schema::ColumnId,
    datum::Datum,
    hash::hash64,
    row::RowGroup,
};
use common_util::codec::{compact::MemCompactEncoder, Encoder};
use datafusion::{
    error::DataFusionError,
    logical_expr::ColumnarValue as DfColumnarValue,
    logical_plan::ToDFSchema,
    optimizer::simplify_expressions::ConstEvaluator,
    physical_expr::{
        create_physical_expr, execution_props::ExecutionProps, expressions::TryCastExpr,
    },
};
use datafusion_expr::{expr::Expr as DfLogicalExpr, expr_rewriter::ExprRewritable};
use df_operator::visitor::find_columns_by_expr;
use snafu::{OptionExt, ResultExt, Snafu};
use sql::plan::InsertPlan;
use table_engine::table::{TableRef, WriteRequest};

use crate::{
    context::Context,
    interpreter::{Insert, Interpreter, InterpreterPtr, Output, Result as InterpreterResult},
};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Failed to generate datafusion expr, err:{}", source))]
    DataFusionExpr { source: DataFusionError },

    #[snafu(display(
        "Failed to get data type from datafusion physical expr, err:{}",
        source
    ))]
    DataFusionDataType { source: DataFusionError },

    #[snafu(display("Failed to get arrow schema, err:{}", source))]
    ArrowSchema { source: ArrowError },

    #[snafu(display("Failed to get datafusion schema, err:{}", source))]
    DatafusionSchema { source: DataFusionError },

    #[snafu(display("Failed to evaluate datafusion physical expr, err:{}", source))]
    DataFusionExecutor { source: DataFusionError },

    #[snafu(display("Failed to build arrow record batch, err:{}", source))]
    BuildArrowRecordBatch { source: ArrowError },

    #[snafu(display("Failed to write table, err:{}", source))]
    WriteTable { source: table_engine::table::Error },

    #[snafu(display("Failed to encode tsid, err:{}", source))]
    EncodeTsid {
        source: common_util::codec::compact::Error,
    },

    #[snafu(display("Failed to convert arrow array to column block, err:{}", source))]
    ConvertColumnBlock { source: common_types::column::Error },

    #[snafu(display("Failed to find input columns of expr"))]
    FindExpressionInput,

    #[snafu(display("Failed to build column block, err:{}", source))]
    BuildColumnBlock { source: common_types::column::Error },
}

define_result!(Error);

pub struct InsertInterpreter {
    ctx: Context,
    plan: InsertPlan,
}

impl InsertInterpreter {
    pub fn create(ctx: Context, plan: InsertPlan) -> InterpreterPtr {
        Box::new(Self { ctx, plan })
    }
}

#[async_trait]
impl Interpreter for InsertInterpreter {
    async fn execute(mut self: Box<Self>) -> InterpreterResult<Output> {
        // Generate tsid if needed.
        self.maybe_generate_tsid().context(Insert)?;
        let InsertPlan {
            table,
            mut rows,
            default_value_map,
        } = self.plan;

        // Fill default values
        fill_default_values(table.clone(), &mut rows, &default_value_map).context(Insert)?;

        // Context is unused now
        let _ctx = self.ctx;

        let request = WriteRequest { row_group: rows };

        let num_rows = table
            .write(request)
            .await
            .context(WriteTable)
            .context(Insert)?;

        Ok(Output::AffectedRows(num_rows))
    }
}

impl InsertInterpreter {
    fn maybe_generate_tsid(&mut self) -> Result<()> {
        let schema = self.plan.rows.schema();
        let tsid_idx = schema.index_of_tsid();

        if let Some(idx) = tsid_idx {
            // Vec of (`index of tag`, `column id of tag`).
            let tag_idx_column_ids: Vec<_> = schema
                .columns()
                .iter()
                .enumerate()
                .filter_map(|(i, column)| {
                    if column.is_tag {
                        Some((i, column.id))
                    } else {
                        None
                    }
                })
                .collect();

            let mut hash_bytes = Vec::new();
            for i in 0..self.plan.rows.num_rows() {
                let row = self.plan.rows.get_row_mut(i).unwrap();

                let mut tsid_builder = TsidBuilder::new(&mut hash_bytes);

                for (idx, column_id) in &tag_idx_column_ids {
                    tsid_builder.maybe_write_datum(*column_id, &row[*idx])?;
                }

                let tsid = tsid_builder.finish();
                row[idx] = Datum::UInt64(tsid);
            }
        }
        Ok(())
    }
}

struct TsidBuilder<'a> {
    encoder: MemCompactEncoder,
    hash_bytes: &'a mut Vec<u8>,
}

impl<'a> TsidBuilder<'a> {
    fn new(hash_bytes: &'a mut Vec<u8>) -> Self {
        // Clear the bytes buffer.
        hash_bytes.clear();

        Self {
            encoder: MemCompactEncoder,
            hash_bytes,
        }
    }

    fn maybe_write_datum(&mut self, column_id: ColumnId, datum: &Datum) -> Result<()> {
        // Null datum will be ignored, so tsid remains unchanged after adding a null
        // column.
        if datum.is_null() {
            return Ok(());
        }

        // Write column id first.
        self.encoder
            .encode(self.hash_bytes, &Datum::UInt64(u64::from(column_id)))
            .context(EncodeTsid)?;
        // Write datum.
        self.encoder
            .encode(self.hash_bytes, datum)
            .context(EncodeTsid)?;
        Ok(())
    }

    fn finish(self) -> u64 {
        hash64(self.hash_bytes)
    }
}

/// Fill missing columns which can be calculated via default value expr.
fn fill_default_values(
    table: TableRef,
    row_groups: &mut RowGroup,
    default_value_map: &BTreeMap<usize, DfLogicalExpr>,
) -> Result<()> {
    let mut cached_columns_map: HashMap<usize, DfColumnarValue> = HashMap::new();
    let table_arrow_schema = table.schema().to_arrow_schema_ref();

    for (column_idx, default_value_expr) in default_value_map.iter() {
        // Optimize logical expr
        let execution_props = ExecutionProps::default();
        let mut const_optimizer =
            ConstEvaluator::try_new(&execution_props).context(DataFusionExpr)?;
        let evaluated_expr = default_value_expr
            .clone()
            .rewrite(&mut const_optimizer)
            .context(DataFusionExpr)?;

        // Find input columns
        let required_column_idxes = find_columns_by_expr(&evaluated_expr)
            .iter()
            .map(|column_name| table.schema().index_of(column_name))
            .collect::<Option<Vec<usize>>>()
            .context(FindExpressionInput)?;
        let input_arrow_schema = table_arrow_schema
            .project(&required_column_idxes)
            .context(ArrowSchema)?;
        let input_df_schema = input_arrow_schema
            .clone()
            .to_dfschema()
            .context(DatafusionSchema)?;

        // Create physical expr
        let execution_props = ExecutionProps::default();
        let physical_expr = create_physical_expr(
            &evaluated_expr,
            &input_df_schema,
            &input_arrow_schema,
            &execution_props,
        )
        .context(DataFusionExpr)?;

        let from_type = physical_expr
            .data_type(&input_arrow_schema)
            .context(DataFusionDataType)?;
        let to_type = row_groups.schema().column(*column_idx).data_type;

        let casted_physical_expr = if from_type != to_type.into() {
            Arc::new(TryCastExpr::new(physical_expr, to_type.into()))
        } else {
            physical_expr
        };

        // Build input record batch
        let input_arrays = required_column_idxes
            .into_iter()
            .map(|col_idx| {
                get_or_extract_column_from_row_groups(col_idx, row_groups, &mut cached_columns_map)
            })
            .collect::<Result<Vec<_>>>()?;
        let input = if input_arrays.is_empty() {
            RecordBatch::new_empty(Arc::new(input_arrow_schema))
        } else {
            RecordBatch::try_new(Arc::new(input_arrow_schema), input_arrays)
                .context(BuildArrowRecordBatch)?
        };

        let output = casted_physical_expr
            .evaluate(&input)
            .context(DataFusionExecutor)?;

        fill_column_to_row_group(*column_idx, &output, row_groups)?;

        // Write output to cache.
        cached_columns_map.insert(*column_idx, output);
    }

    Ok(())
}

fn fill_column_to_row_group(
    column_idx: usize,
    column: &DfColumnarValue,
    rows: &mut RowGroup,
) -> Result<()> {
    match column {
        DfColumnarValue::Array(array) => {
            let datum_kind = rows.schema().column(column_idx).data_type;
            let column_block = ColumnBlock::try_from_arrow_array_ref(&datum_kind, array)
                .context(ConvertColumnBlock)?;
            for row_idx in 0..rows.num_rows() {
                let datum = column_block.datum(row_idx);
                rows.get_row_mut(row_idx)
                    .map(|row| std::mem::replace(row.index_mut(column_idx), datum.clone()));
            }
        }
        DfColumnarValue::Scalar(scalar) => {
            if let Some(datum) = Datum::from_scalar_value(scalar) {
                for row_idx in 0..rows.num_rows() {
                    rows.get_row_mut(row_idx)
                        .map(|row| std::mem::replace(row.index_mut(column_idx), datum.clone()));
                }
            }
        }
    };

    Ok(())
}

/// This method is used to get specific column data. 
/// There are two path:
///  1. get from cached_columns_map
///  2. extract from row_groups
/// 
/// For performance reasons, we cached the columns which extract from row_groups before, 
/// and we will also cache the output of the exprs.
fn get_or_extract_column_from_row_groups(
    column_idx: usize,
    row_groups: &RowGroup,
    cached_columns_map: &mut HashMap<usize, DfColumnarValue>,
) -> Result<ArrayRef> {
    let num_rows = row_groups.num_rows();
    let column = cached_columns_map
        .get(&column_idx)
        .map(|c| Ok(c.clone()))
        .unwrap_or_else(|| {
            let data_type = row_groups.schema().column(column_idx).data_type;
            let iter = row_groups.iter_column(column_idx);
            let mut builder = ColumnBlockBuilder::with_capacity(&data_type, iter.size_hint().0);

            for datum in iter {
                builder.append(datum.clone()).context(BuildColumnBlock)?;
            }

            let columnar_value = DfColumnarValue::Array(builder.build().to_arrow_array_ref());
            cached_columns_map.insert(column_idx, columnar_value.clone());
            Ok(columnar_value)
        })?;

    Ok(column.into_array(num_rows))
}
