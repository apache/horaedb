// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

//! Interpreter for insert statement

use std::{collections::HashMap, ops::IndexMut, sync::Arc};

use arrow_deps::{
    arrow::{datatypes::Schema as ArrowSchema, record_batch::RecordBatch},
    datafusion::{
        common::DFSchema,
        error::DataFusionError,
        logical_expr::ColumnarValue as DfColumnarValue,
        optimizer::simplify_expressions::ConstEvaluator,
        physical_expr::{
            create_physical_expr, execution_props::ExecutionProps, expressions::TryCastExpr,
        },
    },
    datafusion_expr::{expr::Expr as DfLogicalExpr, expr_rewriter::ExprRewritable},
};
use async_trait::async_trait;
use common_types::{
    column::ColumnBlock, column_schema::ColumnId, datum::Datum, hash::hash64, row::RowGroup,
};
use common_util::codec::{compact::MemCompactEncoder, Encoder};
use snafu::{ResultExt, Snafu};
use sql::plan::InsertPlan;
use table_engine::table::WriteRequest;

use crate::{
    context::Context,
    interpreter::{Insert, Interpreter, InterpreterPtr, Output, Result as InterpreterResult},
};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("DataFusion Failed to generate expr, err:{}", source))]
    DataFusionExpr { source: DataFusionError },

    #[snafu(display("DataFusion Failed to get data type from PhysicalExpr, err:{}", source))]
    DataFusionDataType { source: DataFusionError },

    #[snafu(display("DataFusion Failed to evaluate the physical expr, err:{}", source))]
    DataFusionExecutor { source: DataFusionError },

    #[snafu(display("Failed to write table, err:{}", source))]
    WriteTable { source: table_engine::table::Error },

    #[snafu(display("Failed to encode tsid, err:{}", source))]
    EncodeTsid {
        source: common_util::codec::compact::Error,
    },

    #[snafu(display("Failed to convert arrow Array to ColumnBlock, err:{}", source))]
    ConvertColumnBlock { source: common_types::column::Error },
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
        fill_default_values(&mut rows, &default_value_map).context(Insert)?;

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

fn fill_default_values(
    rows: &mut RowGroup,
    default_value_map: &HashMap<usize, DfLogicalExpr>,
) -> Result<()> {
    let input_df_schema = DFSchema::empty();
    let input_arrow_schema = Arc::new(ArrowSchema::empty());
    let input_batch = RecordBatch::new_empty(input_arrow_schema.clone());
    for (column_idx, default_value_expr) in default_value_map.iter() {
        // Optimize logical expr
        let execution_props = ExecutionProps::default();
        let mut const_optimizer = ConstEvaluator::new(&execution_props);
        let evaluated_expr = default_value_expr
            .clone()
            .rewrite(&mut const_optimizer)
            .context(DataFusionExpr)?;

        // Create physical expr
        let physical_expr = create_physical_expr(
            &evaluated_expr,
            &input_df_schema,
            &input_arrow_schema,
            &ExecutionProps::default(),
        )
        .context(DataFusionExpr)?;

        let from_type = physical_expr
            .data_type(&input_arrow_schema)
            .context(DataFusionDataType)?;
        let to_type = rows.schema().column(*column_idx).data_type;

        let casted_physical_expr = if from_type != to_type.into() {
            Arc::new(TryCastExpr::new(physical_expr, to_type.into()))
        } else {
            physical_expr
        };

        let output = casted_physical_expr
            .evaluate(&input_batch)
            .context(DataFusionExecutor)?;

        fill_column_to_row_group(*column_idx, &output, rows)?;
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
            for row_idx in 0..rows.num_rows() {
                let datum_kind = rows.schema().column(column_idx).data_type;
                let column_block = ColumnBlock::try_from_arrow_array_ref(&datum_kind, array)
                    .context(ConvertColumnBlock)?;
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
