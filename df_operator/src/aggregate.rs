// Copyright 2022-2023 CeresDB Project Authors. Licensed under Apache-2.0.

//! Aggregate functions.

use std::{fmt, ops::Deref};

use arrow::array::ArrayRef as DfArrayRef;
use common_types::{column::ColumnBlock, datum::DatumView};
use datafusion::{
    error::{DataFusionError, Result as DfResult},
    physical_plan::Accumulator as DfAccumulator,
    scalar::ScalarValue as DfScalarValue,
};
use generic_error::GenericError;
use macros::define_result;
use snafu::Snafu;

use crate::functions::ScalarValue;

#[derive(Debug, Snafu)]
#[snafu(visibility(pub(crate)))]
pub enum Error {
    #[snafu(display("Failed to get state, err:{}", source))]
    GetState { source: GenericError },

    #[snafu(display("Failed to merge state, err:{}", source))]
    MergeState { source: GenericError },
}

define_result!(Error);

pub struct State(Vec<DfScalarValue>);

impl State {
    /// Convert to a set of ScalarValues
    fn into_state(self) -> Vec<DfScalarValue> {
        self.0
    }
}

impl From<ScalarValue> for State {
    fn from(value: ScalarValue) -> Self {
        Self(vec![value.into_df_scalar_value()])
    }
}

pub struct Input<'a>(&'a [DatumView<'a>]);

impl<'a> Input<'a> {
    pub fn iter(&self) -> impl Iterator<Item = &DatumView<'a>> {
        self.0.iter()
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub fn value(&self, index: usize) -> &DatumView<'a> {
        self.0.get(index).unwrap()
    }
}

pub struct StateRef<'a>(Input<'a>);

impl<'a> Deref for StateRef<'a> {
    type Target = Input<'a>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

/// An accumulator represents a stateful object that lives throughout the
/// evaluation of multiple rows and generically accumulates values.
///
/// An accumulator knows how to:
/// * update its state from inputs via `update`
/// * convert its internal state to a vector of scalar values
/// * update its state from multiple accumulators' states via `merge`
/// * compute the final value from its internal state via `evaluate`
pub trait Accumulator: Send + Sync + fmt::Debug {
    /// Returns the state of the accumulator at the end of the accumulation.
    // in the case of an average on which we track `sum` and `n`, this function
    // should return a vector of two values, sum and n.
    fn state(&self) -> Result<State>;

    /// updates the accumulator's state from a vector of scalars.
    fn update(&mut self, values: Input) -> Result<()>;

    /// updates the accumulator's state from a vector of scalars.
    fn merge(&mut self, states: StateRef) -> Result<()>;

    /// returns its value based on its current state.
    fn evaluate(&self) -> Result<ScalarValue>;
}

#[derive(Debug)]
pub struct ToDfAccumulator<T> {
    accumulator: T,
}

impl<T> ToDfAccumulator<T> {
    pub fn new(accumulator: T) -> Self {
        Self { accumulator }
    }
}

impl<T: Accumulator> DfAccumulator for ToDfAccumulator<T> {
    fn state(&self) -> DfResult<Vec<DfScalarValue>> {
        let state = self.accumulator.state().map_err(|e| {
            DataFusionError::Execution(format!("Accumulator failed to get state, err:{e}"))
        })?;
        Ok(state.into_state())
    }

    fn update_batch(&mut self, values: &[DfArrayRef]) -> DfResult<()> {
        if values.is_empty() {
            return Ok(());
        };

        let column_blocks = values
            .iter()
            .map(|array| {
                ColumnBlock::try_cast_arrow_array_ref(array).map_err(|e| {
                    DataFusionError::Execution(format!(
                        "Accumulator failed to cast arrow array to column block, column, err:{e}"
                    ))
                })
            })
            .collect::<DfResult<Vec<_>>>()?;

        let mut row = Vec::with_capacity(column_blocks.len());
        let num_rows = column_blocks[0].num_rows();
        (0..num_rows).try_for_each(|index| {
            row.clear();

            for column_block in &column_blocks {
                let datum_view = column_block.datum_view(index);
                row.push(datum_view);
            }
            let input = Input(&row);

            self.accumulator.update(input).map_err(|e| {
                DataFusionError::Execution(format!("Accumulator failed to update, err:{e}"))
            })
        })
    }

    fn merge_batch(&mut self, states: &[DfArrayRef]) -> DfResult<()> {
        if states.is_empty() {
            return Ok(());
        };

        let column_blocks = states
            .iter()
            .map(|array| {
                ColumnBlock::try_cast_arrow_array_ref(array).map_err(|e| {
                    DataFusionError::Execution(format!(
                        "Accumulator failed to cast arrow array to column block, column, err:{e}"
                    ))
                })
            })
            .collect::<DfResult<Vec<_>>>()?;

        let mut row = Vec::with_capacity(column_blocks.len());
        let num_rows = column_blocks[0].num_rows();
        (0..num_rows).try_for_each(|index| {
            row.clear();

            for column_block in &column_blocks {
                let datum_view = column_block.datum_view(index);
                row.push(datum_view);
            }
            let state_ref = StateRef(Input(&row));

            self.accumulator.merge(state_ref).map_err(|e| {
                DataFusionError::Execution(format!("Accumulator failed to merge, err:{e}"))
            })
        })
    }

    fn evaluate(&self) -> DfResult<DfScalarValue> {
        let value = self.accumulator.evaluate().map_err(|e| {
            DataFusionError::Execution(format!("Accumulator failed to evaluate, err:{e}"))
        })?;

        Ok(value.into_df_scalar_value())
    }

    fn size(&self) -> usize {
        std::mem::size_of_val(self)
    }
}
