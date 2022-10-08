// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

//! Aggregate functions.

use std::{fmt, ops::Deref};

use arrow::array::ArrayRef as DfArrayRef;
use common_util::define_result;
use datafusion::{
    error::{DataFusionError, Result as DfResult},
    physical_plan::Accumulator as DfAccumulator,
    scalar::ScalarValue as DfScalarValue,
};
use datafusion_expr::AggregateState as DfAggregateState;
use snafu::Snafu;

use crate::functions::{ScalarValue, ScalarValueRef};

#[derive(Debug, Snafu)]
#[snafu(visibility(pub(crate)))]
pub enum Error {
    #[snafu(display("Failed to get state, err:{}", source))]
    GetState {
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display("Failed to merge state, err:{}", source))]
    MergeState {
        source: Box<dyn std::error::Error + Send + Sync>,
    },
}

define_result!(Error);

pub struct State(Vec<DfScalarValue>);

impl State {
    fn into_df_aggregate_states(self) -> Vec<DfAggregateState> {
        self.0.into_iter().map(DfAggregateState::Scalar).collect()
    }
}

impl From<ScalarValue> for State {
    fn from(value: ScalarValue) -> Self {
        Self(vec![value.into_df_scalar_value()])
    }
}

pub struct Input<'a>(&'a [DfScalarValue]);

impl<'a> Input<'a> {
    pub fn iter(&self) -> impl Iterator<Item = ScalarValueRef> {
        self.0.iter().map(ScalarValueRef::from)
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub fn value(&self, index: usize) -> ScalarValueRef {
        ScalarValueRef::from(&self.0[index])
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
    fn state(&self) -> DfResult<Vec<DfAggregateState>> {
        let state = self.accumulator.state().map_err(|e| {
            DataFusionError::Execution(format!("Accumulator failed to get state, err:{}", e))
        })?;
        Ok(state.into_df_aggregate_states())
    }

    fn update_batch(&mut self, values: &[DfArrayRef]) -> DfResult<()> {
        if values.is_empty() {
            return Ok(());
        };
        (0..values[0].len()).try_for_each(|index| {
            let v = values
                .iter()
                .map(|array| DfScalarValue::try_from_array(array, index))
                .collect::<DfResult<Vec<DfScalarValue>>>()?;
            let input = Input(&v);

            self.accumulator.update(input).map_err(|e| {
                DataFusionError::Execution(format!("Accumulator failed to update, err:{}", e))
            })
        })
    }

    fn merge_batch(&mut self, states: &[DfArrayRef]) -> DfResult<()> {
        if states.is_empty() {
            return Ok(());
        };
        (0..states[0].len()).try_for_each(|index| {
            let v = states
                .iter()
                .map(|array| DfScalarValue::try_from_array(array, index))
                .collect::<DfResult<Vec<DfScalarValue>>>()?;
            let state_ref = StateRef(Input(&v));

            self.accumulator.merge(state_ref).map_err(|e| {
                DataFusionError::Execution(format!("Accumulator failed to merge, err:{}", e))
            })
        })
    }

    fn evaluate(&self) -> DfResult<DfScalarValue> {
        let value = self.accumulator.evaluate().map_err(|e| {
            DataFusionError::Execution(format!("Accumulator failed to evaluate, err:{}", e))
        })?;

        Ok(value.into_df_scalar_value())
    }
}
