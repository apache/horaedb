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

//! thetasketch_distinct() udaf.

use std::fmt;

use arrow::datatypes::DataType;
use common_types::datum::DatumKind;
use generic_error::BoxError;
use hyperloglog::HyperLogLog;
use macros::define_result;
use snafu::{ensure, OptionExt, ResultExt, Snafu};

use crate::{
    aggregate::{self, Accumulator, GetState, Input, MergeState, State, StateRef},
    functions::{AggregateFunction, ScalarValue, TypeSignature},
    registry::{self, FunctionRegistry},
    udaf::AggregateUdf,
};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Invalid argument number."))]
    InvalidArgNum,

    #[snafu(display("Invalid state len."))]
    InvalidStateLen,

    #[snafu(display("Invalid state, state is not string."))]
    StateNotString,

    #[snafu(display("Failed to decode base64 of hll, err:{}.", source))]
    DecodeBase64 { source: base64::DecodeError },

    #[snafu(display("Invalid state, failed to decode hll, err:{}.", source))]
    DecodeHll { source: bincode::Error },

    #[snafu(display("Invalid state, failed to encode hll, err:{}.", source))]
    EncodeHll { source: bincode::Error },
}

define_result!(Error);

const HLL_ERROR_RATE: f64 = 0.01;
// Hll seed:
const HLL_KEY: u128 = 0;

pub fn register_to_registry(registry: &mut dyn FunctionRegistry) -> registry::Result<()> {
    registry.register_udaf(new_udaf())
}

fn new_udaf() -> AggregateUdf {
    let aggregate_function = new_function();

    AggregateUdf::create("thetasketch_distinct", aggregate_function)
}

pub(crate) fn new_function() -> AggregateFunction {
    // Always use the same hasher with same keys.
    let hll = HyperLogLog::new_deterministic(HLL_ERROR_RATE, HLL_KEY);

    let accumulator_fn = move |_: &DataType| {
        let distinct = HllDistinct {
            hll: HyperLogLog::new_from_template(&hll),
        };

        Ok(distinct)
    };

    let type_signature = make_type_signature();
    let state_type = make_state_type();

    AggregateFunction::make_by_fn(
        type_signature,
        DatumKind::UInt64,
        state_type,
        accumulator_fn,
    )
}

fn make_type_signature() -> TypeSignature {
    TypeSignature::Uniform(
        1,
        vec![
            DatumKind::Timestamp,
            DatumKind::Double,
            DatumKind::Varbinary,
            DatumKind::String,
            DatumKind::UInt64,
        ],
    )
}

fn make_state_type() -> Vec<DatumKind> {
    vec![DatumKind::String]
}

/// Distinct counter based on HyperLogLog.
///
/// The HyperLogLogs must be initialized with same hash seeds (new from same
/// template).
struct HllDistinct {
    hll: HyperLogLog,
}

// binary datatype to scalarvalue.
// TODO: maybe we can remove base64 encoding?
impl HllDistinct {
    fn merge_impl(&mut self, states: StateRef) -> Result<()> {
        // The states are serialize from hll.
        ensure!(states.num_columns() == 1, InvalidStateLen);
        let merged_col = states.column(0).unwrap();

        let num_rows = merged_col.num_rows();
        for row_idx in 0..num_rows {
            let datum = merged_col.datum_view(row_idx);
            // Try to deserialize the hll.
            let hll_string = datum.into_str().context(StateNotString)?;
            let hll_bytes = base64::decode(hll_string).context(DecodeBase64)?;
            // Try to deserialize the hll.
            let hll = bincode::deserialize(&hll_bytes).context(DecodeHll)?;

            // Merge the hll, note that the two hlls must created or serialized from the
            // same template hll.
            self.hll.merge(&hll);
        }

        Ok(())
    }
}

impl fmt::Debug for HllDistinct {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("HllDistinct")
            .field("len", &self.hll.len())
            .finish()
    }
}

impl Accumulator for HllDistinct {
    // TODO: maybe we can remove base64 encoding?
    fn state(&self) -> aggregate::Result<State> {
        // Serialize `self.hll` to bytes.
        let buf = bincode::serialize(&self.hll).box_err().context(GetState)?;
        // HACK: DataFusion does not support creating a scalar from binary, so we need
        // to use base64 to convert a binary into string.
        let hll_string = base64::encode(buf);

        Ok(State::from(ScalarValue::from(hll_string)))
    }

    fn update(&mut self, input: Input) -> aggregate::Result<()> {
        if input.is_empty() {
            return Ok(());
        }

        // Has found it not empty, so we can unwrap here.
        let first_col = input.column(0).unwrap();
        let num_rows = first_col.num_rows();
        if num_rows == 0 {
            return Ok(());
        }

        // Loop over the datums in the column blocks, insert them into hll.
        let num_cols = input.num_columns();
        for col_idx in 0..num_cols {
            let col = input.column(col_idx).unwrap();
            for row_idx in 0..num_rows {
                let datum = col.datum_view(row_idx);
                // Insert datum into hll.
                self.hll.insert(&datum);
            }
        }

        Ok(())
    }

    fn merge(&mut self, states: StateRef) -> aggregate::Result<()> {
        self.merge_impl(states).box_err().context(MergeState)
    }

    fn evaluate(&self) -> aggregate::Result<ScalarValue> {
        let count = self.hll.len() as u64;

        Ok(ScalarValue::from(count))
    }
}
