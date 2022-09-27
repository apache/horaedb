// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

//! thetasketch_distinct() udaf.

use std::fmt;

use arrow_deps::arrow::datatypes::DataType;
use common_types::datum::DatumKind;
use common_util::define_result;
use hyperloglog::HyperLogLog;
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

// TODO(yingwen): Avoid base64 encode/decode if datafusion supports converting
// binary datatype to scalarvalue.
impl HllDistinct {
    fn merge_impl(&mut self, states: StateRef) -> Result<()> {
        // The states are serialize from hll.
        ensure!(states.len() == 1, InvalidStateLen);
        let value_ref = states.value(0);
        let hll_string = value_ref.as_str().context(StateNotString)?;
        let hll_bytes = base64::decode(hll_string).context(DecodeBase64)?;
        // Try to deserialize the hll.
        let hll = bincode::deserialize(&hll_bytes).context(DecodeHll)?;

        // Merge the hll, note that the two hlls must created or serialized from the
        // same template hll.
        self.hll.merge(&hll);

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
    fn state(&self) -> aggregate::Result<State> {
        // Serialize `self.hll` to bytes.
        let buf = bincode::serialize(&self.hll)
            .map_err(|e| Box::new(e) as _)
            .context(GetState)?;
        // HACK: DataFusion does not support creating a scalar from binary, so we need
        // to use base64 to convert a binary into string.
        let hll_string = base64::encode(buf);

        Ok(State::from(ScalarValue::from(hll_string)))
    }

    fn update(&mut self, values: Input) -> aggregate::Result<()> {
        for value_ref in values.iter() {
            // Insert value into hll.
            self.hll.insert(&value_ref);
        }

        Ok(())
    }

    fn merge(&mut self, states: StateRef) -> aggregate::Result<()> {
        self.merge_impl(states)
            .map_err(|e| Box::new(e) as _)
            .context(MergeState)
    }

    fn evaluate(&self) -> aggregate::Result<ScalarValue> {
        let count = self.hll.len() as u64;

        Ok(ScalarValue::from(count))
    }
}
