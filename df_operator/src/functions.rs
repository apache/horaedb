// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

//! Functions.

use std::{
    hash::{Hash, Hasher},
    sync::Arc,
};

use arrow::{
    array::{TimestampMillisecondArray, TimestampNanosecondArray},
    datatypes::DataType,
};
use common_types::{column::ColumnBlock, datum::DatumKind};
use common_util::{define_result, error::GenericError};
use datafusion::{
    error::DataFusionError, physical_plan::ColumnarValue as DfColumnarValue,
    scalar::ScalarValue as DfScalarValue,
};
use datafusion_expr::{
    AccumulatorFunctionImplementation, ReturnTypeFunction, ScalarFunctionImplementation,
    Signature as DfSignature, StateTypeFunction, TypeSignature as DfTypeSignature, Volatility,
};
use smallvec::SmallVec;
use snafu::{ResultExt, Snafu};

use crate::aggregate::{Accumulator, ToDfAccumulator};

// Most functions have no more than 5 args.
const FUNC_ARG_NUM: usize = 5;

#[derive(Debug, Snafu)]
#[snafu(visibility(pub(crate)))]
pub enum Error {
    #[snafu(display("Failed to convert array to ColumnarValue, err:{}", source))]
    InvalidArray { source: common_types::column::Error },

    #[snafu(display("Invalid function arguments, err:{}", source))]
    InvalidArguments { source: GenericError },

    #[snafu(display("Failed to execute function, err:{}", source))]
    CallFunction { source: GenericError },
}

define_result!(Error);

/// A dynamically typed, nullable single value.
// TODO(yingwen): Can we use Datum?
#[derive(Debug)]
pub struct ScalarValue(DfScalarValue);

impl ScalarValue {
    pub(crate) fn into_df_scalar_value(self) -> DfScalarValue {
        self.0
    }

    fn from_df_scalar_value(df_scalar: &DfScalarValue) -> Self {
        Self(df_scalar.clone())
    }

    pub fn as_str(&self) -> Option<&str> {
        match &self.0 {
            DfScalarValue::Utf8(value_opt) => value_opt.as_ref().map(|v| v.as_str()),
            _ => None,
        }
    }
}

impl From<String> for ScalarValue {
    fn from(value: String) -> Self {
        Self(DfScalarValue::Utf8(Some(value)))
    }
}

impl From<u64> for ScalarValue {
    fn from(value: u64) -> Self {
        Self(value.into())
    }
}

pub struct ScalarValueRef<'a>(&'a DfScalarValue);

impl<'a> ScalarValueRef<'a> {
    pub fn as_str(&self) -> Option<&str> {
        match self.0 {
            DfScalarValue::Utf8(value_opt) | DfScalarValue::LargeUtf8(value_opt) => {
                value_opt.as_ref().map(|v| v.as_str())
            }
            _ => None,
        }
    }
}

impl<'a> From<&'a DfScalarValue> for ScalarValueRef<'a> {
    fn from(value: &DfScalarValue) -> ScalarValueRef {
        ScalarValueRef(value)
    }
}

impl<'a> Hash for ScalarValueRef<'a> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.0.hash(state)
    }
}

/// Represent a value of function result.
#[derive(Debug)]
pub enum ColumnarValue {
    /// Array of values.
    Array(ColumnBlock),
    /// A single value.
    Scalar(ScalarValue),
}

impl ColumnarValue {
    fn into_df_columnar_value(self) -> DfColumnarValue {
        match self {
            ColumnarValue::Array(v) => DfColumnarValue::Array(v.to_arrow_array_ref()),
            ColumnarValue::Scalar(v) => DfColumnarValue::Scalar(v.into_df_scalar_value()),
        }
    }

    fn try_from_df_columnar_value(df_value: &DfColumnarValue) -> Result<Self> {
        let columnar_value = match df_value {
            DfColumnarValue::Array(array) => {
                let column_block =
                    ColumnBlock::try_cast_arrow_array_ref(array).context(InvalidArray)?;
                ColumnarValue::Array(column_block)
            }
            DfColumnarValue::Scalar(v) => {
                ColumnarValue::Scalar(ScalarValue::from_df_scalar_value(v))
            }
        };

        Ok(columnar_value)
    }
}

/// A function's TypeSignature.
#[derive(Debug)]
pub enum TypeSignature {
    /// exact number of arguments of an exact type
    Exact(Vec<DatumKind>),
    /// fixed number of arguments of an arbitrary but equal type out of a list
    /// of valid types
    // A function of one argument of double is `Uniform(1, vec![DatumKind::Double])`
    // A function of one argument of double or uint64 is `Uniform(1, vec![DatumKind::Double,
    // DatumKind::UInt64])`
    Uniform(usize, Vec<DatumKind>),
    /// One of a list of signatures
    OneOf(Vec<TypeSignature>),
}

impl TypeSignature {
    pub(crate) fn to_datafusion_signature(&self) -> DfSignature {
        DfSignature::new(self.to_datafusion_type_signature(), Volatility::Immutable)
    }

    fn to_datafusion_type_signature(&self) -> DfTypeSignature {
        match self {
            TypeSignature::Exact(kinds) => {
                let data_types = kinds.iter().map(|v| DataType::from(*v)).collect();
                DfTypeSignature::Exact(data_types)
            }
            TypeSignature::Uniform(num, kinds) => {
                let data_types = kinds.iter().map(|v| DataType::from(*v)).collect();
                DfTypeSignature::Uniform(*num, data_types)
            }
            TypeSignature::OneOf(sigs) => {
                let df_sigs = sigs
                    .iter()
                    .map(|v| v.to_datafusion_type_signature())
                    .collect();
                DfTypeSignature::OneOf(df_sigs)
            }
        }
    }
}

/// A scalar function's return type.
#[derive(Debug)]
pub struct ReturnType {
    kind: DatumKind,
}

impl ReturnType {
    pub(crate) fn to_datafusion_return_type(&self) -> ReturnTypeFunction {
        let data_type = Arc::new(DataType::from(self.kind));
        Arc::new(move |_| Ok(data_type.clone()))
    }
}

pub struct ScalarFunction {
    signature: TypeSignature,
    return_type: ReturnType,
    df_scalar_fn: ScalarFunctionImplementation,
}

impl ScalarFunction {
    pub fn make_by_fn<F>(signature: TypeSignature, return_type: DatumKind, func: F) -> Self
    where
        F: Fn(&[ColumnarValue]) -> Result<ColumnarValue> + Send + Sync + 'static,
    {
        let return_type = ReturnType { kind: return_type };

        // Adapter to map func to Fn(&[DfColumnarValue]) -> Result<DfColumnarValue>
        let df_adapter = move |df_args: &[DfColumnarValue]| {
            // Convert df_args from DfColumnarValue to ColumnarValue.
            let mut values: SmallVec<[ColumnarValue; FUNC_ARG_NUM]> =
                SmallVec::with_capacity(df_args.len());
            for df_arg in df_args {
                let value = ColumnarValue::try_from_df_columnar_value(df_arg).map_err(|e| {
                    DataFusionError::Internal(format!(
                        "Failed to convert datafusion columnar value, err:{e}"
                    ))
                })?;
                values.push(value);
            }

            // Execute our function.
            let result_value = func(&values).map_err(|e| {
                DataFusionError::Execution(format!("Failed to execute function, err:{e}"))
            })?;

            // Convert the result value to DfColumnarValue.
            Ok(result_value.into_df_columnar_value())
        };

        let df_scalar_fn = Arc::new(df_adapter);

        Self {
            signature,
            return_type,
            df_scalar_fn,
        }
    }

    #[inline]
    pub fn signature(&self) -> &TypeSignature {
        &self.signature
    }

    #[inline]
    pub fn return_type(&self) -> &ReturnType {
        &self.return_type
    }

    #[inline]
    pub(crate) fn to_datafusion_function(&self) -> ScalarFunctionImplementation {
        self.df_scalar_fn.clone()
    }
}

pub struct AggregateFunction {
    type_signature: TypeSignature,
    return_type: ReturnType,
    df_accumulator: AccumulatorFunctionImplementation,
    state_type: Vec<DatumKind>,
}

impl AggregateFunction {
    pub fn make_by_fn<F, A>(
        type_signature: TypeSignature,
        return_type: DatumKind,
        state_type: Vec<DatumKind>,
        accumulator_fn: F,
    ) -> Self
    where
        F: Fn(&DataType) -> Result<A> + Send + Sync + 'static,
        A: Accumulator + 'static,
    {
        // Create accumulator.
        let df_adapter = move |data_type: &DataType| {
            let accumulator = accumulator_fn(data_type).map_err(|e| {
                DataFusionError::Execution(format!("Failed to create accumulator, err:{e}"))
            })?;
            let accumulator = Box::new(ToDfAccumulator::new(accumulator));

            Ok(accumulator as _)
        };
        let df_accumulator = Arc::new(df_adapter);

        // Create return type.
        let return_type = ReturnType { kind: return_type };

        Self {
            type_signature,
            return_type,
            df_accumulator,
            state_type,
        }
    }

    #[inline]
    pub fn signature(&self) -> &TypeSignature {
        &self.type_signature
    }

    #[inline]
    pub fn return_type(&self) -> &ReturnType {
        &self.return_type
    }

    #[inline]
    pub(crate) fn to_datafusion_accumulator(&self) -> AccumulatorFunctionImplementation {
        self.df_accumulator.clone()
    }

    pub(crate) fn to_datafusion_state_type(&self) -> StateTypeFunction {
        let data_types = Arc::new(
            self.state_type
                .iter()
                .map(|kind| DataType::from(*kind))
                .collect::<Vec<_>>(),
        );
        Arc::new(move |_| Ok(data_types.clone()))
    }
}
