// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

//! Column
use std::sync::Arc;

use arrow::{
    array::{
        Array, ArrayBuilder, ArrayRef, BinaryArray, BinaryBuilder, BooleanArray, BooleanBuilder,
        Date32Array as DateArray, Date32Builder as DateBuilder, Float32Array as FloatArray,
        Float32Builder as FloatBuilder, Float64Array as DoubleArray,
        Float64Builder as DoubleBuilder, Int16Array, Int16Builder, Int32Array, Int32Builder,
        Int64Array, Int64Builder, Int8Array, Int8Builder, NullArray, StringArray, StringBuilder,
        Time64NanosecondArray as TimeArray, Time64NanosecondBuilder as TimeBuilder,
        TimestampMillisecondArray, TimestampMillisecondBuilder, UInt16Array, UInt16Builder,
        UInt32Array, UInt32Builder, UInt64Array, UInt64Builder, UInt8Array, UInt8Builder, StringDictionaryBuilder, DictionaryArray, ArrayAccessor,
    },
    datatypes::{Int32Type, DataType, TimeUnit},
    error::ArrowError,
};
use datafusion::physical_plan::{
    expressions::{cast_column, DEFAULT_DATAFUSION_CAST_OPTIONS},
    ColumnarValue,
};
use paste::paste;
use snafu::{Backtrace, OptionExt, ResultExt, Snafu};

use crate::{
    bytes::Bytes,
    datum::{Datum, DatumKind, DatumView},
    string::StringBytes,
    time::Timestamp,
};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display(
        "Invalid array type, datum_kind:{:?}, data_type:{:?}.\nBacktrace:\n{}",
        datum_kind,
        data_type,
        backtrace
    ))]
    InvalidArrayType {
        datum_kind: DatumKind,
        data_type: DataType,
        backtrace: Backtrace,
    },

    #[snafu(display("Failed to append value, err:{}.\nBacktrace:\n{}", source, backtrace))]
    Append {
        source: ArrowError,
        backtrace: Backtrace,
    },

    #[snafu(display(
        "Data type conflict, expect:{:?}, given:{:?}.\nBacktrace:\n{}",
        expect,
        given,
        backtrace
    ))]
    ConflictType {
        expect: DatumKind,
        given: DatumKind,
        backtrace: Backtrace,
    },

    #[snafu(display(
        "Failed to convert arrow data type, data_type:{}.\nBacktrace:\n{}",
        data_type,
        backtrace
    ))]
    UnsupportedArray {
        data_type: DataType,
        backtrace: Backtrace,
    },

    #[snafu(display(
        "Failed to cast nanosecond to millisecond, data_type:{}. err:{}",
        data_type,
        source,
    ))]
    CastTimestamp {
        data_type: DataType,
        source: datafusion::error::DataFusionError,
    },

    #[snafu(display("Operation not yet implemented."))]
    NotImplemented,
}

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug)]
pub struct NullColumn(NullArray);

impl NullColumn {
    fn new_null(rows: usize) -> Self {
        Self(NullArray::new(rows))
    }

    /// Only the first datum of NullColumn is considered not duplicated.
    #[inline]
    pub fn dedup(&self, selected: &mut [bool]) {
        if !self.0.is_empty() {
            selected[0] = true;
        }
    }
}

// TODO(yingwen): Builder for columns.

macro_rules! define_numeric_column {
    ($($Kind: ident), *) => {
        $(paste! {
            #[derive(Debug)]
            pub struct [<$Kind Column>]([<$Kind Array>]);

            #[inline]
            fn [<get_ $Kind:lower _datum>](array: &[<$Kind Array>], index: usize) -> Datum {
                let value = array.value(index);
                Datum::$Kind(value)
            }

            #[inline]
            fn [<get_ $Kind:lower _datum_view>](array: &[<$Kind Array>], index: usize) -> DatumView {
                let value = array.value(index);
                DatumView::$Kind(value)
            }
        })*
    }
}

define_numeric_column!(
    Float, Double, UInt64, UInt32, UInt16, UInt8, Int64, Int32, Int16, Int8, Boolean
);

#[derive(Debug)]
pub struct TimestampColumn(TimestampMillisecondArray);

#[derive(Debug)]
pub struct VarbinaryColumn(BinaryArray);

#[derive(Debug)]
pub struct StringColumn(StringArray);

#[derive(Debug)]
pub struct StringDictionaryColumn(DictionaryArray<Int32Type>);

#[derive(Debug)]
pub struct DateColumn(DateArray);

#[derive(Debug)]
pub struct TimeColumn(TimeArray);

#[inline]
fn get_null_datum_view(_array: &NullArray, _index: usize) -> DatumView {
    DatumView::Null
}

#[inline]
fn get_timestamp_datum_view(array: &TimestampMillisecondArray, index: usize) -> DatumView {
    let value = array.value(index);
    DatumView::Timestamp(Timestamp::new(value))
}

#[inline]
fn get_varbinary_datum_view(array: &BinaryArray, index: usize) -> DatumView {
    let value = array.value(index);
    DatumView::Varbinary(value)
}

#[inline]
fn get_string_datum_view(array: &StringArray, index: usize) -> DatumView {
    let value = array.value(index);
    DatumView::String(value)
}

#[inline]
fn get_date_datum_view(array: &DateArray, index: usize) -> DatumView {
    let value = array.value(index);
    DatumView::Date(value)
}

#[inline]
fn get_time_datum_view(array: &TimeArray, index: usize) -> DatumView {
    let value = array.value(index);
    DatumView::Time(value)
}

#[inline]
fn get_null_datum(_array: &NullArray, _index: usize) -> Datum {
    Datum::Null
}

#[inline]
fn get_timestamp_datum(array: &TimestampMillisecondArray, index: usize) -> Datum {
    let value = array.value(index);
    Datum::Timestamp(Timestamp::new(value))
}

// TODO(yingwen): Avoid clone of data.
// Require a clone.
#[inline]
fn get_varbinary_datum(array: &BinaryArray, index: usize) -> Datum {
    let value = array.value(index);
    Datum::Varbinary(Bytes::copy_from_slice(value))
}

// TODO(yingwen): Avoid clone of data.
// Require a clone.
#[inline]
fn get_string_datum(array: &StringArray, index: usize) -> Datum {
    let value = array.value(index);
    Datum::String(StringBytes::copy_from_str(value))
}

#[inline]
fn get_date_datum(array: &DateArray, index: usize) -> Datum {
    let value = array.value(index);
    Datum::Date(value)
}

#[inline]
fn get_time_datum(array: &TimeArray, index: usize) -> Datum {
    let value = array.value(index);
    Datum::Time(value)
}

macro_rules! impl_column {
    ($Column: ident, $get_datum: expr, $get_datum_view: expr) => {
        impl $Column {
            /// Get datum by index.
            pub fn datum_opt(&self, index: usize) -> Option<Datum> {
                // Do bound check.
                if index >= self.0.len() {
                    return None;
                }

                Some(self.datum(index))
            }

            pub fn datum_view_opt(&self, index: usize) -> Option<DatumView> {
                if index >= self.0.len() {
                    return None;
                }

                Some(self.datum_view(index))
            }

            pub fn datum_view(&self, index: usize) -> DatumView {
                // If this datum is null.
                if self.0.is_null(index) {
                    return DatumView::Null;
                }

                $get_datum_view(&self.0, index)
            }

            pub fn datum(&self, index: usize) -> Datum {
                // If this datum is null.
                if self.0.is_null(index) {
                    return Datum::Null;
                }

                $get_datum(&self.0, index)
            }

            #[inline]
            pub fn num_rows(&self) -> usize {
                self.0.len()
            }

            #[inline]
            pub fn is_empty(&self) -> bool {
                self.num_rows() == 0
            }
        }
    };
}

impl_column!(NullColumn, get_null_datum, get_null_datum_view);
impl_column!(
    TimestampColumn,
    get_timestamp_datum,
    get_timestamp_datum_view
);
impl_column!(
    VarbinaryColumn,
    get_varbinary_datum,
    get_varbinary_datum_view
);
impl_column!(StringColumn, get_string_datum, get_string_datum_view);

// TODO
// impl_column!(StringDictionaryColumn, get_string_datum, get_string_datum_view);
impl StringDictionaryColumn {
    #[doc = " Get datum by index."]
    pub fn datum_opt(&self, index: usize) -> Option<Datum> {
        if index >= self.0.len() {
            return None;
        }
        Some(self.datum(index))
    }

    pub fn datum_view_opt(&self, index: usize) -> Option<DatumView> {
        if index >= self.0.len() {
            return None;
        }
        Some(self.datum_view(index))
    }

    pub fn datum_view(&self, index: usize) -> DatumView {
        if self.0.is_null(index) {
            return DatumView::Null;
        }
       // TODO : Is this the efficient way?
       DatumView::String(self.0.downcast_dict::<StringArray>().unwrap().value(index).into())
    }

    pub fn datum(&self, index: usize) -> Datum {
        if self.0.is_null(index) {
            return Datum::Null;
        }
        // TODO : Is this the efficient way?
        Datum::String(self.0.downcast_dict::<StringArray>().unwrap().value(index).into())
    }

    #[inline]
    pub fn num_rows(&self) -> usize {
        self.0.len()
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.num_rows() == 0
    }
}


macro_rules! impl_dedup {
    ($Column: ident) => {
        impl $Column {
            /// If datum i is not equal to previous datum i - 1, mark `selected[i]` to
            /// true.
            ///
            /// The first datum is marked to true.
            ///
            /// The size of selected must equal to the size of this column and
            /// initialized to false.
            #[allow(clippy::float_cmp)]
            pub fn dedup(&self, selected: &mut [bool]) {
                if self.0.is_empty() {
                    return;
                }

                selected[0] = true;
                for i in 1..self.0.len() {
                    let current = self.0.value(i);
                    let prev = self.0.value(i - 1);

                    if current != prev {
                        selected[i] = true;
                    }
                }
            }
        }
    };
}

impl_dedup!(TimestampColumn);
impl_dedup!(VarbinaryColumn);
impl_dedup!(StringColumn);
// impl_dedup!(StringDictionaryColumn);
// TODO
impl StringDictionaryColumn {
    #[doc = " If datum i is not equal to previous datum i - 1, mark `selected[i]` to"]
    #[doc = " true."]
    #[doc = ""]
    #[doc = " The first datum is marked to true."]
    #[doc = ""]
    #[doc = " The size of selected must equal to the size of this column and"]
    #[doc = " initialized to false."]
    #[allow(clippy::float_cmp)]
    pub fn dedup(&self, selected: &mut [bool]) {
        if self.0.is_empty() {
            return;
        }
        selected[0] = true;
        for i in 1..self.0.len() {
            let current = self.0.key(i);
            let prev = self.0.key(i - 1);
            if current != prev {
                selected[i] = true;
            }
        }
    }
}

macro_rules! impl_new_null {
    ($Column: ident, $Builder: ident) => {
        impl $Column {
            /// Create a column that all values are null.
            fn new_null(num_rows: usize) -> Self {
                let mut builder = $Builder::with_capacity(num_rows);
                for _ in 0..num_rows {
                    builder.append_null();
                }
                let array = builder.finish();

                Self(array)
            }
        }
    };
}

impl_new_null!(TimestampColumn, TimestampMillisecondBuilder);

macro_rules! impl_from_array_and_slice {
    ($Column: ident, $ArrayType: ident) => {
        impl From<$ArrayType> for $Column {
            fn from(array: $ArrayType) -> Self {
                Self(array)
            }
        }

        impl From<&$ArrayType> for $Column {
            fn from(array_ref: &$ArrayType) -> Self {
                // We need to clone the [arrow::array::ArrayData], which clones
                // the underlying vector of [arrow::buffer::Buffer] and Bitmap (also
                // holds a Buffer), thus require some allocation. However, the Buffer is
                // managed by Arc, so cloning the buffer is not too expensive.
                let array_data = array_ref.into_data();
                let array = $ArrayType::from(array_data);

                Self(array)
            }
        }

        impl $Column {
            fn to_arrow_array(&self) -> $ArrayType {
                // Clone the array data.
                let array_data = self.0.clone().into_data();
                $ArrayType::from(array_data)
            }

            /// Returns a zero-copy slice of this array with the indicated offset and
            /// length.
            ///
            /// Panics if offset with length is greater than column length.
            fn slice(&self, offset: usize, length: usize) -> Self {
                let array_slice = self.0.slice(offset, length);
                // Clone the slice data.
                let array_data = array_slice.into_data();
                let array = $ArrayType::from(array_data);

                Self(array)
            }
        }
    };
}

impl_from_array_and_slice!(NullColumn, NullArray);
impl_from_array_and_slice!(TimestampColumn, TimestampMillisecondArray);
impl_from_array_and_slice!(VarbinaryColumn, BinaryArray);
impl_from_array_and_slice!(StringColumn, StringArray);
// impl_from_array_and_slice!(StringDictionaryColumn, DictionaryArray<Int32Type>);


impl From<DictionaryArray<Int32Type>> for StringDictionaryColumn {
    fn from(array: DictionaryArray<Int32Type>) -> Self {
        Self(array)
    }
}
impl From<&DictionaryArray<Int32Type>> for StringDictionaryColumn {
    fn from(array_ref: &DictionaryArray<Int32Type>) -> Self {
        let array_data = array_ref.into_data();
        let array = DictionaryArray::<Int32Type>::from(array_data);
        Self(array)
    }
}
impl StringDictionaryColumn {
    fn to_arrow_array(&self) -> DictionaryArray<Int32Type> {
        let array_data = self.0.clone().into_data();
        DictionaryArray::<Int32Type>::from(array_data)
    }

    #[doc = " Returns a zero-copy slice of this array with the indicated offset and"]
    #[doc = " length."]
    #[doc = ""]
    #[doc = " Panics if offset with length is greater than column length."]
    fn slice(&self, offset: usize, length: usize) -> Self {
        let array_slice = self.0.slice(offset, length);
        let array_data = array_slice.into_data();
        let array = DictionaryArray::<Int32Type>::from(array_data);
        Self(array)
    }
}


macro_rules! impl_iter {
    ($Column: ident, $Value: ident) => {
        impl $Column {
            /// Iter column values.
            pub fn iter(&self) -> impl Iterator<Item = Option<$Value>> + '_ {
                self.0.iter()
            }
        }
    };
}

macro_rules! impl_iter_map {
    ($Column: ident, $Value: ident) => {
        impl $Column {
            /// Iter column values.
            pub fn iter(&self) -> impl Iterator<Item = Option<$Value>> + '_ {
                self.0.iter().map(|v| v.map($Value::from))
            }
        }
    };
}

impl_iter_map!(TimestampColumn, Timestamp);

impl VarbinaryColumn {
    fn new_null(num_rows: usize) -> Self {
        let mut builder = BinaryBuilder::with_capacity(num_rows, 0usize);
        for _ in 0..num_rows {
            builder.append_null();
        }
        let array = builder.finish();

        Self(array)
    }
}

impl StringColumn {
    /// Create a column that all values are null.
    fn new_null(num_rows: usize) -> Self {
        let mut builder = StringBuilder::with_capacity(num_rows, 0usize);
        for _ in 0..num_rows {
            builder.append_null();
        }
        let array = builder.finish();

        Self(array)
    }
}
impl StringDictionaryColumn {
    /// Create a column that all values are null.
    fn new_null(num_rows: usize) -> Self {
        let mut builder = StringDictionaryBuilder::<Int32Type>::new();
        for _ in 0..num_rows {
            builder.append_null();
        }
        let array = builder.finish();

        Self(array)
    }
}


macro_rules! impl_numeric_column {
    ($(($Kind: ident, $type: ty)), *) =>  {
        $(
            paste! {
                impl_column!([<$Kind Column>], [<get_ $Kind:lower _datum>], [<get_ $Kind:lower _datum_view>]);
                impl_from_array_and_slice!([<$Kind Column>], [<$Kind Array>]);
                impl_new_null!([<$Kind Column>], [<$Kind Builder>]);
                impl_iter!([<$Kind Column>], $type);
                impl_dedup!([<$Kind Column>]);
            }
        )*
    }
}

impl_numeric_column!(
    (Double, f64),
    (Float, f32),
    (UInt64, u64),
    (UInt32, u32),
    (UInt16, u16),
    (UInt8, u8),
    (Int64, i64),
    (Int32, i32),
    (Int16, i16),
    (Int8, i8),
    (Boolean, bool),
    (Date, i32),
    (Time, i64)
);

macro_rules! impl_numeric_value {
    ($Column: ident, $Value: ident) => {
        impl $Column {
            /// Get value at index.
            pub fn value(&self, index: usize) -> Option<$Value> {
                if self.0.is_valid(index) {
                    unsafe { Some(self.0.value_unchecked(index)) }
                } else {
                    None
                }
            }
        }
    };
}

macro_rules! batch_impl_numeric_value {
    ($(($Kind: ident, $type: ty)), *) =>  {
        $(
            paste! {
                impl_numeric_value!([<$Kind Column>], $type);
            }
        )*
    }
}

batch_impl_numeric_value!(
    (Timestamp, i64),
    (Double, f64),
    (Float, f32),
    (UInt64, u64),
    (UInt32, u32),
    (UInt16, u16),
    (UInt8, u8),
    (Int64, i64),
    (Int32, i32),
    (Int16, i16),
    (Int8, i8),
    (Boolean, bool),
    (Date, i32),
    (Time, i64)
);

impl VarbinaryColumn {
    pub fn iter(&self) -> impl Iterator<Item = Option<&[u8]>> + '_ {
        self.0.iter()
    }

    pub fn value(&self, index: usize) -> Option<&[u8]> {
        if self.0.is_valid(index) {
            unsafe { Some(self.0.value_unchecked(index)) }
        } else {
            None
        }
    }
}

impl StringColumn {
    pub fn iter(&self) -> impl Iterator<Item = Option<&str>> + '_ {
        self.0.iter()
    }

    pub fn value(&self, index: usize) -> Option<&str> {
        if self.0.is_valid(index) {
            unsafe { Some(self.0.value_unchecked(index)) }
        } else {
            None
        }
    }
}

// impl StringDictionaryColumn {
//     pub fn iter(&self) -> impl Iterator<Item = Option<&str>> + '_ {
//         self.0.iter()
//     }

//     pub fn value(&self, index: usize) -> Option<&str> {
//         if self.0.is_valid(index) {
//             unsafe { Some(self.0.value_unchecked(index)) }
//         } else {
//             None
//         }
//     }
// }


macro_rules! impl_column_block {
    ($($Kind: ident), *) => {
        impl ColumnBlock {
            pub fn datum_kind(&self) -> DatumKind {
                match self {
                    ColumnBlock::StringDictionary(_) => DatumKind::String,
                    $(ColumnBlock::$Kind(_) => DatumKind::$Kind,)*
                }
            }

            pub fn datum_opt(&self, index: usize) -> Option<Datum> {
                match self {
                    ColumnBlock::StringDictionary(col) => col.datum_opt(index),
                    $(ColumnBlock::$Kind(col) => col.datum_opt(index),)*
                }
            }

            pub fn datum_view_opt(&self, index: usize) -> Option<DatumView> {
                match self {
                    ColumnBlock::StringDictionary(col) => col.datum_view_opt(index),
                    $(ColumnBlock::$Kind(col) => col.datum_view_opt(index),)*
                }
            }

            /// Panic if index is out fo bound.
            pub fn datum_view(&self, index: usize) -> DatumView {
                match self {
                    ColumnBlock::StringDictionary(col) => col.datum_view(index),
                    $(ColumnBlock::$Kind(col) => col.datum_view(index),)*
                }
            }

            /// Panic if index is out fo bound.
            pub fn datum(&self, index: usize) -> Datum {
                match self {
                    ColumnBlock::StringDictionary(col) => col.datum(index),
                    $(ColumnBlock::$Kind(col) => col.datum(index),)*
                }
            }

            pub fn num_rows(&self) -> usize {
                match self {
                    ColumnBlock::StringDictionary(col) => col.num_rows(),
                    $(ColumnBlock::$Kind(col) => col.num_rows(),)*
                }
            }

            pub fn to_arrow_array_ref(&self) -> ArrayRef {
                match self {
                    ColumnBlock::StringDictionary(col) =>  Arc::new(col.to_arrow_array()),
                    $(ColumnBlock::$Kind(col) => Arc::new(col.to_arrow_array()),)*
                }
            }

            /// If datum i is not equal to previous datum i - 1, mark `selected[i]` to true.
            ///
            /// The first datum is not marked to true.
            pub fn dedup(&self, selected: &mut [bool]) {
                match self {
                    ColumnBlock::StringDictionary(col) =>  col.dedup(selected),
                    $(ColumnBlock::$Kind(col) => col.dedup(selected),)*
                }
            }

            /// Returns a zero-copy slice of this array with the indicated offset and length.
            ///
            /// Panics if offset with length is greater than column length.
            #[must_use]
            pub fn slice(&self, offset: usize, length: usize) -> Self {
                match self {
                    ColumnBlock::StringDictionary(col) =>  ColumnBlock::StringDictionary(col.slice(offset, length)),
                    $(ColumnBlock::$Kind(col) => ColumnBlock::$Kind(col.slice(offset, length)),)*
                }
            }
        }

        $(paste! {
            impl From<[<$Kind Column>]> for ColumnBlock {
                fn from(column: [<$Kind Column>]) -> Self {
                    Self::$Kind(column)
                }
            }
        })*

        impl From<StringDictionaryColumn> for ColumnBlock {
            fn from(column: StringDictionaryColumn) -> Self {
                Self::StringDictionary(column)
            }
        }
    };
}

impl_column_block!(
    Null, Timestamp, Double, Float, Varbinary, String, UInt64, UInt32, UInt16, UInt8, Int64, Int32,
    Int16, Int8, Boolean, Date, Time
);

// TODO(yingwen): We can add a unsafe function that don't do bound check.

macro_rules! define_column_block {
    ($($Kind: ident), *) => {
        paste! {
            #[derive(Debug)]
            pub enum ColumnBlock {
                Null(NullColumn),
                StringDictionary(StringDictionaryColumn),
                $(
                    $Kind([<$Kind Column>]),
                )*
            }

            impl ColumnBlock {
                pub fn try_from_arrow_array_ref(datum_kind: &DatumKind, array: &ArrayRef) -> Result<Self> {
                    let is_dictionary : bool =  if let DataType::Dictionary(..)  = array.data_type() {
                        true
                    } else {
                        false
                    };
                    let column = match datum_kind {
                        DatumKind::Null => ColumnBlock::Null(NullColumn::new_null(array.len())),
                        $(
                            DatumKind::$Kind => {
                                let mills_array;
                                let cast_column = match array.data_type() {
                                    DataType::Timestamp(TimeUnit::Nanosecond, None) =>  {
                                        mills_array = cast_nanosecond_to_mills(array)?;
                                        cast_array(datum_kind, &mills_array)?
                                    },
                                    _ => {
                                        cast_array(datum_kind, array)?
                                    }
                                };

                                ColumnBlock::$Kind([<$Kind Column>]::from(cast_column))
                            }
                        )*
                    };
                    Ok(column)
                }

                pub fn new_null_with_type(kind: &DatumKind, rows: usize) -> Result<Self> {
                    let block = match kind {
                        DatumKind::Null => ColumnBlock::Null(NullColumn::new_null(rows)),
                        $(
                            DatumKind::$Kind => ColumnBlock::$Kind([<$Kind Column>]::new_null(rows)),
                        )*
                    };

                    Ok(block)
                }
            }
        }
    }
}

// Define column blocks, Null is defined explicitly in macro.
define_column_block!(
    Timestamp, Double, Float, Varbinary, String, UInt64, UInt32, UInt16, UInt8, Int64, Int32,
    Int16, Int8, Boolean, Date, Time
);

impl ColumnBlock {
    pub fn try_cast_arrow_array_ref(array: &ArrayRef) -> Result<Self> {
        let datum_kind =
            DatumKind::from_data_type(array.data_type()).with_context(|| UnsupportedArray {
                data_type: array.data_type().clone(),
            })?;
        Self::try_from_arrow_array_ref(&datum_kind, array)
    }

    pub fn new_null(rows: usize) -> Self {
        Self::Null(NullColumn::new_null(rows))
    }

    pub fn as_timestamp(&self) -> Option<&TimestampColumn> {
        match self {
            ColumnBlock::Timestamp(c) => Some(c),
            _ => None,
        }
    }
}

// TODO: This is a temp workaround to support nanoseconds, a better way
// is to support nanoseconds natively.
// This is also required for influxql.
pub fn cast_nanosecond_to_mills(array: &ArrayRef) -> Result<Arc<dyn Array>> {
    let column = ColumnarValue::Array(array.clone());
    let mills_column = cast_column(
        &column,
        &DataType::Timestamp(TimeUnit::Millisecond, None),
        &DEFAULT_DATAFUSION_CAST_OPTIONS,
    )
    .with_context(|| CastTimestamp {
        data_type: DataType::Timestamp(TimeUnit::Millisecond, None),
    })?;

    match mills_column {
        ColumnarValue::Array(array) => Ok(array),
        _ => Err(Error::NotImplemented),
    }
}

fn cast_array<'a, T: 'static>(datum_kind: &DatumKind, array: &'a ArrayRef) -> Result<&'a T> {
    array
        .as_any()
        .downcast_ref::<T>()
        .with_context(|| InvalidArrayType {
            datum_kind: *datum_kind,
            data_type: array.data_type().clone(),
        })
}

macro_rules! append_datum {
    ($Kind: ident, $builder: ident, $DatumType: ident, $datum: ident) => {
        match $datum {
            $DatumType::Null => Ok($builder.append_null()),
            $DatumType::$Kind(v) => Ok($builder.append_value(v)),
            _ => ConflictType {
                expect: DatumKind::$Kind,
                given: $datum.kind(),
            }
            .fail(),
        }
    };
}

macro_rules! append_datum_into {
    ($Kind: ident, $builder: ident, $DatumType: ident, $datum: ident) => {
        match $datum {
            $DatumType::Null => Ok($builder.append_null()),
            $DatumType::$Kind(v) => Ok($builder.append_value(v.into())),
            _ => ConflictType {
                expect: DatumKind::$Kind,
                given: $datum.kind(),
            }
            .fail(),
        }
    };
}

macro_rules! append_block {
    ($Kind: ident, $builder: ident, $BlockType: ident, $block: ident, $start: ident, $len: ident) => {
        match $block {
            $BlockType::Null(v) => {
                let end = std::cmp::min($start + $len, v.num_rows());
                for _ in $start..end {
                    $builder.append_null();
                }
                Ok(())
            }
            $BlockType::$Kind(v) => {
                // There is no convenient api to copy a range of data from array to builder, so
                // we still need to clone value one by one using a for loop.
                let end = std::cmp::min($start + $len, v.num_rows());
                for i in $start..end {
                    let value_opt = v.value(i);
                    match value_opt {
                        Some(value) => {
                            $builder.append_value(value);
                        }
                        None => {
                            $builder.append_null();
                        }
                    }
                }
                Ok(())
            }
            _ => ConflictType {
                expect: DatumKind::$Kind,
                given: $block.datum_kind(),
            }
            .fail(),
        }
    };
}

macro_rules! define_column_block_builder {
    ($(($Kind: ident, $Builder: ident)), *) => {
        paste! {
            // #[derive(Debug)]
            pub enum ColumnBlockBuilder {
                Null { rows: usize },
                Timestamp(TimestampMillisecondBuilder),
                Varbinary(BinaryBuilder),
                String(StringBuilder),
                Date(DateBuilder),
                Time(TimeBuilder),
                Dictionary(StringDictionaryBuilder::<Int32Type>),
                $(
                    $Kind($Builder),
                )*
            }

            impl ColumnBlockBuilder {
                /// Create by data type with initial capacity
                pub fn with_capacity(data_type: &DatumKind, item_capacity: usize, is_dictionary : bool) -> Self {
                    match data_type {
                        DatumKind::Null => Self::Null { rows: 0 },
                        DatumKind::Timestamp => Self::Timestamp(TimestampMillisecondBuilder::with_capacity(item_capacity)),
                        // The data_capacity is set as 1024, because the item is variable-size type.
                        DatumKind::Varbinary => Self::Varbinary(BinaryBuilder::with_capacity(item_capacity, 1024)),
                        DatumKind::String if is_dictionary => Self::String(StringBuilder::with_capacity(item_capacity, 1024)),
                        DatumKind::String if !is_dictionary => Self::Dictionary(StringDictionaryBuilder::<Int32Type>::new()),
                        DatumKind::String => Self::Dictionary(StringDictionaryBuilder::<Int32Type>::new()),
                        DatumKind::Date => Self::Date(DateBuilder::with_capacity(item_capacity)),
                        DatumKind::Time => Self::Time(TimeBuilder::with_capacity(item_capacity)),
                        DatumKind::Time => Self::Time(TimeBuilder::with_capacity(item_capacity)),
                        $(
                            DatumKind::$Kind => Self::$Kind($Builder::with_capacity(item_capacity)),
                        )*
                    }
                }

                /// Append the datum into the builder, the datum should have same the data
                /// type of builder
                pub fn append(&mut self, datum: Datum) -> Result<()> {
                    let given = datum.kind();
                    match self {
                        Self::Null { rows } => match datum {
                            Datum::Null => {
                                *rows += 1;
                                Ok(())
                            }
                            _ => ConflictType {
                                expect: DatumKind::Null,
                                given,
                            }
                            .fail(),
                        },
                        Self::Timestamp(builder) => append_datum_into!(Timestamp, builder, Datum, datum),
                        Self::Varbinary(builder) => append_datum!(Varbinary, builder, Datum, datum),
                        Self::String(builder) => append_datum!(String, builder, Datum, datum),
                        Self::Date(builder) => append_datum!(Date, builder, Datum, datum),
                        Self::Time(builder) => append_datum!(Time, builder, Datum, datum),
                        Self::Dictionary(builder) => {
                            Ok(())
                        },
                        $(
                            Self::$Kind(builder) => append_datum!($Kind, builder, Datum, datum),
                        )*
                    }
                }

                /// Append the [DatumView] into the builder, the datum view should have same the data
                /// type of builder
                pub fn append_view<'a>(&mut self, datum: DatumView<'a>) -> Result<()> {
                    let given = datum.kind();
                    match self {
                        Self::Null { rows } => match datum {
                            DatumView::Null => {
                                *rows += 1;
                                Ok(())
                            }
                            _ => ConflictType {
                                expect: DatumKind::Null,
                                given,
                            }
                            .fail(),
                        },
                        Self::Timestamp(builder) => append_datum_into!(Timestamp, builder, DatumView, datum),
                        Self::Varbinary(builder) => append_datum!(Varbinary, builder, DatumView, datum),
                        Self::String(builder) => append_datum!(String, builder, DatumView, datum),
                        Self::Date(builder) => append_datum!(Date, builder, DatumView, datum),
                        Self::Time(builder) => append_datum!(Time, builder, DatumView, datum),
                        Self::Dictionary(builder) => {
                            Ok(())
                        },
                        $(
                            Self::$Kind(builder) => append_datum!($Kind, builder, DatumView, datum),
                        )*
                    }
                }

                /// Append rows in [start..start + len) from `block` to the builder.
                ///
                /// Returns rows actually appended.
                pub fn append_block_range(&mut self, block: &ColumnBlock, start: usize, len: usize) -> Result<()> {
                    match self {
                        Self::Null { rows } => {
                            if start + len >= block.num_rows() {
                                *rows += block.num_rows() - start;
                            } else {
                                *rows += len;
                            }
                            Ok(())
                        },
                        Self::Timestamp(builder) => append_block!(Timestamp, builder, ColumnBlock, block, start, len),
                        Self::Varbinary(builder) => append_block!(Varbinary, builder, ColumnBlock, block, start, len),
                        Self::String(builder) => append_block!(String, builder, ColumnBlock, block, start, len),
                        Self::Date(builder) => append_block!(Date, builder, ColumnBlock, block, start, len),
                        Self::Time(builder) => append_block!(Time, builder, ColumnBlock, block, start, len),
                        Self::Dictionary(builder) => {
                            Ok(())
                        },
                        $(
                            Self::$Kind(builder) => append_block!($Kind, builder, ColumnBlock, block, start, len),
                        )*
                    }
                }

                pub fn len(&self) -> usize {
                    match &self {
                        Self::Null { rows } => *rows,
                        Self::Timestamp(builder) => builder.len(),
                        Self::Varbinary(builder) => builder.len(),
                        Self::String(builder) => builder.len(),
                        Self::Date(builder) => builder.len(),
                        Self::Time(builder) => builder.len(),
                        Self::Dictionary(builder) => builder.len(),
                        $(
                            Self::$Kind(builder) =>  builder.len(),
                        )*
                    }
                }

                // Build and reset the builder.
                pub fn build(&mut self) -> ColumnBlock {
                    match self {
                        Self::Null { rows } => {
                            let block = ColumnBlock::new_null(*rows);
                            *rows = 0;
                            block
                        }
                        Self::Timestamp(builder) => TimestampColumn::from(builder.finish()).into(),
                        Self::Varbinary(builder) => VarbinaryColumn::from(builder.finish()).into(),
                        Self::String(builder) => StringColumn::from(builder.finish()).into(),
                        Self::Date(builder) => DateColumn::from(builder.finish()).into(),
                        Self::Time(builder) => TimeColumn::from(builder.finish()).into(),
                        Self::Dictionary(builder) => {
                            StringDictionaryColumn::from(builder.finish()).into()
                        },
                        $(
                            Self::$Kind(builder) => [<$Kind Column>]::from(builder.finish()).into(),
                        )*
                    }
                }
            }
        }
    }
}

// Define column block builders, Null and Timestamp are defined explicitly in
// macro.
define_column_block_builder!(
    (Double, DoubleBuilder),
    (Float, FloatBuilder),
    (UInt64, UInt64Builder),
    (UInt32, UInt32Builder),
    (UInt16, UInt16Builder),
    (UInt8, UInt8Builder),
    (Int64, Int64Builder),
    (Int32, Int32Builder),
    (Int16, Int16Builder),
    (Int8, Int8Builder),
    (Boolean, BooleanBuilder)
);

impl ColumnBlockBuilder {
    /// Create by data type
    pub fn new(data_type: &DatumKind, is_dictionry : bool) -> Self {
        Self::with_capacity(data_type.into(), 0, is_dictionry)
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Clear the builder by calling `build()` and drop the built result.
    pub fn clear(&mut self) {
        let _ = self.build();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tests::{build_rows, build_schema};

    #[test]
    fn test_column_block_builder() {
        let schema = build_schema();
        let rows = build_rows();
        // DatumKind::Varbinary
        let column = schema.column(0);
        let mut builder = ColumnBlockBuilder::with_capacity(&column.data_type, 2, false);

        // append
        builder.append(rows[0][0].clone()).unwrap();
        let ret = builder.append(rows[0][1].clone());
        assert!(ret.is_err());

        // append_view
        builder.append_view(rows[1][0].as_view()).unwrap();
        let ret = builder.append_view(rows[0][1].as_view());
        assert!(ret.is_err());

        let column_block = builder.build();
        assert_eq!(column_block.num_rows(), 2);
        let mut builder = ColumnBlockBuilder::with_capacity(&column.data_type, 2, false);

        // append_block_range
        builder.append_block_range(&column_block, 0, 1).unwrap();
        builder.append_block_range(&column_block, 1, 1).unwrap();

        let column_block = builder.build();
        assert_eq!(column_block.num_rows(), 2);
        assert_eq!(
            column_block.datum(0),
            Datum::Varbinary(Bytes::copy_from_slice(b"binary key"))
        );
        assert_eq!(
            column_block.datum(1),
            Datum::Varbinary(Bytes::copy_from_slice(b"binary key1"))
        );
    }
}
