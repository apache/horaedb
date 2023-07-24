// Copyright 2023 CeresDB Project Authors. Licensed under Apache-2.0.

// Fork from https://github.com/influxdata/influxdb_iox/blob/7d878b21bd78cf7d0618804c1ccf8506521703bd/mutable_batch/src/column.rs.

//! A [`Column`] stores the rows for a given column name

use std::{fmt::Formatter, mem, sync::Arc};

use arrow::{
    array::{
        ArrayDataBuilder, ArrayRef, BinaryArray, BooleanArray, Float32Array, Float64Array,
        Int16Array, Int32Array, Int64Array, Int8Array, StringArray, TimestampNanosecondArray,
        UInt16Array, UInt32Array, UInt64Array, UInt8Array,
    },
    buffer::NullBuffer,
    datatypes::DataType,
    error::ArrowError,
};
use bytes_ext::Bytes;
use datafusion::parquet::data_type::AsBytes;
use snafu::{Backtrace, ResultExt, Snafu};

use crate::{
    bitset::BitSet,
    datum::{Datum, DatumKind},
    string::StringBytes,
    time::Timestamp,
};

#[derive(Debug, Snafu)]
#[allow(missing_copy_implementations, missing_docs)]
pub enum Error {
    #[snafu(display(
        "Invalid null mask, expected to be {} bytes but was {}",
        expected_bytes,
        actual_bytes
    ))]
    InvalidNullMask {
        expected_bytes: usize,
        actual_bytes: usize,
    },

    #[snafu(display("Internal MUB error constructing Arrow Array: {}", source))]
    CreatingArrowArray { source: ArrowError },

    #[snafu(display(
        "Data type conflict, expect:{:?}, given:{:?}.\nBacktrace:\n{}",
        expect,
        given,
        backtrace
    ))]
    ConflictType {
        expect: ColumnDataKind,
        given: DatumKind,
        backtrace: Backtrace,
    },

    #[snafu(display(
        "Column type conflict, expect:{:?}, given:{:?}.\nBacktrace:\n{}",
        expect,
        given,
        backtrace
    ))]
    ConflictColumnType {
        expect: ColumnDataKind,
        given: ColumnDataKind,
        backtrace: Backtrace,
    },

    #[snafu(display("Data type unsupported, kind:{:?}.\nBacktrace:\n{}", kind, backtrace))]
    UnsupportedType {
        kind: DatumKind,
        backtrace: Backtrace,
    },
}

/// A specialized `Error` for [`Column`] errors
pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Stores the actual data for columns in a chunk along with summary
/// statistics
#[derive(Debug, Clone)]
pub struct Column {
    pub(crate) datum_kind: DatumKind,
    pub(crate) valid: BitSet,
    pub(crate) data: ColumnData,
    pub(crate) to_insert: usize,
}

#[derive(Debug)]
pub enum ColumnDataKind {
    F64,
    F32,
    I64,
    I32,
    I16,
    I8,
    U64,
    U32,
    U16,
    U8,
    String,
    StringBytes,
    Varbinary,
    Bool,
}

/// The data for a column
#[derive(Debug, Clone)]
#[allow(missing_docs)]
pub enum ColumnData {
    F64(Vec<f64>),
    F32(Vec<f32>),
    I64(Vec<i64>),
    I32(Vec<i32>),
    I16(Vec<i16>),
    I8(Vec<i8>),
    U64(Vec<u64>),
    U32(Vec<u32>),
    U16(Vec<u16>),
    U8(Vec<u8>),
    String(Vec<String>),
    StringBytes(Vec<StringBytes>),
    Varbinary(Vec<Vec<u8>>),
    Bool(BitSet),
}

impl ColumnData {
    pub fn kind(&self) -> ColumnDataKind {
        match self {
            Self::F64(_) => ColumnDataKind::F64,
            Self::F32(_) => ColumnDataKind::F32,
            Self::I64(_) => ColumnDataKind::I64,
            Self::I32(_) => ColumnDataKind::I32,
            Self::I16(_) => ColumnDataKind::I16,
            Self::I8(_) => ColumnDataKind::I8,
            Self::U64(_) => ColumnDataKind::U64,
            Self::U32(_) => ColumnDataKind::U32,
            Self::U16(_) => ColumnDataKind::U16,
            Self::U8(_) => ColumnDataKind::U8,
            Self::String(_) => ColumnDataKind::String,
            Self::StringBytes(_) => ColumnDataKind::StringBytes,
            Self::Varbinary(_) => ColumnDataKind::Varbinary,
            Self::Bool(_) => ColumnDataKind::Bool,
        }
    }
}

impl std::fmt::Display for ColumnData {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::F64(col_data) => write!(f, "F64({})", col_data.len()),
            Self::F32(col_data) => write!(f, "F32({})", col_data.len()),
            Self::I64(col_data) => write!(f, "I64({})", col_data.len()),
            Self::I32(col_data) => write!(f, "I32({})", col_data.len()),
            Self::I16(col_data) => write!(f, "I16({})", col_data.len()),
            Self::I8(col_data) => write!(f, "I8({})", col_data.len()),
            Self::U64(col_data) => write!(f, "U64({})", col_data.len()),
            Self::U32(col_data) => write!(f, "U32({})", col_data.len()),
            Self::U16(col_data) => write!(f, "U16({})", col_data.len()),
            Self::U8(col_data) => write!(f, "U8({})", col_data.len()),
            Self::String(col_data) => write!(f, "String({})", col_data.len()),
            Self::StringBytes(col_data) => write!(f, "StringBytes({})", col_data.len()),
            Self::Varbinary(col_data) => write!(f, "Varbinary({})", col_data.len()),
            Self::Bool(col_data) => write!(f, "Bool({})", col_data.len()),
        }
    }
}

impl Column {
    pub fn new(row_count: usize, datum_kind: DatumKind) -> Result<Self> {
        let mut valid = BitSet::new();
        valid.append_unset(row_count);

        let data = match datum_kind {
            DatumKind::Boolean => {
                let mut data = BitSet::new();
                data.append_unset(row_count);
                ColumnData::Bool(data)
            }
            DatumKind::UInt64 => ColumnData::U64(vec![0; row_count]),
            DatumKind::UInt32 => ColumnData::U32(vec![0; row_count]),
            DatumKind::UInt16 => ColumnData::U16(vec![0; row_count]),
            DatumKind::UInt8 => ColumnData::U8(vec![0; row_count]),
            DatumKind::Double => ColumnData::F64(vec![0.0; row_count]),
            DatumKind::Float => ColumnData::F32(vec![0.0; row_count]),
            DatumKind::Int64 | DatumKind::Timestamp => ColumnData::I64(vec![0; row_count]),
            DatumKind::Int32 => ColumnData::I32(vec![0; row_count]),
            DatumKind::Int16 => ColumnData::I16(vec![0; row_count]),
            DatumKind::Int8 => ColumnData::I8(vec![0; row_count]),
            DatumKind::String => ColumnData::StringBytes(vec![StringBytes::new(); row_count]),
            DatumKind::Varbinary => ColumnData::Varbinary(vec![vec![]; row_count]),
            kind => {
                return UnsupportedType { kind }.fail();
            }
        };

        Ok(Self {
            datum_kind,
            valid,
            data,
            to_insert: 0,
        })
    }

    pub fn append_column(&mut self, mut column: Column) -> Result<()> {
        assert_eq!(self.datum_kind, column.datum_kind);
        self.valid.append_set(column.len());
        self.to_insert += column.len();
        match (&mut self.data, &mut column.data) {
            (ColumnData::F64(data), ColumnData::F64(ref mut column_data)) => {
                data.append(column_data)
            }
            (ColumnData::F32(data), ColumnData::F32(ref mut column_data)) => {
                data.append(column_data)
            }
            (ColumnData::I64(data), ColumnData::I64(ref mut column_data)) => {
                data.append(column_data)
            }
            (ColumnData::I32(data), ColumnData::I32(ref mut column_data)) => {
                data.append(column_data)
            }
            (ColumnData::I16(data), ColumnData::I16(ref mut column_data)) => {
                data.append(column_data)
            }
            (ColumnData::I8(data), ColumnData::I8(ref mut column_data)) => data.append(column_data),
            (ColumnData::U64(data), ColumnData::U64(ref mut column_data)) => {
                data.append(column_data)
            }
            (ColumnData::U32(data), ColumnData::U32(ref mut column_data)) => {
                data.append(column_data)
            }
            (ColumnData::U16(data), ColumnData::U16(ref mut column_data)) => {
                data.append(column_data)
            }
            (ColumnData::U8(data), ColumnData::U8(ref mut column_data)) => data.append(column_data),
            (ColumnData::String(data), ColumnData::String(ref mut column_data)) => {
                data.append(column_data)
            }
            (ColumnData::StringBytes(data), ColumnData::StringBytes(ref mut column_data)) => {
                data.append(column_data)
            }
            (ColumnData::Varbinary(data), ColumnData::Varbinary(ref mut column_data)) => {
                data.append(column_data)
            }
            (ColumnData::Bool(data), ColumnData::Bool(column_data)) => {
                data.extend_from(column_data)
            }
            (expect, given) => {
                return ConflictColumnType {
                    expect: expect.kind(),
                    given: given.kind(),
                }
                .fail()
            }
        }

        Ok(())
    }

    pub fn append_nulls(&mut self, count: usize) {
        self.to_insert += count;
    }

    pub fn append_datum_ref(&mut self, value: &Datum) -> Result<()> {
        match (&mut self.data, value) {
            (ColumnData::F64(data), Datum::Double(v)) => data[self.to_insert] = *v,
            (ColumnData::F32(data), Datum::Float(v)) => data[self.to_insert] = *v,
            (ColumnData::I64(data), Datum::Int64(v)) => data[self.to_insert] = *v,
            (ColumnData::I64(data), Datum::Timestamp(v)) => data[self.to_insert] = v.as_i64(),
            (ColumnData::I32(data), Datum::Int32(v)) => data[self.to_insert] = *v,
            (ColumnData::I16(data), Datum::Int16(v)) => data[self.to_insert] = *v,
            (ColumnData::I8(data), Datum::Int8(v)) => data[self.to_insert] = *v,
            (ColumnData::U64(data), Datum::UInt64(v)) => data[self.to_insert] = *v,
            (ColumnData::U32(data), Datum::UInt32(v)) => data[self.to_insert] = *v,
            (ColumnData::U16(data), Datum::UInt16(v)) => data[self.to_insert] = *v,
            (ColumnData::U8(data), Datum::UInt8(v)) => data[self.to_insert] = *v,
            (ColumnData::String(data), Datum::String(v)) => data[self.to_insert] = v.to_string(),
            (ColumnData::StringBytes(data), Datum::String(v)) => {
                data[self.to_insert] = StringBytes::from(v.as_str())
            }
            (ColumnData::Varbinary(data), Datum::Varbinary(v)) => {
                data[self.to_insert] = v.to_vec();
            }
            (ColumnData::Bool(data), Datum::Boolean(v)) => {
                if *v {
                    data.set(self.to_insert);
                }
            }

            (column_data, datum) => {
                return ConflictType {
                    expect: column_data.kind(),
                    given: datum.kind(),
                }
                .fail()
            }
        }
        self.valid.set(self.to_insert);
        self.to_insert += 1;
        Ok(())
    }

    pub fn get_datum(&self, idx: usize) -> Datum {
        if !self.valid.get(idx) {
            return Datum::Null;
        }
        match self.data {
            ColumnData::F64(ref data) => Datum::Double(data[idx]),
            ColumnData::F32(ref data) => Datum::Float(data[idx]),
            ColumnData::I64(ref data) => match self.datum_kind {
                DatumKind::Timestamp => Datum::Timestamp(Timestamp::from(data[idx])),
                DatumKind::Int64 => Datum::Int64(data[idx]),
                _ => unreachable!(),
            },
            ColumnData::I32(ref data) => Datum::Int32(data[idx]),
            ColumnData::I16(ref data) => Datum::Int16(data[idx]),
            ColumnData::I8(ref data) => Datum::Int8(data[idx]),
            ColumnData::U64(ref data) => Datum::UInt64(data[idx]),
            ColumnData::U32(ref data) => Datum::UInt32(data[idx]),
            ColumnData::U16(ref data) => Datum::UInt16(data[idx]),
            ColumnData::U8(ref data) => Datum::UInt8(data[idx]),
            ColumnData::String(ref data) => Datum::String(data[idx].clone().into()),
            ColumnData::StringBytes(ref data) => Datum::String(data[idx].clone()),
            ColumnData::Varbinary(ref data) => Datum::Varbinary(Bytes::from(data[idx].clone())),
            ColumnData::Bool(ref data) => Datum::Boolean(data.get(idx)),
        }
    }

    /// Returns the [`DatumKind`] of this column
    pub fn datum_kind(&self) -> DatumKind {
        self.datum_kind
    }

    /// Returns the validity bitmask of this column
    pub fn valid_mask(&self) -> &BitSet {
        &self.valid
    }

    /// Returns a reference to this column's data
    pub fn data(&self) -> &ColumnData {
        &self.data
    }

    /// Ensures that the total length of this column is `len` rows,
    /// padding it with trailing NULLs if necessary
    #[allow(dead_code)]
    pub(crate) fn push_nulls_to_len(&mut self, len: usize) {
        if self.valid.len() == len {
            return;
        }
        assert!(len > self.valid.len(), "cannot shrink column");
        let delta = len - self.valid.len();
        self.valid.append_unset(delta);

        match &mut self.data {
            ColumnData::F64(data) => {
                data.resize(len, 0.);
            }
            ColumnData::F32(data) => {
                data.resize(len, 0.);
            }
            ColumnData::I64(data) => {
                data.resize(len, 0);
            }
            ColumnData::I32(data) => {
                data.resize(len, 0);
            }
            ColumnData::I16(data) => {
                data.resize(len, 0);
            }
            ColumnData::I8(data) => {
                data.resize(len, 0);
            }
            ColumnData::U64(data) => {
                data.resize(len, 0);
            }
            ColumnData::U32(data) => {
                data.resize(len, 0);
            }
            ColumnData::U16(data) => {
                data.resize(len, 0);
            }
            ColumnData::U8(data) => {
                data.resize(len, 0);
            }
            ColumnData::String(data) => {
                data.resize(len, "".to_string());
            }
            ColumnData::Varbinary(data) => {
                data.resize(len, vec![]);
            }
            ColumnData::Bool(data) => {
                data.append_unset(delta);
            }
            ColumnData::StringBytes(data) => {
                data.resize(len, StringBytes::new());
            }
        }
    }

    /// Returns the number of rows in this column
    pub fn len(&self) -> usize {
        self.valid.len()
    }

    /// Returns true if this column contains no rows
    pub fn is_empty(&self) -> bool {
        self.valid.is_empty()
    }

    /// The approximate memory size of the data in the column.
    ///
    /// This includes the size of `self`.
    pub fn size(&self) -> usize {
        let data_size = match &self.data {
            ColumnData::F64(v) => mem::size_of::<f64>() * v.capacity(),
            ColumnData::F32(v) => mem::size_of::<f32>() * v.capacity(),
            ColumnData::I64(v) => mem::size_of::<i64>() * v.capacity(),
            ColumnData::I32(v) => mem::size_of::<i32>() * v.capacity(),
            ColumnData::I16(v) => mem::size_of::<i16>() * v.capacity(),
            ColumnData::I8(v) => mem::size_of::<i8>() * v.capacity(),
            ColumnData::U64(v) => mem::size_of::<u64>() * v.capacity(),
            ColumnData::U32(v) => mem::size_of::<u32>() * v.capacity(),
            ColumnData::U16(v) => mem::size_of::<u16>() * v.capacity(),
            ColumnData::U8(v) => mem::size_of::<u8>() * v.capacity(),
            ColumnData::Bool(v) => v.byte_len(),
            ColumnData::String(v) => {
                v.iter().map(|s| s.len()).sum::<usize>()
                    + (v.capacity() - v.len()) * mem::size_of::<String>()
            }
            ColumnData::StringBytes(v) => {
                v.iter().map(|s| s.len()).sum::<usize>()
                    + (v.capacity() - v.len()) * mem::size_of::<StringBytes>()
            }
            ColumnData::Varbinary(v) => {
                v.iter().map(|s| s.len()).sum::<usize>() + (v.capacity() - v.len())
            }
        };
        mem::size_of::<Self>() + data_size + self.valid.byte_len()
    }

    /// The approximate memory size of the data in the column, not counting for
    /// stats or self or whatever extra space has been allocated for the
    /// vecs
    pub fn size_data(&self) -> usize {
        match &self.data {
            ColumnData::F64(_) => mem::size_of::<f64>() * self.len(),
            ColumnData::F32(_) => mem::size_of::<f32>() * self.len(),
            ColumnData::I64(_) => mem::size_of::<i64>() * self.len(),
            ColumnData::I32(_) => mem::size_of::<i32>() * self.len(),
            ColumnData::I16(_) => mem::size_of::<i16>() * self.len(),
            ColumnData::I8(_) => mem::size_of::<i8>() * self.len(),
            ColumnData::U64(_) => mem::size_of::<u64>() * self.len(),
            ColumnData::U32(_) => mem::size_of::<u32>() * self.len(),
            ColumnData::U16(_) => mem::size_of::<u16>() * self.len(),
            ColumnData::U8(_) => mem::size_of::<u8>() * self.len(),
            ColumnData::Bool(_) => mem::size_of::<bool>() * self.len(),
            ColumnData::String(v) => v.iter().map(|s| s.len()).sum(),
            ColumnData::StringBytes(v) => v.iter().map(|s| s.len()).sum(),
            ColumnData::Varbinary(v) => v.iter().map(|s| s.len()).sum(),
        }
    }

    /// Converts this column to an arrow [`ArrayRef`]
    pub fn to_arrow(&self) -> Result<ArrayRef> {
        let nulls = Some(NullBuffer::new(self.valid.to_arrow()));

        let data: ArrayRef = match &self.data {
            ColumnData::F64(data) => {
                let data = ArrayDataBuilder::new(DataType::Float64)
                    .len(data.len())
                    .add_buffer(data.iter().cloned().collect())
                    .nulls(nulls)
                    .build()
                    .context(CreatingArrowArray)?;
                Arc::new(Float64Array::from(data))
            }
            ColumnData::F32(data) => {
                let data = ArrayDataBuilder::new(DataType::Float32)
                    .len(data.len())
                    .add_buffer(data.iter().cloned().collect())
                    .nulls(nulls)
                    .build()
                    .context(CreatingArrowArray)?;
                Arc::new(Float32Array::from(data))
            }
            ColumnData::I64(data) => match self.datum_kind {
                DatumKind::Timestamp => {
                    let data = ArrayDataBuilder::new(DatumKind::Timestamp.to_arrow_data_type())
                        .len(data.len())
                        .add_buffer(data.iter().cloned().collect())
                        .nulls(nulls)
                        .build()
                        .context(CreatingArrowArray)?;
                    Arc::new(TimestampNanosecondArray::from(data))
                }

                DatumKind::Int64 => {
                    let data = ArrayDataBuilder::new(DataType::Int64)
                        .len(data.len())
                        .add_buffer(data.iter().cloned().collect())
                        .nulls(nulls)
                        .build()
                        .context(CreatingArrowArray)?;
                    Arc::new(Int64Array::from(data))
                }
                _ => unreachable!(),
            },
            ColumnData::I32(data) => {
                let data = ArrayDataBuilder::new(DataType::Int32)
                    .len(data.len())
                    .add_buffer(data.iter().cloned().collect())
                    .nulls(nulls)
                    .build()
                    .context(CreatingArrowArray)?;
                Arc::new(Int32Array::from(data))
            }
            ColumnData::I16(data) => {
                let data = ArrayDataBuilder::new(DataType::Int16)
                    .len(data.len())
                    .add_buffer(data.iter().cloned().collect())
                    .nulls(nulls)
                    .build()
                    .context(CreatingArrowArray)?;
                Arc::new(Int16Array::from(data))
            }
            ColumnData::I8(data) => {
                let data = ArrayDataBuilder::new(DataType::Int8)
                    .len(data.len())
                    .add_buffer(data.iter().cloned().collect())
                    .nulls(nulls)
                    .build()
                    .context(CreatingArrowArray)?;
                Arc::new(Int8Array::from(data))
            }
            ColumnData::U64(data) => {
                let data = ArrayDataBuilder::new(DataType::UInt64)
                    .len(data.len())
                    .add_buffer(data.iter().cloned().collect())
                    .nulls(nulls)
                    .build()
                    .context(CreatingArrowArray)?;
                Arc::new(UInt64Array::from(data))
            }
            ColumnData::U32(data) => {
                let data = ArrayDataBuilder::new(DataType::UInt32)
                    .len(data.len())
                    .add_buffer(data.iter().cloned().collect())
                    .nulls(nulls)
                    .build()
                    .context(CreatingArrowArray)?;
                Arc::new(UInt32Array::from(data))
            }
            ColumnData::U16(data) => {
                let data = ArrayDataBuilder::new(DataType::UInt16)
                    .len(data.len())
                    .add_buffer(data.iter().cloned().collect())
                    .nulls(nulls)
                    .build()
                    .context(CreatingArrowArray)?;
                Arc::new(UInt16Array::from(data))
            }
            ColumnData::U8(data) => {
                let data = ArrayDataBuilder::new(DataType::UInt8)
                    .len(data.len())
                    .add_buffer(data.iter().cloned().collect())
                    .nulls(nulls)
                    .build()
                    .context(CreatingArrowArray)?;
                Arc::new(UInt8Array::from(data))
            }
            ColumnData::String(data) => {
                let data =
                    StringArray::from(data.iter().map(|s| Some(s.as_str())).collect::<Vec<_>>());
                Arc::new(data)
            }
            ColumnData::StringBytes(data) => {
                let data =
                    StringArray::from(data.iter().map(|s| Some(s.as_str())).collect::<Vec<_>>());
                Arc::new(data)
            }
            ColumnData::Varbinary(data) => {
                let data =
                    BinaryArray::from(data.iter().map(|s| Some(s.as_bytes())).collect::<Vec<_>>());
                Arc::new(data)
            }
            ColumnData::Bool(data) => {
                let data = ArrayDataBuilder::new(DataType::Boolean)
                    .len(data.len())
                    .add_buffer(data.to_arrow().into_inner())
                    .nulls(nulls)
                    .build()
                    .context(CreatingArrowArray)?;
                Arc::new(BooleanArray::from(data))
            }
        };

        assert_eq!(data.len(), self.len());

        Ok(data)
    }
}
