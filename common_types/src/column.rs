// Copyright 2023 CeresDB Project Authors. Licensed under Apache-2.0.

// Fork from https://github.com/influxdata/influxdb_iox/blob/7d878b21bd78cf7d0618804c1ccf8506521703bd/mutable_batch/src/column.rs.

//! A [`Column`] stores the rows for a given column name

use std::{fmt::Formatter, mem};

use arrow::error::ArrowError;
use bytes_ext::Bytes;
use snafu::{Backtrace, Snafu};

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

    #[snafu(display("Data type conflict, msg:{:?}.\nBacktrace:\n{}", msg, backtrace))]
    ConflictType { msg: String, backtrace: Backtrace },

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
    String(Vec<StringBytes>),
    Varbinary(Vec<Vec<u8>>),
    Bool(BitSet),
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
            Self::String(col_data) => write!(f, "StringBytes({})", col_data.len()),
            Self::Varbinary(col_data) => write!(f, "Varbinary({})", col_data.len()),
            Self::Bool(col_data) => write!(f, "Bool({})", col_data.len()),
        }
    }
}

impl Column {
    pub fn with_capacity(row_count: usize, datum_kind: DatumKind) -> Result<Self> {
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
            DatumKind::String => ColumnData::String(vec![StringBytes::new(); row_count]),
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
            (ColumnData::Varbinary(data), ColumnData::Varbinary(ref mut column_data)) => {
                data.append(column_data)
            }
            (ColumnData::Bool(data), ColumnData::Bool(column_data)) => {
                data.extend_from(column_data)
            }
            (expect, given) => {
                return ConflictType {
                    msg: format!("column data type expect:{expect:?}, but given:{given:?}"),
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
            (ColumnData::String(data), Datum::String(v)) => {
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
                    msg: format!(
                        "column data type:{:?} but got datum type:{}",
                        column_data,
                        datum.kind()
                    ),
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
        match &self.data {
            ColumnData::F64(data) => Datum::Double(data[idx]),
            ColumnData::F32(data) => Datum::Float(data[idx]),
            ColumnData::I64(data) => match self.datum_kind {
                DatumKind::Timestamp => Datum::Timestamp(Timestamp::from(data[idx])),
                DatumKind::Int64 => Datum::Int64(data[idx]),
                _ => unreachable!(),
            },
            ColumnData::I32(data) => Datum::Int32(data[idx]),
            ColumnData::I16(data) => Datum::Int16(data[idx]),
            ColumnData::I8(data) => Datum::Int8(data[idx]),
            ColumnData::U64(data) => Datum::UInt64(data[idx]),
            ColumnData::U32(data) => Datum::UInt32(data[idx]),
            ColumnData::U16(data) => Datum::UInt16(data[idx]),
            ColumnData::U8(data) => Datum::UInt8(data[idx]),
            ColumnData::String(data) => Datum::String(data[idx].clone()),
            ColumnData::Varbinary(data) => Datum::Varbinary(Bytes::from(data[idx].clone())),
            ColumnData::Bool(data) => Datum::Boolean(data.get(idx)),
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
            ColumnData::Varbinary(data) => {
                data.resize(len, vec![]);
            }
            ColumnData::Bool(data) => {
                data.append_unset(delta);
            }
            ColumnData::String(data) => {
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
            ColumnData::Varbinary(v) => v.iter().map(|s| s.len()).sum(),
        }
    }
}
