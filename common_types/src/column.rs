// Copyright 2023 CeresDB Project Authors. Licensed under Apache-2.0.

// Fork from https://github.com/influxdata/influxdb_iox/blob/7d878b21bd78cf7d0618804c1ccf8506521703bd/mutable_batch/src/column.rs.

//! A [`Column`] stores the rows for a given column name

use std::{fmt::Formatter, mem, slice::Iter, sync::Arc};

use arrow::{
    array::{
        ArrayDataBuilder, ArrayRef, BinaryArray, BooleanArray, Float64Array, Int64Array,
        StringArray, TimestampNanosecondArray, UInt64Array,
    },
    buffer::NullBuffer,
    datatypes::DataType,
    error::ArrowError,
};
use bytes_ext::{BufMut, Bytes, BytesMut};
use ceresdbproto::storage::value;
use datafusion::parquet::data_type::AsBytes;
use snafu::{ResultExt, Snafu};
use sqlparser::ast::Value;

use crate::{
    bitset::BitSet,
    datum::{Datum, DatumKind},
    string::StringBytes,
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

impl IntoIterator for Column {
    type IntoIter = ColumnDataIter;
    type Item = value::Value;

    fn into_iter(self) -> Self::IntoIter {
        self.data.into_iter()
    }
}

/// The data for a column
#[derive(Debug, Clone)]
#[allow(missing_docs)]
pub enum ColumnData {
    F64(Vec<f64>),
    I64(Vec<i64>),
    U64(Vec<u64>),
    String(Vec<String>),
    StringBytes(Vec<StringBytes>),
    Varbinary(Vec<Vec<u8>>),
    Bool(BitSet),
}

impl std::fmt::Display for ColumnData {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::F64(col_data) => write!(f, "F64({})", col_data.len()),
            Self::I64(col_data) => write!(f, "I64({})", col_data.len()),
            Self::U64(col_data) => write!(f, "U64({})", col_data.len()),
            Self::String(col_data) => write!(f, "String({})", col_data.len()),
            Self::Varbinary(col_data) => write!(f, "Varbinary({})", col_data.len()),
            Self::Bool(col_data) => write!(f, "Bool({})", col_data.len()),
            _ => todo!(),
        }
    }
}

pub enum ColumnDataIter {
    F64(std::vec::IntoIter<f64>),
    I64(std::vec::IntoIter<i64>),
    U64(std::vec::IntoIter<u64>),
    String(std::vec::IntoIter<String>),
    StringBytes(std::vec::IntoIter<StringBytes>),
    Varbinary(std::vec::IntoIter<Vec<u8>>),
    Bool(std::vec::IntoIter<bool>),
}

impl<'a> Iterator for ColumnDataIter {
    type Item = value::Value;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Self::F64(col_data) => col_data.next().map(|x| value::Value::Float64Value(x)),
            Self::I64(col_data) => col_data.next().map(|x| value::Value::Int64Value(x)),
            Self::U64(col_data) => col_data.next().map(|x| value::Value::Uint64Value(x)),
            Self::String(col_data) => col_data.next().map(|x| value::Value::StringValue(x)),
            Self::StringBytes(col_data) => col_data
                .next()
                .map(|x| value::Value::StringValue(x.to_string())),
            Self::Varbinary(col_data) => col_data.next().map(|x| value::Value::VarbinaryValue(x)),
            Self::Bool(col_data) => col_data.next().map(|x| value::Value::BoolValue(x)),
        }
    }
}

impl IntoIterator for ColumnData {
    type IntoIter = ColumnDataIter;
    type Item = value::Value;

    fn into_iter(self) -> Self::IntoIter {
        match self {
            Self::F64(col_data) => ColumnDataIter::F64(col_data.into_iter()),
            Self::I64(col_data) => ColumnDataIter::I64(col_data.into_iter()),
            Self::U64(col_data) => ColumnDataIter::U64(col_data.into_iter()),
            Self::String(col_data) => ColumnDataIter::String(col_data.into_iter()),
            Self::StringBytes(col_data) => ColumnDataIter::StringBytes(col_data.into_iter()),
            Self::Varbinary(col_data) => ColumnDataIter::Varbinary(col_data.into_iter()),
            Self::Bool(col_data) => todo!(),
        }
    }
}

impl Column {
    #[allow(dead_code)]
    pub fn new(row_count: usize, datum_kind: DatumKind) -> Self {
        let mut valid = BitSet::new();
        valid.append_unset(row_count);

        let data = match datum_kind {
            DatumKind::Boolean => {
                let mut data = BitSet::new();
                data.append_unset(row_count);
                ColumnData::Bool(data)
            }
            DatumKind::UInt64 => ColumnData::U64(vec![0; row_count]),
            DatumKind::Double => ColumnData::F64(vec![0.0; row_count]),
            DatumKind::Int64 | DatumKind::Timestamp => ColumnData::I64(vec![0; row_count]),
            // DatumKind::String => ColumnData::String(vec!["".to_string(); row_count]),
            DatumKind::String => ColumnData::StringBytes(vec![StringBytes::new(); row_count]),
            DatumKind::Varbinary => ColumnData::Varbinary(vec![vec![]; row_count]),
            _ => todo!(),
        };

        Self {
            datum_kind,
            valid,
            data,
            to_insert: 0,
        }
    }

    pub fn append_column(&mut self, mut column: Column) {
        assert_eq!(self.datum_kind, column.datum_kind);
        self.valid.append_set(column.len());
        self.to_insert += column.len();
        match (&mut self.data, &mut column.data) {
            (ColumnData::F64(data), ColumnData::F64(ref mut column_data)) => {
                data.append(column_data)
            }
            (ColumnData::I64(data), ColumnData::I64(ref mut column_data)) => {
                data.append(column_data)
            }
            (ColumnData::U64(data), ColumnData::U64(ref mut column_data)) => {
                data.append(column_data)
            }
            (ColumnData::String(data), ColumnData::String(ref mut column_data)) => {
                data.append(column_data)
            }
            (ColumnData::StringBytes(data), ColumnData::StringBytes(ref mut column_data)) => {
                data.append(column_data)
            }
            (ColumnData::Varbinary(data), ColumnData::Varbinary(ref mut column_data)) => {
                data.append(column_data)
            }
            (ColumnData::Bool(data), ColumnData::Bool(ref mut column_data)) => todo!(),
            _ => todo!(),
        }
    }

    pub fn append_nulls(&mut self, count: usize) {
        self.valid.append_unset(count);
    }

    pub fn append_datum_ref(&mut self, value: &Datum) -> Result<()> {
        match (&mut self.data, value) {
            (ColumnData::F64(data), Datum::Double(v)) => data[self.to_insert] = *v,
            (ColumnData::I64(data), Datum::Int64(v)) => data[self.to_insert] = *v,
            (ColumnData::I64(data), Datum::Timestamp(v)) => data[self.to_insert] = v.as_i64(),
            (ColumnData::U64(data), Datum::UInt64(v)) => data[self.to_insert] = *v,
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

            (c, v) => println!("c: {:?}, v: {:?}", c, v),
        }
        self.valid.set(self.to_insert);
        self.to_insert += 1;
        Ok(())
    }

    pub fn get_datum(&self, idx: usize) -> Datum {
        match self.data {
            ColumnData::F64(ref data) => Datum::Double(data[idx]),
            ColumnData::I64(ref data) => Datum::Int64(data[idx]),
            ColumnData::U64(ref data) => Datum::UInt64(data[idx]),
            ColumnData::String(ref data) => Datum::String(data[idx].clone().into()),
            ColumnData::StringBytes(ref data) => Datum::String(data[idx].clone()),
            ColumnData::Varbinary(ref data) => Datum::Varbinary(Bytes::from(data[idx].clone())),
            ColumnData::Bool(ref data) => Datum::Boolean(data.get(idx)),
        }
    }

    pub fn get_value(&self, idx: usize) -> value::Value {
        match self.data {
            ColumnData::F64(ref data) => value::Value::Float64Value(data[idx]),
            ColumnData::I64(ref data) => value::Value::Int64Value(data[idx]),
            ColumnData::U64(ref data) => value::Value::Uint64Value(data[idx]),
            ColumnData::String(ref data) => value::Value::StringValue(data[idx].clone().into()),
            ColumnData::StringBytes(ref data) => value::Value::StringValue(data[idx].to_string()),
            ColumnData::Varbinary(ref data) => value::Value::VarbinaryValue(data[idx].clone()),
            ColumnData::Bool(ref data) => value::Value::BoolValue(data.get(idx)),
        }
    }

    pub fn get_bytes(&self, idx: usize, buf: &mut BytesMut) {
        match self.data {
            ColumnData::F64(ref data) => buf.put_slice(data[idx].to_le_bytes().as_slice()),
            ColumnData::I64(ref data) => buf.put_slice(data[idx].to_le_bytes().as_slice()),
            ColumnData::U64(ref data) => buf.put_slice(data[idx].to_le_bytes().as_slice()),
            ColumnData::String(ref data) => buf.put_slice(data[idx].as_bytes()),
            ColumnData::StringBytes(ref data) => buf.put_slice(data[idx].as_bytes()),
            ColumnData::Varbinary(ref data) => buf.put_slice(data[idx].as_slice()),
            ColumnData::Bool(ref data) => todo!(),
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
            ColumnData::I64(data) => {
                data.resize(len, 0);
            }
            ColumnData::U64(data) => {
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
            _ => todo!(),
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
            ColumnData::I64(v) => mem::size_of::<i64>() * v.capacity(),
            ColumnData::U64(v) => mem::size_of::<u64>() * v.capacity(),
            ColumnData::Bool(v) => v.byte_len(),
            ColumnData::String(v) => {
                v.iter().map(|s| s.len()).sum::<usize>()
                    + (v.capacity() - v.len()) * mem::size_of::<String>()
            }
            ColumnData::Varbinary(v) => {
                v.iter().map(|s| s.len()).sum::<usize>() + (v.capacity() - v.len())
            }
            _ => todo!(),
        };
        mem::size_of::<Self>() + data_size + self.valid.byte_len()
    }

    /// The approximate memory size of the data in the column, not counting for
    /// stats or self or whatever extra space has been allocated for the
    /// vecs
    pub fn size_data(&self) -> usize {
        match &self.data {
            ColumnData::F64(_) => mem::size_of::<f64>() * self.len(),
            ColumnData::I64(_) => mem::size_of::<i64>() * self.len(),
            ColumnData::U64(_) => mem::size_of::<u64>() * self.len(),
            ColumnData::Bool(_) => mem::size_of::<bool>() * self.len(),
            ColumnData::String(v) => v.iter().map(|s| s.len()).sum(),
            ColumnData::Varbinary(v) => v.iter().map(|s| s.len()).sum(),
            _ => todo!(),
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
            ColumnData::U64(data) => {
                let data = ArrayDataBuilder::new(DataType::UInt64)
                    .len(data.len())
                    .add_buffer(data.iter().cloned().collect())
                    .nulls(nulls)
                    .build()
                    .context(CreatingArrowArray)?;
                Arc::new(UInt64Array::from(data))
            }
            ColumnData::String(data) => {
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
            _ => todo!(),
        };

        assert_eq!(data.len(), self.len());

        Ok(data)
    }
}
