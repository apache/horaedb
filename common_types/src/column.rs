// Copyright 2023 CeresDB Project Authors. Licensed under Apache-2.0.

// Fork from https://github.com/influxdata/influxdb_iox/blob/7d878b21bd78cf7d0618804c1ccf8506521703bd/mutable_batch/src/column.rs.

//! A [`Column`] stores the rows for a given column name

use std::{fmt::Formatter, mem, sync::Arc};

use arrow::{
    array::{
        ArrayDataBuilder, ArrayRef, BooleanArray, Float64Array, Int64Array, StringArray,
        TimestampNanosecondArray, UInt64Array,
    },
    buffer::NullBuffer,
    datatypes::DataType,
    error::ArrowError,
};
use snafu::{ResultExt, Snafu};

use crate::{bitset::BitSet, datum::DatumKind};

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
}

/// The data for a column
#[derive(Debug, Clone)]
#[allow(missing_docs)]
pub enum ColumnData {
    F64(Vec<f64>),
    I64(Vec<i64>),
    U64(Vec<u64>),
    String(Vec<String>),
    Bool(BitSet),
}

impl std::fmt::Display for ColumnData {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::F64(col_data) => write!(f, "F64({})", col_data.len()),
            Self::I64(col_data) => write!(f, "I64({})", col_data.len()),
            Self::U64(col_data) => write!(f, "U64({})", col_data.len()),
            Self::String(col_data) => write!(f, "String({})", col_data.len()),
            Self::Bool(col_data) => write!(f, "Bool({})", col_data.len()),
        }
    }
}

impl Column {
    #[allow(dead_code)]
    pub(crate) fn new(row_count: usize, datum_kind: DatumKind) -> Self {
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
            DatumKind::String => ColumnData::String(vec!["".to_string(); row_count]),
            _ => todo!(),
        };

        Self {
            datum_kind,
            valid,
            data,
        }
    }

    /// Returns the [`DatumKind`] of this column
    pub fn datum_kine(&self) -> DatumKind {
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
            ColumnData::Bool(data) => {
                data.append_unset(delta);
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
            ColumnData::I64(v) => mem::size_of::<i64>() * v.capacity(),
            ColumnData::U64(v) => mem::size_of::<u64>() * v.capacity(),
            ColumnData::Bool(v) => v.byte_len(),
            ColumnData::String(v) => {
                v.iter().map(|s| s.len()).sum::<usize>()
                    + (v.capacity() - v.len()) * mem::size_of::<String>()
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
            ColumnData::I64(_) => mem::size_of::<i64>() * self.len(),
            ColumnData::U64(_) => mem::size_of::<u64>() * self.len(),
            ColumnData::Bool(_) => mem::size_of::<bool>() * self.len(),
            ColumnData::String(v) => v.iter().map(|s| s.len()).sum(),
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
