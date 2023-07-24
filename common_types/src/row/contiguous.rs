// Copyright 2022-2023 CeresDB Project Authors. Licensed under Apache-2.0.

//! Contiguous row.

use std::{
    convert::TryInto,
    debug_assert_eq, fmt, mem,
    ops::{Deref, DerefMut},
    str,
};

use prost::encoding::{decode_varint, encode_varint, encoded_len_varint};
use snafu::{ensure, Backtrace, Snafu};

use crate::{
    datum::{Datum, DatumKind, DatumView},
    projected_schema::RowProjector,
    row::{
        bitset::{BitSet, RoBitSet},
        Row,
    },
    schema::{IndexInWriterSchema, Schema},
    time::Timestamp,
};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display(
        "String is too long to encode into row (max is {MAX_STRING_LEN}), len:{len}.\nBacktrace:\n{backtrace}",
    ))]
    StringTooLong { len: usize, backtrace: Backtrace },

    #[snafu(display(
        "Row is too long to encode(max is {MAX_ROW_LEN}), len:{len}.\nBacktrace:\n{backtrace}",
    ))]
    RowTooLong { len: usize, backtrace: Backtrace },

    #[snafu(display("Number of null columns is missing.\nBacktrace:\n{backtrace}"))]
    NumNullColsMissing { backtrace: Backtrace },

    #[snafu(display("The raw bytes of bit set is invalid, expect_len:{expect_len}, give_len:{given_len}.\nBacktrace:\n{backtrace}"))]
    InvalidBitSetBytes {
        expect_len: usize,
        given_len: usize,
        backtrace: Backtrace,
    },
}

pub type Result<T> = std::result::Result<T, Error>;

/// Offset used in row's encoding
type Offset = u32;

/// Max allowed string length of datum to store in a contiguous row (16 MB).
const MAX_STRING_LEN: usize = 1024 * 1024 * 16;
/// Max allowed length of total bytes in a contiguous row (1 GB).
const MAX_ROW_LEN: usize = 1024 * 1024 * 1024;

/// Row encoded in a contiguous buffer.
pub trait ContiguousRow {
    /// Returns the number of datums.
    fn num_datum_views(&self) -> usize;

    /// Returns [DatumView] of column in given index.
    ///
    /// Panic if index or buffer is out of bound.
    fn datum_view_at(&self, index: usize, datum_kind: &DatumKind) -> DatumView;
}

/// Here is the layout of the encoded continuous row:
/// ```plaintext
/// +------------------+-----------------+-------------------------+-------------------------+
/// | num_bits(u32)    |  nulls_bit_set  | datum encoding block... | var-len payload block   |
/// +------------------+-----------------+-------------------------+-------------------------+
/// ```
/// The first block is the number of bits of the `nulls_bit_set`, which is used
/// to rebuild the bit set. The `nulls_bit_set` is used to record which columns
/// are null. With the bitset, any null column won't be encoded in the following
/// datum encoding block.
///
/// And if `num_bits` is equal to zero, it will still take 4B while the
/// `nulls_bit_set` block will be ignored.
///
/// As for the datum encoding block, most type shares the similar pattern:
/// ```plaintext
/// +----------------+
/// | payload/offset |
/// +----------------+
/// ```
/// If the type has a fixed size, here will be the data payload.
/// Otherwise, a offset in the var-len payload block pointing the real payload.
struct Encoding;

impl Encoding {
    const fn size_of_offset() -> usize {
        mem::size_of::<Offset>()
    }

    const fn size_of_num_bits() -> usize {
        mem::size_of::<u32>()
    }
}

pub enum ContiguousRowReader<'a, T> {
    NoNulls(ContiguousRowReaderNoNulls<'a, T>),
    WithNulls(ContiguousRowReaderWithNulls<'a, T>),
}

pub struct ContiguousRowReaderNoNulls<'a, T> {
    inner: &'a T,
    byte_offsets: &'a [usize],
    datum_offset: usize,
}

pub struct ContiguousRowReaderWithNulls<'a, T> {
    buf: &'a T,
    byte_offsets: Vec<isize>,
    datum_offset: usize,
}

impl<'a, T: Deref<Target = [u8]>> ContiguousRowReader<'a, T> {
    pub fn try_new(buf: &'a T, schema: &'a Schema) -> Result<Self> {
        let byte_offsets = schema.byte_offsets();
        ensure!(
            buf.len() >= Encoding::size_of_num_bits(),
            NumNullColsMissing
        );
        let num_bits =
            u32::from_ne_bytes(buf[0..Encoding::size_of_num_bits()].try_into().unwrap()) as usize;
        if num_bits > 0 {
            ContiguousRowReaderWithNulls::try_new(buf, schema, num_bits).map(Self::WithNulls)
        } else {
            let reader = ContiguousRowReaderNoNulls {
                inner: buf,
                byte_offsets,
                datum_offset: Encoding::size_of_num_bits(),
            };
            Ok(Self::NoNulls(reader))
        }
    }
}

impl<'a, T: Deref<Target = [u8]>> ContiguousRow for ContiguousRowReader<'a, T> {
    fn num_datum_views(&self) -> usize {
        match self {
            Self::NoNulls(v) => v.num_datum_views(),
            Self::WithNulls(v) => v.num_datum_views(),
        }
    }

    fn datum_view_at(&self, index: usize, datum_kind: &DatumKind) -> DatumView {
        match self {
            Self::NoNulls(v) => v.datum_view_at(index, datum_kind),
            Self::WithNulls(v) => v.datum_view_at(index, datum_kind),
        }
    }
}

impl<'a, T: Deref<Target = [u8]>> ContiguousRowReaderWithNulls<'a, T> {
    fn try_new(buf: &'a T, schema: &'a Schema, num_bits: usize) -> Result<Self> {
        assert!(num_bits > 0);

        let bit_set_size = BitSet::num_bytes(num_bits);
        let bit_set_buf = &buf[Encoding::size_of_num_bits()..];
        ensure!(
            bit_set_buf.len() >= bit_set_size,
            InvalidBitSetBytes {
                expect_len: bit_set_size,
                given_len: bit_set_buf.len()
            }
        );

        let nulls_bit_set = RoBitSet::try_new(&bit_set_buf[..bit_set_size], num_bits).unwrap();

        let mut fixed_byte_offsets = Vec::with_capacity(schema.num_columns());
        let mut acc_null_bytes = 0;
        for (index, expect_offset) in schema.byte_offsets().iter().enumerate() {
            match nulls_bit_set.is_set(index) {
                Some(true) => fixed_byte_offsets.push((*expect_offset - acc_null_bytes) as isize),
                Some(false) => {
                    fixed_byte_offsets.push(-1);
                    acc_null_bytes += byte_size_of_datum(&schema.column(index).data_type);
                }
                None => fixed_byte_offsets.push(-1),
            }
        }

        Ok(Self {
            buf,
            byte_offsets: fixed_byte_offsets,
            datum_offset: Encoding::size_of_num_bits() + bit_set_size,
        })
    }
}

impl<'a, T: Deref<Target = [u8]>> ContiguousRow for ContiguousRowReaderWithNulls<'a, T> {
    fn num_datum_views(&self) -> usize {
        self.byte_offsets.len()
    }

    fn datum_view_at(&self, index: usize, datum_kind: &DatumKind) -> DatumView<'a> {
        let offset = self.byte_offsets[index];
        if offset < 0 {
            DatumView::Null
        } else {
            let datum_offset = self.datum_offset + offset as usize;
            let datum_buf = &self.buf[datum_offset..];
            datum_view_at(datum_buf, self.buf, datum_kind)
        }
    }
}

impl<'a, T: Deref<Target = [u8]>> ContiguousRow for ContiguousRowReaderNoNulls<'a, T> {
    fn num_datum_views(&self) -> usize {
        self.byte_offsets.len()
    }

    fn datum_view_at(&self, index: usize, datum_kind: &DatumKind) -> DatumView<'a> {
        let offset = self.byte_offsets[index];
        let datum_buf = &self.inner[self.datum_offset + offset..];
        datum_view_at(datum_buf, self.inner, datum_kind)
    }
}

fn datum_view_at<'a>(
    datum_buf: &'a [u8],
    string_buf: &'a [u8],
    datum_kind: &DatumKind,
) -> DatumView<'a> {
    must_read_view(datum_kind, datum_buf, string_buf)
}

/// Contiguous row with projection information.
///
/// The caller must ensure the source schema of projector is the same as the
/// schema of source row.
pub struct ProjectedContiguousRow<'a, T> {
    source_row: T,
    projector: &'a RowProjector,
}

impl<'a, T: ContiguousRow> ProjectedContiguousRow<'a, T> {
    pub fn new(source_row: T, projector: &'a RowProjector) -> Self {
        Self {
            source_row,
            projector,
        }
    }

    pub fn num_datum_views(&self) -> usize {
        self.projector.source_projection().len()
    }

    pub fn datum_view_at(&self, index: usize) -> DatumView {
        let p = self.projector.source_projection()[index];

        match p {
            Some(index_in_source) => {
                let datum_kind = self.projector.datum_kind(index_in_source);
                self.source_row.datum_view_at(index_in_source, datum_kind)
            }
            None => DatumView::Null,
        }
    }
}

impl<'a, T: ContiguousRow> fmt::Debug for ProjectedContiguousRow<'a, T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut list = f.debug_list();
        for i in 0..self.num_datum_views() {
            let view = self.datum_view_at(i);
            list.entry(&view);
        }
        list.finish()
    }
}

/// In memory buffer to hold data of a contiguous row.
pub trait RowBuffer: DerefMut<Target = [u8]> {
    /// Clear and resize the buffer size to `new_len` with given `value`.
    fn reset(&mut self, new_len: usize, value: u8);

    /// Append slice into the buffer, resize the buffer automatically.
    fn append_slice(&mut self, src: &[u8]);
}

/// A writer to build a contiguous row.
pub struct ContiguousRowWriter<'a, T> {
    inner: &'a mut T,
    /// The schema the row group need to be encoded into, the schema
    /// of the row need to be write compatible for the table schema.
    table_schema: &'a Schema,
    /// The index mapping from table schema to column in the
    /// schema of row group.
    index_in_writer: &'a IndexInWriterSchema,
}

// TODO(yingwen): Try to replace usage of row by contiguous row.
impl<'a, T: RowBuffer + 'a> ContiguousRowWriter<'a, T> {
    pub fn new(
        inner: &'a mut T,
        table_schema: &'a Schema,
        index_in_writer: &'a IndexInWriterSchema,
    ) -> Self {
        Self {
            inner,
            table_schema,
            index_in_writer,
        }
    }

    fn write_datum(
        inner: &mut T,
        datum: &Datum,
        offset: &mut usize,
        next_string_offset: &mut usize,
    ) -> Result<()> {
        match datum {
            // Already filled by null, nothing to do.
            Datum::Null => {}
            Datum::Timestamp(v) => {
                let value_buf = v.as_i64().to_ne_bytes();
                Self::write_slice_to_offset(inner, offset, &value_buf);
            }
            Datum::Double(v) => {
                let value_buf = v.to_ne_bytes();
                Self::write_slice_to_offset(inner, offset, &value_buf);
            }
            Datum::Float(v) => {
                let value_buf = v.to_ne_bytes();
                Self::write_slice_to_offset(inner, offset, &value_buf);
            }
            Datum::Varbinary(v) => {
                ensure!(
                    *next_string_offset <= MAX_ROW_LEN,
                    StringTooLong {
                        len: *next_string_offset
                    }
                );
                // Encode the string offset as a u32.
                let value_buf = (*next_string_offset as u32).to_ne_bytes();
                Self::write_slice_to_offset(inner, offset, &value_buf);

                // Encode length of string as a varint.
                ensure!(v.len() <= MAX_STRING_LEN, StringTooLong { len: v.len() });
                let mut buf = [0; 4];
                let value_buf = Self::encode_varint(v.len() as u32, &mut buf);
                Self::write_slice_to_offset(inner, next_string_offset, value_buf);
                Self::write_slice_to_offset(inner, next_string_offset, v);
            }
            Datum::String(v) => {
                ensure!(
                    *next_string_offset <= MAX_ROW_LEN,
                    StringTooLong {
                        len: *next_string_offset
                    }
                );
                // Encode the string offset as a u32.
                let value_buf = (*next_string_offset as u32).to_ne_bytes();
                Self::write_slice_to_offset(inner, offset, &value_buf);

                // Encode length of string as a varint.
                ensure!(v.len() <= MAX_STRING_LEN, StringTooLong { len: v.len() });
                let mut buf = [0; 4];
                let value_buf = Self::encode_varint(v.len() as u32, &mut buf);
                Self::write_slice_to_offset(inner, next_string_offset, value_buf);
                Self::write_slice_to_offset(inner, next_string_offset, v.as_bytes());
            }
            Datum::UInt64(v) => {
                let value_buf = v.to_ne_bytes();
                Self::write_slice_to_offset(inner, offset, &value_buf);
            }
            Datum::UInt32(v) => {
                let value_buf = v.to_ne_bytes();
                Self::write_slice_to_offset(inner, offset, &value_buf);
            }
            Datum::UInt16(v) => {
                let value_buf = v.to_ne_bytes();
                Self::write_slice_to_offset(inner, offset, &value_buf);
            }
            Datum::UInt8(v) => {
                Self::write_slice_to_offset(inner, offset, &[*v]);
            }
            Datum::Int64(v) => {
                let value_buf = v.to_ne_bytes();
                Self::write_slice_to_offset(inner, offset, &value_buf);
            }
            Datum::Int32(v) => {
                let value_buf = v.to_ne_bytes();
                Self::write_slice_to_offset(inner, offset, &value_buf);
            }
            Datum::Int16(v) => {
                let value_buf = v.to_ne_bytes();
                Self::write_slice_to_offset(inner, offset, &value_buf);
            }
            Datum::Int8(v) => {
                Self::write_slice_to_offset(inner, offset, &[*v as u8]);
            }
            Datum::Boolean(v) => {
                Self::write_slice_to_offset(inner, offset, &[*v as u8]);
            }
            Datum::Date(v) => {
                let value_buf = v.to_ne_bytes();
                Self::write_slice_to_offset(inner, offset, &value_buf);
            }
            Datum::Time(v) => {
                let value_buf = v.to_ne_bytes();
                Self::write_slice_to_offset(inner, offset, &value_buf);
            }
        }

        Ok(())
    }

    /// Write a row to the buffer, the buffer will be reset first.
    pub fn write_row(&mut self, row: &Row) -> Result<()> {
        let mut num_null_cols = 0;
        for index_in_table in 0..self.table_schema.num_columns() {
            if let Some(writer_index) = self.index_in_writer.column_index_in_writer(index_in_table)
            {
                let datum = &row[writer_index];
                if datum.is_null() {
                    num_null_cols += 1;
                }
            } else {
                num_null_cols += 1;
            }
        }

        if num_null_cols > 0 {
            self.write_row_with_nulls(row)
        } else {
            self.write_row_without_nulls(row)
        }
    }

    fn write_row_with_nulls(&mut self, row: &Row) -> Result<()> {
        let mut encoded_len = 0;
        let mut num_bytes_of_variable_col = 0;
        for index_in_table in 0..self.table_schema.num_columns() {
            if let Some(writer_index) = self.index_in_writer.column_index_in_writer(index_in_table)
            {
                let datum = &row[writer_index];
                // No need to store null column.
                if !datum.is_null() {
                    encoded_len += byte_size_of_datum(&datum.kind());
                }

                if !datum.is_fixed_sized() {
                    // For the datum content and the length of it
                    let len = datum.size();
                    let size = len + encoded_len_varint(len as u64);
                    num_bytes_of_variable_col += size;
                    encoded_len += size;
                }
            } else {
                // No need to store null column.
            }
        }

        let num_bits = self.table_schema.num_columns();
        // Assume most columns are not null, so use a bitset with all bit set at first.
        let mut nulls_bit_set = BitSet::all_set(num_bits);
        // The flag for the BitSet, denoting the number of the columns.
        encoded_len += Encoding::size_of_num_bits() + nulls_bit_set.as_bytes().len();

        // Pre-allocate the memory.
        self.inner.reset(encoded_len, 0);
        let mut next_string_offset = encoded_len - num_bytes_of_variable_col;
        let mut datum_offset = Encoding::size_of_num_bits() + nulls_bit_set.as_bytes().len();
        for index_in_table in 0..self.table_schema.num_columns() {
            if let Some(writer_index) = self.index_in_writer.column_index_in_writer(index_in_table)
            {
                let datum = &row[writer_index];
                // Write datum bytes to the buffer.
                Self::write_datum(
                    self.inner,
                    datum,
                    &mut datum_offset,
                    &mut next_string_offset,
                )?;

                if datum.is_null() {
                    nulls_bit_set.unset(index_in_table);
                }
            } else {
                // This column should be treated as null.
                nulls_bit_set.unset(index_in_table);
            }
        }

        // Storing the number of null columns as u32 is enough.
        Self::write_slice_to_offset(self.inner, &mut 0, &(num_bits as u32).to_ne_bytes());
        Self::write_slice_to_offset(
            self.inner,
            &mut Encoding::size_of_num_bits(),
            nulls_bit_set.as_bytes(),
        );

        debug_assert_eq!(datum_offset, encoded_len - num_bytes_of_variable_col);
        debug_assert_eq!(next_string_offset, encoded_len);

        Ok(())
    }

    fn write_row_without_nulls(&mut self, row: &Row) -> Result<()> {
        let datum_buffer_len =
            self.table_schema.string_buffer_offset() + Encoding::size_of_num_bits();
        let mut encoded_len = datum_buffer_len;
        for index_in_table in 0..self.table_schema.num_columns() {
            if let Some(writer_index) = self.index_in_writer.column_index_in_writer(index_in_table)
            {
                let datum = &row[writer_index];
                if !datum.is_fixed_sized() {
                    // For the datum content and the length of it
                    let len = datum.size();
                    encoded_len += encoded_len_varint(len as u64) + len;
                }
            } else {
                unreachable!("The column is ensured to be non-null");
            }
        }

        // Pre-allocate memory for row.
        self.inner.reset(encoded_len, DatumKind::Null.into_u8());

        // Offset to next string in string buffer.
        let mut next_string_offset = datum_buffer_len;
        let mut datum_offset = Encoding::size_of_num_bits();
        for index_in_table in 0..self.table_schema.num_columns() {
            if let Some(writer_index) = self.index_in_writer.column_index_in_writer(index_in_table)
            {
                let datum = &row[writer_index];
                // Write datum bytes to the buffer.
                Self::write_datum(
                    self.inner,
                    datum,
                    &mut datum_offset,
                    &mut next_string_offset,
                )?;
            } else {
                unreachable!("The column is ensured to be non-null");
            }
        }

        debug_assert_eq!(datum_offset, datum_buffer_len);
        debug_assert_eq!(next_string_offset, encoded_len);
        Ok(())
    }

    #[inline]
    fn write_slice_to_offset(inner: &mut T, offset: &mut usize, value_buf: &[u8]) {
        let dst = &mut inner[*offset..*offset + value_buf.len()];
        dst.copy_from_slice(value_buf);
        *offset += value_buf.len();
    }

    fn encode_varint(value: u32, buf: &mut [u8; 4]) -> &[u8] {
        let value = value as u64;
        let mut temp = &mut buf[..];
        encode_varint(value, &mut temp);
        &buf[..encoded_len_varint(value)]
    }
}

/// The byte size to encode the datum of this kind in memory.
///
/// Returns the datum size for header. For integer types, the datum
/// size is the memory size of the integer type. For string types, the
/// datum size is the memory size to hold the offset.
pub(crate) fn byte_size_of_datum(kind: &DatumKind) -> usize {
    match kind {
        DatumKind::Null => 1,
        DatumKind::Timestamp => mem::size_of::<Timestamp>(),
        DatumKind::Double => mem::size_of::<f64>(),
        DatumKind::Float => mem::size_of::<f32>(),
        // The size of offset.
        DatumKind::Varbinary | DatumKind::String => Encoding::size_of_offset(),
        DatumKind::UInt64 => mem::size_of::<u64>(),
        DatumKind::UInt32 => mem::size_of::<u32>(),
        DatumKind::UInt16 => mem::size_of::<u16>(),
        DatumKind::UInt8 => mem::size_of::<u8>(),
        DatumKind::Int64 => mem::size_of::<i64>(),
        DatumKind::Int32 => mem::size_of::<i32>(),
        DatumKind::Int16 => mem::size_of::<i16>(),
        DatumKind::Int8 => mem::size_of::<i8>(),
        DatumKind::Boolean => mem::size_of::<bool>(),
        DatumKind::Date => mem::size_of::<i32>(),
        DatumKind::Time => mem::size_of::<i64>(),
    }
}

/// Read datum view from given datum buf, and may reference the string in
/// `string_buf`.
///
/// Panic if out of bound.
///
/// ## Safety
/// The string in buffer must be valid utf8.
fn must_read_view<'a>(
    datum_kind: &DatumKind,
    datum_buf: &'a [u8],
    string_buf: &'a [u8],
) -> DatumView<'a> {
    match datum_kind {
        DatumKind::Null => DatumView::Null,
        DatumKind::Timestamp => {
            let value_buf = datum_buf[..mem::size_of::<i64>()].try_into().unwrap();
            let ts = Timestamp::new(i64::from_ne_bytes(value_buf));
            DatumView::Timestamp(ts)
        }
        DatumKind::Double => {
            let value_buf = datum_buf[..mem::size_of::<f64>()].try_into().unwrap();
            let v = f64::from_ne_bytes(value_buf);
            DatumView::Double(v)
        }
        DatumKind::Float => {
            let value_buf = datum_buf[..mem::size_of::<f32>()].try_into().unwrap();
            let v = f32::from_ne_bytes(value_buf);
            DatumView::Float(v)
        }
        DatumKind::Varbinary => {
            let bytes = must_read_bytes(datum_buf, string_buf);
            DatumView::Varbinary(bytes)
        }
        DatumKind::String => {
            let bytes = must_read_bytes(datum_buf, string_buf);
            let v = unsafe { str::from_utf8_unchecked(bytes) };
            DatumView::String(v)
        }
        DatumKind::UInt64 => {
            let value_buf = datum_buf[..mem::size_of::<u64>()].try_into().unwrap();
            let v = u64::from_ne_bytes(value_buf);
            DatumView::UInt64(v)
        }
        DatumKind::UInt32 => {
            let value_buf = datum_buf[..mem::size_of::<u32>()].try_into().unwrap();
            let v = u32::from_ne_bytes(value_buf);
            DatumView::UInt32(v)
        }
        DatumKind::UInt16 => {
            let value_buf = datum_buf[..mem::size_of::<u16>()].try_into().unwrap();
            let v = u16::from_ne_bytes(value_buf);
            DatumView::UInt16(v)
        }
        DatumKind::UInt8 => DatumView::UInt8(datum_buf[0]),
        DatumKind::Int64 => {
            let value_buf = datum_buf[..mem::size_of::<i64>()].try_into().unwrap();
            let v = i64::from_ne_bytes(value_buf);
            DatumView::Int64(v)
        }
        DatumKind::Int32 => {
            let value_buf = datum_buf[..mem::size_of::<i32>()].try_into().unwrap();
            let v = i32::from_ne_bytes(value_buf);
            DatumView::Int32(v)
        }
        DatumKind::Int16 => {
            let value_buf = datum_buf[..mem::size_of::<i16>()].try_into().unwrap();
            let v = i16::from_ne_bytes(value_buf);
            DatumView::Int16(v)
        }
        DatumKind::Int8 => DatumView::Int8(datum_buf[0] as i8),
        DatumKind::Boolean => DatumView::Boolean(datum_buf[0] != 0),
        DatumKind::Date => {
            let value_buf = datum_buf[..mem::size_of::<i32>()].try_into().unwrap();
            let v = i32::from_ne_bytes(value_buf);
            DatumView::Date(v)
        }
        DatumKind::Time => {
            let value_buf = datum_buf[..mem::size_of::<i64>()].try_into().unwrap();
            let v = i64::from_ne_bytes(value_buf);
            DatumView::Time(v)
        }
    }
}

fn must_read_bytes<'a>(datum_buf: &'a [u8], string_buf: &'a [u8]) -> &'a [u8] {
    // Read offset of string in string buf.
    let value_buf = datum_buf[..mem::size_of::<Offset>()].try_into().unwrap();
    let offset = Offset::from_ne_bytes(value_buf) as usize;
    let mut string_buf = &string_buf[offset..];

    // Read len of the string.
    let string_len = match decode_varint(&mut string_buf) {
        Ok(len) => len as usize,
        Err(e) => panic!("failed to decode string length, string buffer:{string_buf:?}, err:{e}"),
    };

    // Read string.
    &string_buf[..string_len]
}

impl RowBuffer for Vec<u8> {
    fn reset(&mut self, new_len: usize, value: u8) {
        self.clear();

        self.resize(new_len, value);
    }

    fn append_slice(&mut self, src: &[u8]) {
        self.extend_from_slice(src);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        projected_schema::ProjectedSchema,
        tests::{build_rows, build_schema},
    };

    #[test]
    fn test_contiguous_read_write() {
        let schema = build_schema();
        let rows = build_rows();
        let index_in_writer = IndexInWriterSchema::for_same_schema(schema.num_columns());
        let datum_kinds = schema
            .columns()
            .iter()
            .map(|column| &column.data_type)
            .collect::<Vec<_>>();

        let mut buf = Vec::new();
        for row in rows {
            let mut writer = ContiguousRowWriter::new(&mut buf, &schema, &index_in_writer);

            writer.write_row(&row).unwrap();

            let reader = ContiguousRowReader::try_new(&buf, &schema).unwrap();

            let range: Vec<_> = (0..reader.num_datum_views()).collect();
            for i in range {
                let datum = &row[i];
                let view = reader.datum_view_at(i, datum_kinds[i]);

                assert_eq!(datum.as_view(), view);
            }
        }
    }

    #[test]
    fn test_contiguous_read_write_with_different_write_schema() {
        let schema = build_schema();
        let rows = build_rows();
        let index_in_writer = {
            let mut index_schema = IndexInWriterSchema::default();
            index_schema.reserve_columns(schema.num_columns());
            // Make the final column is None.
            for i in 0..schema.num_columns() {
                let col_idx = (i != schema.num_columns() - 1).then_some(i);
                index_schema.push_column(col_idx);
            }
            index_schema
        };

        let datum_kinds = schema
            .columns()
            .iter()
            .map(|column| &column.data_type)
            .collect::<Vec<_>>();

        let mut buf = Vec::new();
        for row in rows {
            let mut writer = ContiguousRowWriter::new(&mut buf, &schema, &index_in_writer);

            writer.write_row(&row).unwrap();

            let reader = ContiguousRowReader::try_new(&buf, &schema).unwrap();

            let final_col_idx = reader.num_datum_views() - 1;
            let range: Vec<_> = (0..=final_col_idx).collect();
            for i in range {
                let datum = &row[i];
                let view = reader.datum_view_at(i, datum_kinds[i]);
                if i == final_col_idx {
                    assert!(matches!(view, DatumView::Null));
                } else {
                    assert_eq!(datum.as_view(), view);
                }
            }
        }
    }

    #[test]
    fn test_project_contiguous_read_write() {
        let schema = build_schema();
        assert!(schema.num_columns() > 1);
        let projection: Vec<usize> = (0..schema.num_columns() - 1).collect();
        let projected_schema =
            ProjectedSchema::new(schema.clone(), Some(projection.clone())).unwrap();
        let row_projected_schema = projected_schema.try_project_with_key(&schema).unwrap();
        let rows = build_rows();
        let index_in_writer = IndexInWriterSchema::for_same_schema(schema.num_columns());

        let mut buf = Vec::new();
        for row in rows {
            let mut writer = ContiguousRowWriter::new(&mut buf, &schema, &index_in_writer);

            writer.write_row(&row).unwrap();

            let source_row = ContiguousRowReader::try_new(&buf, &schema).unwrap();
            let projected_row = ProjectedContiguousRow::new(source_row, &row_projected_schema);

            let range = projection.clone();
            for i in range {
                let datum = &row[i];
                let view = projected_row.datum_view_at(i);

                assert_eq!(datum.as_view(), view);
            }
        }
    }
}
