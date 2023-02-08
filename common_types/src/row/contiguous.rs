// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

//! Contiguous row.

use std::{
    convert::{TryFrom, TryInto},
    fmt, mem,
    ops::{Deref, DerefMut},
    str,
};

use snafu::{ensure, Backtrace, Snafu};

use crate::{
    datum::{Datum, DatumKind, DatumView},
    projected_schema::RowProjector,
    row::Row,
    schema::{IndexInWriterSchema, Schema},
    time::Timestamp,
};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display(
        "String is too long to encode into row (max is {}), len:{}.\nBacktrace:\n{}",
        MAX_STRING_LEN,
        len,
        backtrace
    ))]
    StringTooLong { len: usize, backtrace: Backtrace },
}

pub type Result<T> = std::result::Result<T, Error>;

/// Size to store the offset of string buffer.
type OffsetSize = usize;

/// Max allowed string length of datum to store in a contiguous row (16 MB).
const MAX_STRING_LEN: usize = 1024 * 1024 * 16;

/// Row encoded in a contiguous buffer.
pub trait ContiguousRow {
    /// Returns the number of datums.
    fn num_datum_views(&self) -> usize;

    /// Returns [DatumView] of column in given index, and returns null if the
    /// datum kind is unknown.
    ///
    /// Panic if index or buffer is out of bound.
    fn datum_view_at(&self, index: usize) -> DatumView;
}

pub struct ContiguousRowReader<'a, T> {
    inner: &'a T,
    byte_offsets: &'a [usize],
    string_buffer_offset: usize,
}

impl<'a, T> ContiguousRowReader<'a, T> {
    pub fn with_schema(inner: &'a T, schema: &'a Schema) -> Self {
        Self {
            inner,
            byte_offsets: schema.byte_offsets(),
            string_buffer_offset: schema.string_buffer_offset(),
        }
    }
}

impl<'a, T: Deref<Target = [u8]>> ContiguousRow for ContiguousRowReader<'a, T> {
    fn num_datum_views(&self) -> usize {
        self.byte_offsets.len()
    }

    fn datum_view_at(&self, index: usize) -> DatumView<'a> {
        let offset = self.byte_offsets[index];
        let buf = &self.inner[offset..];

        // Get datum kind, if the datum kind is unknown, returns null.
        let datum_kind = match DatumKind::try_from(buf[0]) {
            Ok(v) => v,
            Err(_) => return DatumView::Null,
        };

        // Advance 1 byte to skip the header byte.
        let datum_buf = &buf[1..];
        // If no string column in this schema, the string buffer offset should
        // equal to the buffer len, and string buf is an empty slice.
        let string_buf = &self.inner[self.string_buffer_offset..];

        must_read_view(&datum_kind, datum_buf, string_buf)
    }
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
}

impl<'a, T: ContiguousRow> ContiguousRow for ProjectedContiguousRow<'a, T> {
    fn num_datum_views(&self) -> usize {
        self.projector.source_projection().len()
    }

    fn datum_view_at(&self, index: usize) -> DatumView {
        let p = self.projector.source_projection()[index];

        match p {
            Some(index_in_source) => self.source_row.datum_view_at(index_in_source),
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
        byte_offset: usize,
        next_string_offset: &mut usize,
    ) -> Result<()> {
        let datum_offset = byte_offset + 1;

        match datum {
            // Already filled by null, nothing to do.
            Datum::Null => (),
            Datum::Timestamp(v) => {
                Self::write_byte_to_offset(inner, byte_offset, DatumKind::Timestamp.into_u8());
                let value_buf = v.as_i64().to_ne_bytes();
                Self::write_slice_to_offset(inner, datum_offset, &value_buf);
            }
            Datum::Double(v) => {
                Self::write_byte_to_offset(inner, byte_offset, DatumKind::Double.into_u8());
                let value_buf = v.to_ne_bytes();
                Self::write_slice_to_offset(inner, datum_offset, &value_buf);
            }
            Datum::Float(v) => {
                Self::write_byte_to_offset(inner, byte_offset, DatumKind::Float.into_u8());
                let value_buf = v.to_ne_bytes();
                Self::write_slice_to_offset(inner, datum_offset, &value_buf);
            }
            Datum::Varbinary(v) => {
                Self::write_byte_to_offset(inner, byte_offset, DatumKind::Varbinary.into_u8());
                let value_buf = next_string_offset.to_ne_bytes();
                Self::write_slice_to_offset(inner, datum_offset, &value_buf);
                // Use u32 to store length of string.
                *next_string_offset += mem::size_of::<u32>() + v.len();

                ensure!(v.len() <= MAX_STRING_LEN, StringTooLong { len: v.len() });

                let string_len = v.len() as u32;
                inner.append_slice(&string_len.to_ne_bytes());
                inner.append_slice(v);
            }
            Datum::String(v) => {
                Self::write_byte_to_offset(inner, byte_offset, DatumKind::String.into_u8());
                let value_buf = next_string_offset.to_ne_bytes();
                Self::write_slice_to_offset(inner, datum_offset, &value_buf);
                // Use u32 to store length of string.
                *next_string_offset += mem::size_of::<u32>() + v.len();

                ensure!(v.len() <= MAX_STRING_LEN, StringTooLong { len: v.len() });

                let string_len = v.len() as u32;
                inner.append_slice(&string_len.to_ne_bytes());
                inner.append_slice(v.as_bytes());
            }
            Datum::UInt64(v) => {
                Self::write_byte_to_offset(inner, byte_offset, DatumKind::UInt64.into_u8());
                let value_buf = v.to_ne_bytes();
                Self::write_slice_to_offset(inner, datum_offset, &value_buf);
            }
            Datum::UInt32(v) => {
                Self::write_byte_to_offset(inner, byte_offset, DatumKind::UInt32.into_u8());
                let value_buf = v.to_ne_bytes();
                Self::write_slice_to_offset(inner, datum_offset, &value_buf);
            }
            Datum::UInt16(v) => {
                Self::write_byte_to_offset(inner, byte_offset, DatumKind::UInt16.into_u8());
                let value_buf = v.to_ne_bytes();
                Self::write_slice_to_offset(inner, datum_offset, &value_buf);
            }
            Datum::UInt8(v) => {
                Self::write_byte_to_offset(inner, byte_offset, DatumKind::UInt8.into_u8());
                Self::write_slice_to_offset(inner, datum_offset, &[*v]);
            }
            Datum::Int64(v) => {
                Self::write_byte_to_offset(inner, byte_offset, DatumKind::Int64.into_u8());
                let value_buf = v.to_ne_bytes();
                Self::write_slice_to_offset(inner, datum_offset, &value_buf);
            }
            Datum::Int32(v) => {
                Self::write_byte_to_offset(inner, byte_offset, DatumKind::Int32.into_u8());
                let value_buf = v.to_ne_bytes();
                Self::write_slice_to_offset(inner, datum_offset, &value_buf);
            }
            Datum::Int16(v) => {
                Self::write_byte_to_offset(inner, byte_offset, DatumKind::Int16.into_u8());
                let value_buf = v.to_ne_bytes();
                Self::write_slice_to_offset(inner, datum_offset, &value_buf);
            }
            Datum::Int8(v) => {
                Self::write_byte_to_offset(inner, byte_offset, DatumKind::Int8.into_u8());
                Self::write_slice_to_offset(inner, datum_offset, &[*v as u8]);
            }
            Datum::Boolean(v) => {
                Self::write_byte_to_offset(inner, byte_offset, DatumKind::Boolean.into_u8());
                Self::write_slice_to_offset(inner, datum_offset, &[*v as u8]);
            }
        }

        Ok(())
    }

    /// Write a row to the buffer, the buffer will be reset first.
    pub fn write_row(&mut self, row: &Row) -> Result<()> {
        let datum_buffer_len = self.table_schema.string_buffer_offset();
        // Reset the buffer and fill the buffer by null, now new slice will be
        // appended to the string buffer.
        self.inner
            .reset(datum_buffer_len, DatumKind::Null.into_u8());

        assert_eq!(row.num_columns(), self.table_schema.num_columns());

        // Offset to next string in string buffer.
        let mut next_string_offset: OffsetSize = 0;
        for index_in_table in 0..self.table_schema.num_columns() {
            if let Some(writer_index) = self.index_in_writer.column_index_in_writer(index_in_table)
            {
                let datum = &row[writer_index];
                let byte_offset = self.table_schema.byte_offset(index_in_table);

                // Write datum bytes to the buffer.
                Self::write_datum(self.inner, datum, byte_offset, &mut next_string_offset)?;
            }
            // Column not in row is already filled by null.
        }

        Ok(())
    }

    #[inline]
    fn write_byte_to_offset(inner: &mut T, offset: usize, value: u8) {
        inner[offset] = value;
    }

    #[inline]
    fn write_slice_to_offset(inner: &mut T, offset: usize, value_buf: &[u8]) {
        let dst = &mut inner[offset..offset + value_buf.len()];
        dst.copy_from_slice(value_buf);
    }
}

/// The byte size to encode the datum of this kind in memory.
///
/// Returns the (datum size + 1) for header. For integer types, the datum
/// size is the memory size of the interger type. For string types, the
/// datum size is the memory size to hold the offset.
pub(crate) fn byte_size_of_datum(kind: &DatumKind) -> usize {
    let datum_size = match kind {
        DatumKind::Null => 1,
        DatumKind::Timestamp => mem::size_of::<Timestamp>(),
        DatumKind::Double => mem::size_of::<f64>(),
        DatumKind::Float => mem::size_of::<f32>(),
        // The size of offset.
        DatumKind::Varbinary | DatumKind::String => mem::size_of::<OffsetSize>(),
        DatumKind::UInt64 => mem::size_of::<u64>(),
        DatumKind::UInt32 => mem::size_of::<u32>(),
        DatumKind::UInt16 => mem::size_of::<u16>(),
        DatumKind::UInt8 => mem::size_of::<u8>(),
        DatumKind::Int64 => mem::size_of::<i64>(),
        DatumKind::Int32 => mem::size_of::<i32>(),
        DatumKind::Int16 => mem::size_of::<i16>(),
        DatumKind::Int8 => mem::size_of::<i8>(),
        DatumKind::Boolean => mem::size_of::<bool>(),
        DatumKind::Date => mem::size_of::<u32>(),
    };

    datum_size + 1
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
            let value_buf = datum_buf[..mem::size_of::<u32>()].try_into().unwrap();
            let v = i32::from_ne_bytes(value_buf);
            DatumView::Date(v)
        },
    }
}

fn must_read_bytes<'a>(datum_buf: &'a [u8], string_buf: &'a [u8]) -> &'a [u8] {
    // Read offset of string in string buf.
    let value_buf = datum_buf[..mem::size_of::<OffsetSize>()]
        .try_into()
        .unwrap();
    let offset = OffsetSize::from_ne_bytes(value_buf);
    let string_buf = &string_buf[offset..];

    // Read len of the string.
    let len_buf = string_buf[..mem::size_of::<u32>()].try_into().unwrap();
    let string_len = u32::from_ne_bytes(len_buf) as usize;
    let string_buf = &string_buf[mem::size_of::<u32>()..];

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

    fn check_contiguous_row(row: &Row, reader: impl ContiguousRow, projection: Option<Vec<usize>>) {
        let range = if let Some(projection) = projection {
            projection
        } else {
            (0..reader.num_datum_views()).collect()
        };
        for i in range {
            let datum = &row[i];
            let view = reader.datum_view_at(i);

            assert_eq!(datum.as_view(), view);
        }
    }

    #[test]
    fn test_contiguous_read_write() {
        let schema = build_schema();
        let rows = build_rows();
        let index_in_writer = IndexInWriterSchema::for_same_schema(schema.num_columns());

        let mut buf = Vec::new();
        for row in rows {
            let mut writer = ContiguousRowWriter::new(&mut buf, &schema, &index_in_writer);

            writer.write_row(&row).unwrap();

            let reader = ContiguousRowReader::with_schema(&buf, &schema);
            check_contiguous_row(&row, reader, None);
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

            let source_row = ContiguousRowReader::with_schema(&buf, &schema);
            let projected_row = ProjectedContiguousRow::new(source_row, &row_projected_schema);
            check_contiguous_row(&row, projected_row, Some(projection.clone()));
        }
    }
}
