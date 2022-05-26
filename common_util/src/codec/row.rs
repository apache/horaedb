// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

//! Row encoding utils
//!
//! Notice: The encoding method is used both in wal and memtable. Be careful for
//! data compatibility

use std::convert::TryFrom;

use common_types::{
    bytes::{BufMut, ByteVec, BytesMut, MemBuf, MemBufMut},
    datum::Datum,
    row::{Row, RowGroup},
    schema::{IndexInWriterSchema, Schema},
};
use snafu::{ResultExt, Snafu};

use crate::codec::{
    compact::{MemCompactDecoder, MemCompactEncoder},
    DecodeTo, Decoder, Encoder,
};

#[derive(Debug, Snafu)]
#[snafu(visibility(pub(crate)))]
pub enum Error {
    #[snafu(display("Failed to encode row datum, err:{}", source))]
    EncodeRowDatum {
        source: crate::codec::compact::Error,
    },

    #[snafu(display("Failed to decode row datum, err:{}", source))]
    DecodeRowDatum {
        source: crate::codec::compact::Error,
    },
}

define_result!(Error);

/// Compact row encoder for wal.
struct WalRowEncoder<'a> {
    /// Schema of table
    table_schema: &'a Schema,
    /// Index of table column in writer
    index_in_writer: &'a IndexInWriterSchema,
}

impl<'a> Encoder<Row> for WalRowEncoder<'a> {
    type Error = Error;

    fn encode<B: MemBufMut>(&self, buf: &mut B, value: &Row) -> Result<()> {
        let encoder = MemCompactEncoder;
        for index_in_table in 0..self.table_schema.num_columns() {
            match self.index_in_writer.column_index_in_writer(index_in_table) {
                Some(writer_index) => {
                    // Column in writer
                    encoder
                        .encode(buf, &value[writer_index])
                        .context(EncodeRowDatum)?;
                }
                None => {
                    // Column not in writer
                    encoder.encode(buf, &Datum::Null).context(EncodeRowDatum)?;
                }
            }
        }

        Ok(())
    }

    fn estimate_encoded_size(&self, value: &Row) -> usize {
        let encoder = MemCompactEncoder;
        let mut total_len = 0;
        for index_in_table in 0..self.table_schema.num_columns() {
            match self.index_in_writer.column_index_in_writer(index_in_table) {
                Some(writer_index) => {
                    // Column in writer
                    total_len += encoder.estimate_encoded_size(&value[writer_index]);
                }
                None => {
                    // Column not in writer
                    total_len += encoder.estimate_encoded_size(&Datum::Null);
                }
            }
        }

        total_len
    }
}

/// Compact row decoder for wal, supports projection.
#[derive(Debug)]
pub struct WalRowDecoder<'a> {
    /// Schema of row to decode
    schema: &'a Schema,
}

impl<'a> WalRowDecoder<'a> {
    /// Create a decoder with given `schema`, the caller should ensure the
    /// schema matches the row to be decoded.
    pub fn new(schema: &'a Schema) -> Self {
        Self { schema }
    }
}

impl<'a> Decoder<Row> for WalRowDecoder<'a> {
    type Error = Error;

    fn decode<B: MemBuf>(&self, buf: &mut B) -> Result<Row> {
        let num_columns = self.schema.num_columns();
        let mut datums = Vec::with_capacity(num_columns);

        for idx in 0..num_columns {
            let column_schema = &self.schema.column(idx);
            let datum_kind = &column_schema.data_type;
            let decoder = MemCompactDecoder;

            // Decode each column
            let mut datum = Datum::empty(datum_kind);
            decoder.decode_to(buf, &mut datum).context(DecodeRowDatum)?;

            datums.push(datum);
        }

        Ok(Row::from_datums(datums))
    }
}

/// Encode the row group in the format that can write to wal.
///
/// Arguments
/// - row_group: The rows to be encoded and wrote to.
/// - table_schema: The schema the row group need to be encoded into, the schema
///   of the row group need to be write compatible for the table schema.
/// - index_in_writer: The index mapping from table schema to column in the
///   schema of row group.
/// - encoded_rows: The Vec to store bytes of each encoded row.
pub fn encode_row_group_for_wal(
    row_group: &RowGroup,
    table_schema: &Schema,
    index_in_writer: &IndexInWriterSchema,
    encoded_rows: &mut Vec<ByteVec>,
) -> Result<()> {
    let row_encoder = WalRowEncoder {
        table_schema,
        index_in_writer,
    };

    // Use estimated size of first row to avoid compute all
    let row_estimated_size = match row_group.get_row(0) {
        Some(first_row) => row_encoder.estimate_encoded_size(first_row),
        // The row group is empty
        None => return Ok(()),
    };

    encoded_rows.reserve(row_group.num_rows());

    // Each row is constructed in writer schema, we need to encode it in
    // `table_schema`
    for row in row_group {
        let mut buf = Vec::with_capacity(row_estimated_size);
        row_encoder.encode(&mut buf, row)?;

        encoded_rows.push(buf);
    }

    Ok(())
}

/// Return the next prefix key
///
/// Assume there are keys like:
///
/// ```text
/// rowkey1
/// rowkey1_column1
/// rowkey1_column2
/// rowKey2
/// ```
///
/// If we seek 'rowkey1' Next, we will get 'rowkey1_column1'.
/// If we seek 'rowkey1' PrefixNext, we will get 'rowkey2'.
///
/// Ported from <https://github.com/pingcap/tidb/blob/f81ef5579551a0523d18b049eb25ab3375bcfb48/kv/key.go#L49>
///
/// REQUIRE: The key should be memory comparable
// TODO(yingwen): Maybe add scratch param
// TODO(yingwen): Move to another mod
pub fn key_prefix_next(key: &[u8]) -> BytesMut {
    let mut buf = BytesMut::from(key);
    // isize should be enough to represent the key len
    let mut idx = isize::try_from(key.len() - 1).unwrap();
    while idx >= 0 {
        let i = idx as usize;
        buf[i] += 1;
        if buf[i] != 0 {
            break;
        }

        idx -= 1;
    }
    if idx == -1 {
        buf.copy_from_slice(key);
        buf.put_u8(0);
    }

    buf
}
#[cfg(test)]
mod test {
    use common_types::schema::IndexInWriterSchema;

    use crate::codec::{
        row::{WalRowDecoder, WalRowEncoder},
        Decoder, Encoder,
    };

    #[test]
    fn test_wal_encode_decode() {
        let schema = common_types::tests::build_schema();
        let rows = common_types::tests::build_rows();
        let index_in_writer = IndexInWriterSchema::for_same_schema(schema.num_columns());
        let wal_encoder = WalRowEncoder {
            table_schema: &schema,
            index_in_writer: &index_in_writer,
        };
        let wal_decoder = WalRowDecoder::new(&schema);
        for row in rows {
            let mut buf = Vec::new();
            wal_encoder.encode(&mut buf, &row).unwrap();
            let row_decoded = wal_decoder.decode(&mut buf.as_slice()).unwrap();
            assert_eq!(row_decoded, row);
        }
    }
}
