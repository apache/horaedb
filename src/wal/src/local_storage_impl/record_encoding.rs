// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use bytes_ext::{Buf, BufMut, SafeBuf, SafeBufMut};
use codec::Encoder;
use crc32fast::Hasher;
use generic_error::GenericError;
use macros::define_result;
use snafu::{ensure, Backtrace, ResultExt, Snafu};

pub const RECORD_ENCODING_V0: u8 = 0;
pub const NEWEST_RECORD_ENCODING_VERSION: u8 = RECORD_ENCODING_V0;

pub const VERSION_SIZE: usize = 1;
pub const CRC_SIZE: usize = 4;
pub const TABLE_ID_SIZE: usize = 8;
pub const SEQUENCE_NUM_SIZE: usize = 8;
pub const VALUE_LENGTH_SIZE: usize = 4;
pub const RECORD_HEADER_SIZE: usize =
    VERSION_SIZE + CRC_SIZE + TABLE_ID_SIZE + SEQUENCE_NUM_SIZE + VALUE_LENGTH_SIZE;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Version mismatch, expect:{}, given:{}", expected, given))]
    Version { expected: u8, given: u8 },

    #[snafu(display("Failed to encode, err:{}", source))]
    Encoding { source: bytes_ext::Error },

    #[snafu(display("Failed to decode, err:{}", source))]
    Decoding { source: bytes_ext::Error },

    #[snafu(display("Invalid record: {}, backtrace:\n{}", source, backtrace))]
    InvalidRecord {
        source: GenericError,
        backtrace: Backtrace,
    },

    #[snafu(display("Length mismatch: expected {} but found {}", expected, actual))]
    LengthMismatch { expected: usize, actual: usize },

    #[snafu(display("Checksum mismatch: expected {}, but got {}", expected, actual))]
    ChecksumMismatch { expected: u32, actual: u32 },
}

define_result!(Error);

/// Record format:
///
/// ```text
/// +---------+--------+------------+--------------+--------------+-------+
/// | version |  crc   |  table id  | sequence num | value length | value |
/// |  (u8)   | (u32)  |   (u64)    |    (u64)     |     (u32)    |(bytes)|
/// +---------+--------+------------+--------------+--------------+-------+
/// ```
#[derive(Debug)]
pub struct Record {
    /// The version number of the record.
    pub version: u8,

    /// The CRC checksum of the record.
    pub crc: u32,

    /// Identifier for tables.
    pub table_id: u64,

    /// Identifier for records, incrementing.
    pub sequence_num: u64,

    /// The length of the value in bytes.
    pub value_length: u32,

    /// Common log value.
    pub value: Vec<u8>,
}

impl Record {
    pub fn new(table_id: u64, sequence_num: u64, value: &[u8]) -> Self {
        Record {
            version: NEWEST_RECORD_ENCODING_VERSION,
            crc: compute_crc32(table_id, sequence_num, value),
            table_id,
            sequence_num,
            value_length: value.len() as u32,
            value: value.to_vec(),
        }
    }

    // Return the length of the record
    pub fn len(&self) -> usize {
        RECORD_HEADER_SIZE + self.value_length as usize
    }
}

#[derive(Clone, Debug)]
pub struct RecordEncoding {
    expected_version: u8,
}

impl RecordEncoding {
    pub fn newest() -> Self {
        Self {
            expected_version: NEWEST_RECORD_ENCODING_VERSION,
        }
    }
}

impl Encoder<Record> for RecordEncoding {
    type Error = Error;

    fn encode<B: BufMut>(&self, buf: &mut B, record: &Record) -> Result<()> {
        // Verify version
        ensure!(
            record.version == self.expected_version,
            Version {
                expected: self.expected_version,
                given: record.version
            }
        );

        buf.try_put_u8(record.version).context(Encoding)?;
        buf.try_put_u32(record.crc).context(Encoding)?;
        buf.try_put_u64(record.table_id).context(Encoding)?;
        buf.try_put_u64(record.sequence_num).context(Encoding)?;
        buf.try_put_u32(record.value_length).context(Encoding)?;
        buf.try_put(record.value.as_slice()).context(Encoding)?;
        Ok(())
    }

    fn estimate_encoded_size(&self, record: &Record) -> usize {
        record.len()
    }
}

impl RecordEncoding {
    pub fn decode<'a>(&'a self, mut buf: &'a [u8]) -> Result<Record> {
        // Ensure that buf is not shorter than the shortest record.
        ensure!(
            buf.remaining() >= RECORD_HEADER_SIZE,
            LengthMismatch {
                expected: RECORD_HEADER_SIZE,
                actual: buf.remaining()
            }
        );

        // Read version
        let version = buf.try_get_u8().context(Decoding)?;

        // Verify version
        ensure!(
            version == self.expected_version,
            Version {
                expected: self.expected_version,
                given: version
            }
        );

        let crc = buf.try_get_u32().context(Decoding)?;
        let table_id = buf.try_get_u64().context(Decoding)?;
        let sequence_num = buf.try_get_u64().context(Decoding)?;
        let value_length = buf.try_get_u32().context(Decoding)?;
        let mut value = vec![0; value_length as usize];
        buf.try_copy_to_slice(&mut value).context(Decoding)?;

        // Verify CRC
        let computed_crc = compute_crc32(table_id, sequence_num, &value);
        ensure!(
            computed_crc == crc,
            ChecksumMismatch {
                expected: crc,
                actual: computed_crc
            }
        );

        Ok(Record {
            version,
            crc,
            table_id,
            sequence_num,
            value_length,
            value,
        })
    }
}

/// The crc32 checksum is calculated over the table_id, sequence_num,
/// value_length and value.
// This function does the same with `crc32fast::hash`.
fn compute_crc32(table_id: u64, seq_num: u64, value: &[u8]) -> u32 {
    let mut h = Hasher::new();
    h.update(&table_id.to_le_bytes());
    h.update(&seq_num.to_le_bytes());
    h.update(&value.len().to_le_bytes());
    h.update(value);
    h.finalize()
}

#[cfg(test)]
mod tests {
    use bytes_ext::BytesMut;
    use codec::Encoder;

    use crate::local_storage_impl::record_encoding::{Record, RecordEncoding};

    #[test]
    fn test_local_wal_record_encoding() {
        let table_id = 1;
        let sequence_num = 2;
        let value = b"test_value";
        let record = Record::new(table_id, sequence_num, value);

        let encoder = RecordEncoding::newest();
        let mut buf = BytesMut::new();
        encoder.encode(&mut buf, &record).unwrap();

        let expected_len = record.len();
        assert_eq!(buf.len(), expected_len);
    }

    #[test]
    fn test_local_wal_record_decoding() {
        let table_id = 1;
        let sequence_num = 2;
        let value = b"test_value";
        let record = Record::new(table_id, sequence_num, value);

        let encoder = RecordEncoding::newest();
        let mut buf = BytesMut::new();
        encoder.encode(&mut buf, &record).unwrap();

        let decoded_record = encoder.decode(&buf).unwrap();

        assert_eq!(decoded_record.version, record.version);
        assert_eq!(decoded_record.crc, record.crc);
        assert_eq!(decoded_record.table_id, record.table_id);
        assert_eq!(decoded_record.sequence_num, record.sequence_num);
        assert_eq!(decoded_record.value_length, record.value_length);
        assert_eq!(decoded_record.value, record.value);
    }
}
