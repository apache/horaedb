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
use generic_error::GenericError;
use macros::define_result;
use snafu::{ensure, Backtrace, ResultExt, Snafu};

pub const RECORD_ENCODING_V0: u8 = 0;
pub const NEWEST_RECORD_ENCODING_VERSION: u8 = RECORD_ENCODING_V0;

pub const VERSION_SIZE: usize = 1;
pub const CRC_SIZE: usize = 4;
pub const RECORD_LENGTH_SIZE: usize = 4;

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
/// +---------+--------+--------+------------+--------------+--------------+-------+
/// | version |  crc   | length |  table id  | sequence num | value length | value |
/// |  (u8)   | (u32)  | (u32)  |   (u64)    |    (u64)     |     (u32)    |       |
/// +---------+--------+--------+------------+--------------+--------------+-------+
/// ```
pub struct Record<'a> {
    /// The version number of the record.
    pub version: u8,

    /// The CRC checksum of the record.
    pub crc: u32,

    /// The length of the record (excluding version, crc and length).
    pub length: u32,

    /// Identifier for tables.
    pub table_id: u64,

    /// Identifier for records, incrementing.
    pub sequence_num: u64,

    /// The length of the value in bytes.
    pub value_length: u32,

    /// Common log value.
    pub value: &'a [u8],
}

impl<'a> Record<'a> {
    pub fn new(table_id: u64, sequence_num: u64, value: &'a [u8]) -> Result<Self> {
        let mut record = Record {
            version: NEWEST_RECORD_ENCODING_VERSION,
            crc: 0,
            length: (8 + 8 + 4 + value.len()) as u32,
            table_id,
            sequence_num,
            value_length: value.len() as u32,
            value,
        };

        // Calculate CRC
        let mut buf = Vec::new();
        buf.try_put_u64(table_id).context(Encoding)?;
        buf.try_put_u64(sequence_num).context(Encoding)?;
        buf.try_put_u32(record.value_length).context(Encoding)?;
        buf.extend_from_slice(value);
        record.crc = crc32fast::hash(&buf);

        Ok(record)
    }

    // Return the length of the record
    pub fn len(&self) -> usize {
        VERSION_SIZE + CRC_SIZE + RECORD_LENGTH_SIZE + self.length as usize
    }
}

#[derive(Clone, Debug)]
pub struct RecordEncoding {
    version: u8,
}

impl RecordEncoding {
    pub fn newest() -> Self {
        Self {
            version: NEWEST_RECORD_ENCODING_VERSION,
        }
    }
}

impl Encoder<Record<'_>> for RecordEncoding {
    type Error = Error;

    fn encode<B: BufMut>(&self, buf: &mut B, record: &Record) -> Result<()> {
        // Verify version
        ensure!(
            record.version == self.version,
            Version {
                expected: self.version,
                given: record.version
            }
        );

        buf.try_put_u8(record.version).context(Encoding)?;
        buf.try_put_u32(record.crc).context(Encoding)?;
        buf.try_put_u32(record.length).context(Encoding)?;
        buf.try_put_u64(record.table_id).context(Encoding)?;
        buf.try_put_u64(record.sequence_num).context(Encoding)?;
        buf.try_put_u32(record.value_length).context(Encoding)?;
        buf.try_put(record.value).context(Encoding)?;
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
            buf.remaining() >= VERSION_SIZE + CRC_SIZE + RECORD_LENGTH_SIZE,
            LengthMismatch {
                expected: VERSION_SIZE + CRC_SIZE + RECORD_LENGTH_SIZE,
                actual: buf.remaining()
            }
        );

        // Read version
        let version = buf.try_get_u8().context(Decoding)?;

        // Verify version
        ensure!(
            version == self.version,
            Version {
                expected: self.version,
                given: version
            }
        );

        // Read CRC
        let crc = buf.try_get_u32().context(Decoding)?;

        // Read length
        let length = buf.try_get_u32().context(Decoding)?;

        // Ensure the buf is long enough
        ensure!(
            buf.remaining() >= length as usize,
            LengthMismatch {
                expected: length as usize,
                actual: buf.remaining()
            }
        );

        // Verify CRC
        let data = &buf[0..length as usize];
        let computed_crc = crc32fast::hash(data);
        ensure!(
            computed_crc == crc,
            ChecksumMismatch {
                expected: crc,
                actual: computed_crc
            }
        );

        // Read table id
        let table_id = buf.try_get_u64().context(Decoding)?;

        // Read sequence number
        let sequence_num = buf.try_get_u64().context(Decoding)?;

        // Read value length
        let value_length = buf.try_get_u32().context(Decoding)?;

        // Read value
        let value = &buf[0..value_length as usize];
        buf.advance(value_length as usize);

        Ok(Record {
            version,
            crc,
            length,
            table_id,
            sequence_num,
            value_length,
            value,
        })
    }
}

#[cfg(test)]
mod tests {
    use bytes_ext::BytesMut;
    use codec::Encoder;

    use crate::local_storage_impl::record_encoding::{Record, RecordEncoding};

    #[test]
    fn test_record_encoding() {
        let table_id = 1;
        let sequence_num = 2;
        let value = b"test_value";
        let record = Record::new(table_id, sequence_num, value).unwrap();

        let encoder = RecordEncoding::newest();
        let mut buf = BytesMut::new();
        encoder.encode(&mut buf, &record).unwrap();

        let expected_len = record.len();
        assert_eq!(buf.len(), expected_len);
    }

    #[test]
    fn test_record_decoding() {
        let table_id = 1;
        let sequence_num = 2;
        let value = b"test_value";
        let record = Record::new(table_id, sequence_num, value).unwrap();

        let encoder = RecordEncoding::newest();
        let mut buf = BytesMut::new();
        encoder.encode(&mut buf, &record).unwrap();

        let decoded_record = encoder.decode(&buf).unwrap();

        assert_eq!(decoded_record.version, record.version);
        assert_eq!(decoded_record.crc, record.crc);
        assert_eq!(decoded_record.length, record.length);
        assert_eq!(decoded_record.table_id, record.table_id);
        assert_eq!(decoded_record.sequence_num, record.sequence_num);
        assert_eq!(decoded_record.value_length, record.value_length);
        assert_eq!(decoded_record.value, record.value);
    }
}
