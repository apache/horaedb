// Copyright 2023 The CeresDB Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Datum encoding in columnar way.
//!
//! Notice: The encoded results may be used in persisting, so the compatibility
//! must be taken considerations into.

use bytes_ext::{Buf, BufMut, Bytes};
use common_types::{
    column_schema::ColumnId,
    datum::{Datum, DatumKind, DatumView},
    row::bitset::{BitSet, RoBitSet},
    string::StringBytes,
    time::Timestamp,
};
use macros::define_result;
use snafu::{self, ensure, Backtrace, OptionExt, ResultExt, Snafu};

use crate::varint;

mod bool;
mod bytes;
mod number;
mod timestamp;

#[derive(Debug, Snafu)]
#[snafu(visibility(pub(crate)))]
pub enum Error {
    #[snafu(display("Invalid version:{version}.\nBacktrace:\n{backtrace}"))]
    InvalidVersion { version: u8, backtrace: Backtrace },

    #[snafu(display("Invalid compression flag:{flag}.\nBacktrace:\n{backtrace}"))]
    InvalidCompression { flag: u8, backtrace: Backtrace },

    #[snafu(display("Invalid boolean value:{value}.\nBacktrace:\n{backtrace}"))]
    InvalidBooleanValue { value: u8, backtrace: Backtrace },

    #[snafu(display("Invalid datum kind, err:{source}"))]
    InvalidDatumKind { source: common_types::datum::Error },

    #[snafu(display("No enough bytes to compose the bit set.\nBacktrace:\n{backtrace}"))]
    InvalidBitSetBuf { backtrace: Backtrace },

    #[snafu(display(
        "Datums is not enough, expect:{expect}, found:{found}.\nBacktrace:\n{backtrace}"
    ))]
    NotEnoughDatums {
        expect: usize,
        found: usize,
        backtrace: Backtrace,
    },

    #[snafu(display("Failed to varint, err:{source}"))]
    Varint { source: varint::Error },

    #[snafu(display("Failed to do compression, err:{source}.\nBacktrace:\n{backtrace}"))]
    Compress {
        source: std::io::Error,
        backtrace: Backtrace,
    },

    #[snafu(display("Failed to decompress, err:{source}.\nBacktrace:\n{backtrace}"))]
    Decompress {
        source: std::io::Error,
        backtrace: Backtrace,
    },

    #[snafu(display("Failed to do compact encoding, err:{source}"))]
    CompactEncode { source: crate::compact::Error },

    #[snafu(display("Too long bytes, length:{num_bytes}.\nBacktrace:\n{backtrace}"))]
    TooLongBytes {
        num_bytes: usize,
        backtrace: Backtrace,
    },

    #[snafu(display("Bytes is not enough, length:{len}.\nBacktrace:\n{backtrace}"))]
    NotEnoughBytes { len: usize, backtrace: Backtrace },

    #[snafu(display("Number operation overflowed, msg:{msg}.\nBacktrace:\n{backtrace}"))]
    Overflow { msg: String, backtrace: Backtrace },
}

define_result!(Error);

/// The trait bound on the encoders for different types.
trait ValuesEncoder<T> {
    /// Encode a batch of values into the `buf`.
    ///
    /// As the `estimated_encoded_size` method is provided, the `buf` should be
    /// pre-allocate.
    fn encode<B, I>(&self, buf: &mut B, values: I) -> Result<()>
    where
        B: BufMut,
        I: Iterator<Item = T> + Clone;

    /// The estimated size for memory pre-allocated.
    fn estimated_encoded_size<I>(&self, values: I) -> usize
    where
        I: Iterator<Item = T>,
    {
        let (lower, higher) = values.size_hint();
        let num = lower.max(higher.unwrap_or_default());
        num * std::mem::size_of::<T>()
    }
}

/// The decode context for decoding column.
pub struct DecodeContext<'a> {
    /// Buffer for reuse during decoding.
    pub buf: &'a mut Vec<u8>,
}

/// The trait bound on the decoders for different types.
trait ValuesDecoder<T> {
    fn decode<B, F>(&self, ctx: DecodeContext<'_>, buf: &mut B, f: F) -> Result<()>
    where
        B: Buf,
        F: FnMut(T) -> Result<()>;
}

#[derive(Debug, Default)]
/// The implementation for [`ValuesEncoder`].
struct ValuesEncoderImpl {
    bytes_compress_threshold: usize,
}

/// The implementation for [`ValuesDecoder`].
struct ValuesDecoderImpl;

#[derive(Clone, Debug)]
pub struct ColumnarEncoder {
    column_id: ColumnId,
    bytes_compress_threshold: usize,
}

/// A hint helps column encoding.
pub struct EncodeHint {
    pub num_nulls: Option<usize>,
    pub num_datums: Option<usize>,
    pub datum_kind: DatumKind,
}

impl EncodeHint {
    fn compute_num_nulls<'a, I>(&mut self, datums: &I) -> usize
    where
        I: Iterator<Item = DatumView<'a>> + Clone,
    {
        if let Some(v) = self.num_nulls {
            v
        } else {
            let num_nulls = datums.clone().filter(|v| v.is_null()).count();
            self.num_nulls = Some(num_nulls);
            num_nulls
        }
    }

    fn compute_num_datums<'a, I>(&mut self, datums: &I) -> usize
    where
        I: Iterator<Item = DatumView<'a>> + Clone,
    {
        if let Some(v) = self.num_datums {
            v
        } else {
            let num_datums = datums.clone().count();
            self.num_datums = Some(num_datums);
            num_datums
        }
    }
}

impl ColumnarEncoder {
    const VERSION: u8 = 0;

    pub fn new(column_id: ColumnId, bytes_compress_threshold: usize) -> Self {
        Self {
            column_id,
            bytes_compress_threshold,
        }
    }

    /// The header includes `version`, `datum_kind`, `column_id`, `num_datums`
    /// and `num_nulls`.
    ///
    /// Refer to the [encode](ColumnarEncoder::encode) method.
    #[inline]
    const fn header_size() -> usize {
        1 + 1 + 4 + 4 + 4
    }

    /// The layout of the final serialized bytes:
    /// ```plaintext
    /// +-------------+----------------+-----------------+-----------------+----------------+---------------+---------------------+
    /// | version(u8) | datum_kind(u8) | column_id(u32) | num_datums(u32) | num_nulls(u32) | nulls_bit_set | non-null data block |
    /// +-------------+----------------+-----------------+-----------------+----------------+---------------+---------------------+
    /// ```
    /// Note:
    /// 1. The `num_nulls`, `nulls_bit_set` and `non-null data block` will not
    /// exist if the kind of datum is null;
    /// 2. The `nulls_bit_set` will not exist if the `num_nulls` is zero;
    /// 3. The `nulls_bit_set` and `non-null data block` will not exist if the
    /// `num_nulls` equals the `num_datums`;
    pub fn encode<'a, I, B>(&self, buf: &mut B, datums: I, hint: &mut EncodeHint) -> Result<()>
    where
        I: Iterator<Item = DatumView<'a>> + Clone,
        B: BufMut,
    {
        buf.put_u8(Self::VERSION);
        buf.put_u8(hint.datum_kind.into_u8());
        buf.put_u32(self.column_id);
        let num_datums = hint.compute_num_datums(&datums);
        assert!(num_datums < u32::MAX as usize);
        buf.put_u32(num_datums as u32);

        // For null datum, there is no more data to put.
        if matches!(hint.datum_kind, DatumKind::Null) {
            return Ok(());
        }

        let num_nulls = hint.compute_num_nulls(&datums);
        assert!(num_nulls < u32::MAX as usize);

        buf.put_u32(num_nulls as u32);
        if num_nulls > 0 {
            let mut bit_set = BitSet::all_set(num_datums);
            for (idx, value) in datums.clone().enumerate() {
                if value.is_null() {
                    bit_set.unset(idx);
                }
            }

            buf.put_slice(bit_set.as_bytes());
        }

        self.encode_datums(buf, datums, hint.datum_kind)
    }

    pub fn estimated_encoded_size<'a, I>(&self, datums: I, hint: &mut EncodeHint) -> usize
    where
        I: Iterator<Item = DatumView<'a>> + Clone,
    {
        let bit_set_size = if matches!(hint.datum_kind, DatumKind::Null) {
            0
        } else {
            let num_datums = hint.compute_num_datums(&datums);
            BitSet::num_bytes(num_datums)
        };

        let enc = ValuesEncoderImpl::default();
        let data_size = match hint.datum_kind {
            DatumKind::Null => 0,
            DatumKind::Timestamp => {
                enc.estimated_encoded_size(datums.clone().filter_map(|v| v.as_timestamp()))
            }
            DatumKind::Double => {
                enc.estimated_encoded_size(datums.clone().filter_map(|v| v.as_f64()))
            }
            DatumKind::Float => {
                enc.estimated_encoded_size(datums.clone().filter_map(|v| v.as_f32()))
            }
            DatumKind::Varbinary => {
                enc.estimated_encoded_size(datums.clone().filter_map(|v| v.into_bytes()))
            }
            DatumKind::String => enc.estimated_encoded_size(
                datums
                    .clone()
                    .filter_map(|v| v.into_str().map(|v| v.as_bytes())),
            ),
            DatumKind::UInt64 => {
                enc.estimated_encoded_size(datums.clone().filter_map(|v| v.as_u64()))
            }
            DatumKind::UInt32 => {
                enc.estimated_encoded_size(datums.clone().filter_map(|v| v.as_u32()))
            }
            DatumKind::UInt16 => {
                enc.estimated_encoded_size(datums.clone().filter_map(|v| v.as_u16()))
            }
            DatumKind::UInt8 => {
                enc.estimated_encoded_size(datums.clone().filter_map(|v| v.as_u8()))
            }
            DatumKind::Int64 => {
                enc.estimated_encoded_size(datums.clone().filter_map(|v| v.as_i64()))
            }
            DatumKind::Int32 => {
                enc.estimated_encoded_size(datums.clone().filter_map(|v| v.as_i32()))
            }
            DatumKind::Int16 => {
                enc.estimated_encoded_size(datums.clone().filter_map(|v| v.as_i16()))
            }
            DatumKind::Int8 => enc.estimated_encoded_size(datums.clone().filter_map(|v| v.as_i8())),
            DatumKind::Boolean => {
                enc.estimated_encoded_size(datums.clone().filter_map(|v| v.as_bool()))
            }
            DatumKind::Date => {
                enc.estimated_encoded_size(datums.clone().filter_map(|v| v.as_date_i32()))
            }
            DatumKind::Time => {
                enc.estimated_encoded_size(datums.clone().filter_map(|v| v.as_timestamp()))
            }
        };

        Self::header_size() + bit_set_size + data_size
    }

    fn encode_datums<'a, I, B>(&self, buf: &mut B, datums: I, datum_kind: DatumKind) -> Result<()>
    where
        I: Iterator<Item = DatumView<'a>> + Clone,
        B: BufMut,
    {
        let enc = ValuesEncoderImpl {
            bytes_compress_threshold: self.bytes_compress_threshold,
        };
        match datum_kind {
            DatumKind::Null => Ok(()),
            DatumKind::Timestamp => enc.encode(buf, datums.filter_map(|v| v.as_timestamp())),
            DatumKind::Double => enc.encode(buf, datums.filter_map(|v| v.as_f64())),
            DatumKind::Float => enc.encode(buf, datums.filter_map(|v| v.as_f32())),
            DatumKind::Varbinary => enc.encode(buf, datums.filter_map(|v| v.into_bytes())),
            DatumKind::String => enc.encode(
                buf,
                datums.filter_map(|v| v.into_str().map(|v| v.as_bytes())),
            ),
            DatumKind::UInt64 => enc.encode(buf, datums.filter_map(|v| v.as_u64())),
            DatumKind::UInt32 => enc.encode(buf, datums.filter_map(|v| v.as_u32())),
            DatumKind::UInt16 => enc.encode(buf, datums.filter_map(|v| v.as_u16())),
            DatumKind::UInt8 => enc.encode(buf, datums.filter_map(|v| v.as_u8())),
            DatumKind::Int64 => enc.encode(buf, datums.filter_map(|v| v.as_i64())),
            DatumKind::Int32 => enc.encode(buf, datums.filter_map(|v| v.as_i32())),
            DatumKind::Int16 => enc.encode(buf, datums.filter_map(|v| v.as_i16())),
            DatumKind::Int8 => enc.encode(buf, datums.filter_map(|v| v.as_i8())),
            DatumKind::Boolean => enc.encode(buf, datums.filter_map(|v| v.as_bool())),
            DatumKind::Date => enc.encode(buf, datums.filter_map(|v| v.as_date_i32())),
            DatumKind::Time => enc.encode(buf, datums.filter_map(|v| v.as_timestamp())),
        }
    }
}

/// The decoder for [`ColumnarEncoder`].
#[derive(Debug, Clone)]
pub struct ColumnarDecoder;

#[derive(Debug, Clone)]
pub struct DecodeResult {
    pub column_id: ColumnId,
    pub datums: Vec<Datum>,
}

impl ColumnarDecoder {
    pub fn decode<B: Buf>(&self, ctx: DecodeContext<'_>, buf: &mut B) -> Result<DecodeResult> {
        let version = buf.get_u8();
        ensure!(
            version == ColumnarEncoder::VERSION,
            InvalidVersion { version }
        );

        let datum_kind = DatumKind::try_from(buf.get_u8()).context(InvalidDatumKind)?;
        let column_id = buf.get_u32();
        let num_datums = buf.get_u32() as usize;

        if matches!(datum_kind, DatumKind::Null) {
            return Ok(DecodeResult {
                column_id,
                datums: vec![Datum::Null; num_datums],
            });
        }

        let num_nulls = buf.get_u32() as usize;
        let datums = if num_nulls == num_datums {
            vec![Datum::Null; num_datums]
        } else if num_nulls > 0 {
            Self::decode_with_nulls(ctx, buf, num_datums, datum_kind)?
        } else {
            Self::decode_without_nulls(ctx, buf, num_datums, datum_kind)?
        };

        Ok(DecodeResult { column_id, datums })
    }
}

impl ColumnarDecoder {
    fn decode_with_nulls<B: Buf>(
        ctx: DecodeContext<'_>,
        buf: &B,
        num_datums: usize,
        datum_kind: DatumKind,
    ) -> Result<Vec<Datum>> {
        let chunk = buf.chunk();
        let bit_set = RoBitSet::try_new(chunk, num_datums).context(InvalidBitSetBuf)?;

        let mut datums = Vec::with_capacity(num_datums);
        let with_datum = |datum: Datum| {
            let idx = datums.len();
            let null = bit_set.is_unset(idx).context(InvalidBitSetBuf)?;
            if null {
                datums.push(Datum::Null);
            }
            datums.push(datum);

            Ok(())
        };

        let mut data_block = &chunk[BitSet::num_bytes(num_datums)..];
        Self::decode_datums(ctx, &mut data_block, datum_kind, with_datum)?;

        Ok(datums)
    }

    fn decode_without_nulls<B: Buf>(
        ctx: DecodeContext<'_>,
        buf: &mut B,
        num_datums: usize,
        datum_kind: DatumKind,
    ) -> Result<Vec<Datum>> {
        let mut datums = Vec::with_capacity(num_datums);
        let with_datum = |datum: Datum| {
            datums.push(datum);
            Ok(())
        };
        Self::decode_datums(ctx, buf, datum_kind, with_datum)?;
        Ok(datums)
    }

    fn decode_datums<B, F>(
        ctx: DecodeContext<'_>,
        buf: &mut B,
        datum_kind: DatumKind,
        mut f: F,
    ) -> Result<()>
    where
        B: Buf,
        F: FnMut(Datum) -> Result<()>,
    {
        match datum_kind {
            DatumKind::Null => Ok(()),
            DatumKind::Timestamp => {
                let with_timestamp = |v: Timestamp| f(Datum::from(v));
                ValuesDecoderImpl.decode(ctx, buf, with_timestamp)
            }
            DatumKind::Double => {
                let with_float = |v: f64| f(Datum::from(v));
                ValuesDecoderImpl.decode(ctx, buf, with_float)
            }
            DatumKind::Float => {
                let with_float = |v: f32| f(Datum::from(v));
                ValuesDecoderImpl.decode(ctx, buf, with_float)
            }
            DatumKind::Varbinary => {
                let with_bytes = |v: Bytes| f(Datum::from(v));
                ValuesDecoderImpl.decode(ctx, buf, with_bytes)
            }
            DatumKind::String => {
                let with_str = |value| {
                    let datum = unsafe { Datum::from(StringBytes::from_bytes_unchecked(value)) };
                    f(datum)
                };
                ValuesDecoderImpl.decode(ctx, buf, with_str)
            }
            DatumKind::UInt64 => {
                let with_u64 = |value: u64| {
                    let datum = Datum::from(value);
                    f(datum)
                };
                ValuesDecoderImpl.decode(ctx, buf, with_u64)
            }
            DatumKind::UInt32 => {
                let with_u32 = |value: u32| {
                    let datum = Datum::from(value);
                    f(datum)
                };
                ValuesDecoderImpl.decode(ctx, buf, with_u32)
            }
            DatumKind::UInt16 => {
                let with_u16 = |value: u16| {
                    let datum = Datum::from(value);
                    f(datum)
                };
                ValuesDecoderImpl.decode(ctx, buf, with_u16)
            }
            DatumKind::UInt8 => {
                let with_u8 = |value: u8| {
                    let datum = Datum::from(value);
                    f(datum)
                };
                ValuesDecoderImpl.decode(ctx, buf, with_u8)
            }
            DatumKind::Int64 => {
                let with_i64 = |value: i64| {
                    let datum = Datum::from(value);
                    f(datum)
                };
                ValuesDecoderImpl.decode(ctx, buf, with_i64)
            }
            DatumKind::Int32 => {
                let with_i32 = |v: i32| f(Datum::from(v));
                ValuesDecoderImpl.decode(ctx, buf, with_i32)
            }
            DatumKind::Int16 => {
                let with_i16 = |value: i16| {
                    let datum = Datum::from(value);
                    f(datum)
                };
                ValuesDecoderImpl.decode(ctx, buf, with_i16)
            }
            DatumKind::Int8 => {
                let with_i8 = |value: i8| {
                    let datum = Datum::from(value);
                    f(datum)
                };
                ValuesDecoderImpl.decode(ctx, buf, with_i8)
            }
            DatumKind::Boolean => {
                let with_bool = |v: bool| f(Datum::from(v));
                ValuesDecoderImpl.decode(ctx, buf, with_bool)
            }
            DatumKind::Date => {
                let with_i32 = |value: i32| {
                    let datum = Datum::Date(value);
                    f(datum)
                };
                ValuesDecoderImpl.decode(ctx, buf, with_i32)
            }
            DatumKind::Time => {
                let with_timestamp = |v: Timestamp| f(Datum::Time(v.as_i64()));
                ValuesDecoderImpl.decode(ctx, buf, with_timestamp)
            }
        }
    }
}
#[cfg(test)]
mod tests {
    use super::*;

    fn check_encode_end_decode(column_id: ColumnId, datums: Vec<Datum>, datum_kind: DatumKind) {
        let encoder = ColumnarEncoder::new(column_id, 256);
        let views = datums.iter().map(|v| v.as_view());
        let mut hint = EncodeHint {
            num_nulls: None,
            num_datums: None,
            datum_kind,
        };

        let buf_len = encoder.estimated_encoded_size(views.clone(), &mut hint);
        let mut buf = Vec::with_capacity(buf_len);
        encoder.encode(&mut buf, views, &mut hint).unwrap();

        // Ensure no growth over the capacity.
        assert!(buf.capacity() <= buf_len);

        let mut reused_buf = Vec::new();
        let ctx = DecodeContext {
            buf: &mut reused_buf,
        };
        let decoder = ColumnarDecoder;
        let DecodeResult {
            column_id: decoded_column_id,
            datums: decoded_datums,
        } = decoder.decode(ctx, &mut buf.as_slice()).unwrap();
        assert_eq!(column_id, decoded_column_id);
        assert_eq!(datums, decoded_datums);
    }

    #[test]
    fn test_small_int() {
        let datums = [10u32, 1u32, 2u32, 81u32, 82u32];

        check_encode_end_decode(
            10,
            datums.iter().map(|v| Datum::from(*v)).collect(),
            DatumKind::UInt32,
        );

        check_encode_end_decode(
            10,
            datums.iter().map(|v| Datum::from(*v as i32)).collect(),
            DatumKind::Int32,
        );

        check_encode_end_decode(
            10,
            datums.iter().map(|v| Datum::from(*v as u16)).collect(),
            DatumKind::UInt16,
        );

        check_encode_end_decode(
            10,
            datums.iter().map(|v| Datum::from(*v as i16)).collect(),
            DatumKind::Int16,
        );

        check_encode_end_decode(
            10,
            datums.iter().map(|v| Datum::from(*v as i8)).collect(),
            DatumKind::Int8,
        );

        check_encode_end_decode(
            10,
            datums.iter().map(|v| Datum::from(*v as u8)).collect(),
            DatumKind::UInt8,
        );
    }

    #[test]
    fn test_with_empty_datums() {
        check_encode_end_decode(1, vec![], DatumKind::Null);
        check_encode_end_decode(1, vec![], DatumKind::Timestamp);
        check_encode_end_decode(1, vec![], DatumKind::Double);
        check_encode_end_decode(1, vec![], DatumKind::Float);
        check_encode_end_decode(1, vec![], DatumKind::Varbinary);
        check_encode_end_decode(1, vec![], DatumKind::String);
        check_encode_end_decode(1, vec![], DatumKind::UInt64);
        check_encode_end_decode(1, vec![], DatumKind::UInt32);
        check_encode_end_decode(1, vec![], DatumKind::UInt8);
        check_encode_end_decode(1, vec![], DatumKind::Int64);
        check_encode_end_decode(1, vec![], DatumKind::Int32);
        check_encode_end_decode(1, vec![], DatumKind::Int16);
        check_encode_end_decode(1, vec![], DatumKind::Int8);
        check_encode_end_decode(1, vec![], DatumKind::Boolean);
        check_encode_end_decode(1, vec![], DatumKind::Date);
        check_encode_end_decode(1, vec![], DatumKind::Time);
    }

    #[test]
    fn test_i32_with_null() {
        let datums = vec![
            Datum::from(10i32),
            Datum::from(1i32),
            Datum::Null,
            Datum::from(18i32),
            Datum::from(38i32),
            Datum::from(48i32),
            Datum::Null,
            Datum::from(81i32),
            Datum::from(82i32),
        ];

        check_encode_end_decode(10, datums, DatumKind::Int32);
    }

    #[test]
    fn test_all_nulls() {
        let datums = vec![
            Datum::Null,
            Datum::Null,
            Datum::Null,
            Datum::Null,
            Datum::Null,
            Datum::Null,
        ];

        check_encode_end_decode(10, datums, DatumKind::Int32);
    }

    #[test]
    fn test_null() {
        let datums = vec![
            Datum::Null,
            Datum::Null,
            Datum::Null,
            Datum::Null,
            Datum::Null,
            Datum::Null,
        ];

        check_encode_end_decode(10, datums, DatumKind::Null);
    }

    #[test]
    fn test_float() {
        let datums = vec![Datum::from(10.0f32), Datum::from(9.0f32)];
        check_encode_end_decode(10, datums, DatumKind::Float);

        let datums = vec![Datum::from(10.0f64), Datum::from(1.0f64)];
        check_encode_end_decode(10, datums, DatumKind::Double);
    }

    #[test]
    fn test_i64() {
        let datums = vec![
            Datum::from(10i64),
            Datum::from(1i64),
            Datum::from(2i64),
            Datum::from(18i64),
            Datum::from(38i64),
            Datum::from(48i64),
            Datum::from(-80i64),
            Datum::from(-81i64),
            Datum::from(-82i64),
        ];

        check_encode_end_decode(10, datums, DatumKind::Int64);
    }

    #[test]
    fn test_u64() {
        let datums = vec![
            Datum::from(10u64),
            Datum::from(1u64),
            Datum::from(2u64),
            Datum::from(18u64),
            Datum::from(38u64),
            Datum::from(48u64),
            Datum::from(80u64),
            Datum::from(81u64),
            Datum::from(82u64),
        ];

        check_encode_end_decode(10, datums, DatumKind::UInt64);
    }

    #[test]
    fn test_timestamp() {
        let datums = vec![
            Datum::from(Timestamp::new(-10)),
            Datum::from(Timestamp::new(10)),
            Datum::from(Timestamp::new(1024)),
            Datum::from(Timestamp::new(1024)),
            Datum::from(Timestamp::new(1025)),
        ];

        check_encode_end_decode(10, datums, DatumKind::Timestamp);
    }

    #[test]
    fn test_time() {
        let datums = vec![
            Datum::Time(-10),
            Datum::Time(10),
            Datum::Time(1024),
            Datum::Time(1024),
            Datum::Time(1025),
        ];

        check_encode_end_decode(10, datums, DatumKind::Time);
    }

    #[test]
    fn test_overflow_timestamp() {
        let datums = vec![
            Datum::from(Timestamp::new(i64::MIN)),
            Datum::from(Timestamp::new(10)),
            Datum::from(Timestamp::new(1024)),
            Datum::from(Timestamp::new(1024)),
            Datum::from(Timestamp::new(1025)),
        ];

        let encoder = ColumnarEncoder::new(0, 256);
        let views = datums.iter().map(|v| v.as_view());
        let mut hint = EncodeHint {
            num_nulls: None,
            num_datums: None,
            datum_kind: DatumKind::Timestamp,
        };

        let mut buf = Vec::new();
        let enc_res = encoder.encode(&mut buf, views, &mut hint);
        assert!(enc_res.is_err());
    }

    #[test]
    fn test_string() {
        let datums = vec![
            Datum::from("vvvv"),
            Datum::from("xxxx"),
            Datum::from("8"),
            Datum::from("9999"),
            Datum::from(""),
        ];

        check_encode_end_decode(10, datums, DatumKind::String);
    }

    #[test]
    fn test_boolean() {
        let datums = vec![
            Datum::from(false),
            Datum::from(false),
            Datum::from(true),
            Datum::Null,
            Datum::from(false),
        ];

        check_encode_end_decode(10, datums.clone(), DatumKind::Boolean);

        let mut massive_datums = Vec::with_capacity(10 * datums.len());
        for _ in 0..10 {
            massive_datums.append(&mut datums.clone());
        }

        check_encode_end_decode(10, massive_datums, DatumKind::Boolean);
    }

    #[test]
    fn test_massive_string() {
        let sample_datums = vec![
            Datum::from("vvvv"),
            Datum::from("xxxx"),
            Datum::from("8"),
            Datum::from("9999"),
        ];
        let mut datums = Vec::with_capacity(sample_datums.len() * 100);
        for _ in 0..100 {
            datums.append(&mut sample_datums.clone());
        }

        check_encode_end_decode(10, datums, DatumKind::String);
    }

    #[test]
    fn test_large_string() {
        let large_string_bytes = vec![
            vec![b'a'; 500],
            vec![b'x'; 5000],
            vec![b'x'; 5],
            vec![],
            vec![b' '; 15000],
        ];
        let datums = large_string_bytes
            .iter()
            .map(|v| Datum::from(&*String::from_utf8_lossy(&v[..])))
            .collect();

        check_encode_end_decode(10, datums, DatumKind::String);
    }
}
