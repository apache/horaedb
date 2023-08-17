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

use bytes_ext::{Buf, BufMut};
use common_types::{
    datum::{Datum, DatumKind, DatumView},
    row::bitset::{BitSet, RoBitSet},
    string::StringBytes,
};
use macros::define_result;
use snafu::{self, ensure, Backtrace, OptionExt, ResultExt, Snafu};

use self::{
    bytes::{BytesValuesDecoder, BytesValuesEncoder},
    float::{F64ValuesDecoder, F64ValuesEncoder},
    int::{I32ValuesDecoder, I64ValuesDecoder, I64ValuesEncoder},
};
use crate::{columnar::int::I32ValuesEncoder, varint, Decoder};

mod bytes;
mod float;
mod int;
mod timestamp;

#[derive(Debug, Snafu)]
#[snafu(visibility(pub(crate)))]
pub enum Error {
    #[snafu(display("Invalid version:{version}.\nBacktrace:\n{backtrace}"))]
    InvalidVersion { version: u8, backtrace: Backtrace },

    #[snafu(display("Invalid datum kind, err:{source}"))]
    InvalidDatumKind { source: common_types::datum::Error },

    #[snafu(display("No enough bytes to compose the nulls bit set.\nBacktrace:\n{backtrace}"))]
    InvalidNullsBitSet { backtrace: Backtrace },

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

    #[snafu(display("Failed to do compact encoding, err:{source}"))]
    CompactEncode { source: crate::compact::Error },
}

define_result!(Error);

/// The trait bound on the encoders for different types.
pub trait ValuesEncoder {
    type ValueType;
    /// Encode a batch of values into the `buf`.
    ///
    /// As the `estimated_encoded_size` method is provided, the `buf` should be
    /// pre-allocate.
    fn encode<B, I>(&self, buf: &mut B, values: I) -> Result<()>
    where
        B: BufMut,
        I: Iterator<Item = Self::ValueType>;

    /// The estimated size for memory pre-allocated.
    fn estimated_encoded_size<I>(&self, values: I) -> usize
    where
        I: Iterator<Item = Self::ValueType>,
    {
        let (lower, higher) = values.size_hint();
        let num = lower.max(higher.unwrap_or_default());
        num * std::mem::size_of::<Self::ValueType>()
    }
}

pub trait ValuesDecoder {
    type ValueType;

    fn decode<B, F>(&self, buf: &mut B, f: F) -> Result<()>
    where
        B: Buf,
        F: FnMut(Self::ValueType) -> Result<()>;
}

#[derive(Clone, Debug)]
pub struct ColumnarEncoder {
    column_idx: u32,
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

    pub fn new(column_idx: u32) -> Self {
        Self { column_idx }
    }

    /// The header includes `version`, `datum_kind`, `column_idx`, `num_datums`
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
    /// | version(u8) | datum_kind(u8) | column_idx(u32) | num_datums(u32) | num_nulls(u32) | nulls_bit_set | non-null data block |
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
        buf.put_u32(self.column_idx);
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

        Self::encode_datums(buf, datums, hint.datum_kind)
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

        let data_size = match hint.datum_kind {
            DatumKind::Null => 0,
            DatumKind::Timestamp => todo!(),
            DatumKind::Double => todo!(),
            DatumKind::Float => todo!(),
            DatumKind::Varbinary => {
                let enc = BytesValuesEncoder::default();
                enc.estimated_encoded_size(datums.clone().filter_map(|v| v.into_bytes()))
            }
            DatumKind::String => {
                let enc = BytesValuesEncoder::default();
                enc.estimated_encoded_size(
                    datums
                        .clone()
                        .filter_map(|v| v.into_str().map(|v| v.as_bytes())),
                )
            }
            DatumKind::UInt64 => todo!(),
            DatumKind::UInt32 => todo!(),
            DatumKind::UInt16 => todo!(),
            DatumKind::UInt8 => todo!(),
            DatumKind::Int64 => {
                let enc = I64ValuesEncoder;
                enc.estimated_encoded_size(datums.clone().filter_map(|v| v.as_i64()))
            }
            DatumKind::Int32 => {
                let enc = I32ValuesEncoder;
                enc.estimated_encoded_size(datums.clone().filter_map(|v| v.as_i32()))
            }
            DatumKind::Int16 => todo!(),
            DatumKind::Int8 => todo!(),
            DatumKind::Boolean => todo!(),
            DatumKind::Date => todo!(),
            DatumKind::Time => todo!(),
        };

        Self::header_size() + bit_set_size + data_size
    }

    fn encode_datums<'a, I, B>(buf: &mut B, datums: I, datum_kind: DatumKind) -> Result<()>
    where
        I: Iterator<Item = DatumView<'a>>,
        B: BufMut,
    {
        match datum_kind {
            DatumKind::Null => Ok(()),
            DatumKind::Timestamp => todo!(),
            DatumKind::Double => {
                let enc = F64ValuesEncoder;
                enc.encode(buf, datums.filter_map(|v| v.as_f64()))
            }
            DatumKind::Float => todo!(),
            DatumKind::Varbinary => {
                let enc = BytesValuesEncoder::default();
                enc.encode(buf, datums.filter_map(|v| v.into_bytes()))
            }
            DatumKind::String => {
                let enc = BytesValuesEncoder::default();
                enc.encode(
                    buf,
                    datums.filter_map(|v| v.into_str().map(|v| v.as_bytes())),
                )
            }
            DatumKind::UInt64 => todo!(),
            DatumKind::UInt32 => todo!(),
            DatumKind::UInt16 => todo!(),
            DatumKind::UInt8 => todo!(),
            DatumKind::Int64 => {
                let enc = I64ValuesEncoder;
                enc.encode(buf, datums.filter_map(|v| v.as_i64()))
            }
            DatumKind::Int32 => {
                let enc = I32ValuesEncoder;
                enc.encode(buf, datums.filter_map(|v| v.as_i32()))
            }
            DatumKind::Int16 => todo!(),
            DatumKind::Int8 => todo!(),
            DatumKind::Boolean => todo!(),
            DatumKind::Date => todo!(),
            DatumKind::Time => todo!(),
        }
    }
}

/// The decoder for [`ColumnarEncoder`].
#[derive(Debug, Clone)]
pub struct ColumnarDecoder;

#[derive(Debug, Clone)]
pub struct DecodeResult {
    pub column_idx: u32,
    pub datums: Vec<Datum>,
}

impl Decoder<DecodeResult> for ColumnarDecoder {
    type Error = Error;

    fn decode<B: Buf>(&self, buf: &mut B) -> Result<DecodeResult> {
        let version = buf.get_u8();
        ensure!(
            version == ColumnarEncoder::VERSION,
            InvalidVersion { version }
        );

        let datum_kind = DatumKind::try_from(buf.get_u8()).context(InvalidDatumKind)?;
        let column_idx = buf.get_u32();
        let num_datums = buf.get_u32() as usize;

        if matches!(datum_kind, DatumKind::Null) {
            return Ok(DecodeResult {
                column_idx,
                datums: vec![Datum::Null; num_datums],
            });
        }

        let num_nulls = buf.get_u32() as usize;
        let datums = if num_nulls == num_datums {
            vec![Datum::Null; num_datums]
        } else if num_nulls > 0 {
            Self::decode_with_nulls(buf, num_datums, datum_kind)?
        } else {
            Self::decode_without_nulls(buf, num_datums, datum_kind)?
        };

        Ok(DecodeResult { column_idx, datums })
    }
}

impl ColumnarDecoder {
    fn decode_with_nulls<B: Buf>(
        buf: &mut B,
        num_datums: usize,
        datum_kind: DatumKind,
    ) -> Result<Vec<Datum>> {
        let chunk = buf.chunk();
        let bit_set = RoBitSet::try_new(chunk, num_datums).context(InvalidNullsBitSet)?;

        let mut datums = Vec::with_capacity(num_datums);
        let with_datum = |datum: Datum| {
            let idx = datums.len();
            let null = bit_set.is_unset(idx).context(InvalidNullsBitSet)?;
            if null {
                datums.push(Datum::Null);
            }
            datums.push(datum);

            Ok(())
        };

        let mut data_block = &chunk[BitSet::num_bytes(num_datums)..];
        Self::decode_datums(&mut data_block, datum_kind, with_datum)?;

        Ok(datums)
    }

    fn decode_without_nulls<B: Buf>(
        buf: &mut B,
        num_datums: usize,
        datum_kind: DatumKind,
    ) -> Result<Vec<Datum>> {
        let mut datums = Vec::with_capacity(num_datums);
        let with_datum = |datum: Datum| {
            datums.push(datum);
            Ok(())
        };
        Self::decode_datums(buf, datum_kind, with_datum)?;
        Ok(datums)
    }

    fn decode_datums<B, F>(buf: &mut B, datum_kind: DatumKind, mut f: F) -> Result<()>
    where
        B: Buf,
        F: FnMut(Datum) -> Result<()>,
    {
        match datum_kind {
            DatumKind::Null => Ok(()),
            DatumKind::Timestamp => todo!(),
            DatumKind::Double => {
                let with_float = |v| f(Datum::from(v));
                let decoder = F64ValuesDecoder;
                decoder.decode(buf, with_float)
            }
            DatumKind::Float => todo!(),
            DatumKind::Varbinary => {
                let with_bytes = |v| f(Datum::from(v));
                let decoder = BytesValuesDecoder::default();
                decoder.decode(buf, with_bytes)
            }
            DatumKind::String => {
                let with_str = |value| {
                    let datum = unsafe { Datum::from(StringBytes::from_bytes_unchecked(value)) };
                    f(datum)
                };
                let decoder = BytesValuesDecoder::default();
                decoder.decode(buf, with_str)
            }
            DatumKind::UInt64 => todo!(),
            DatumKind::UInt32 => todo!(),
            DatumKind::UInt16 => todo!(),
            DatumKind::UInt8 => todo!(),
            DatumKind::Int64 => {
                let with_i64 = |value: i64| {
                    let datum = Datum::from(value);
                    f(datum)
                };
                let decoder = I64ValuesDecoder;
                decoder.decode(buf, with_i64)
            }
            DatumKind::Int32 => {
                let with_i32 = |v| f(Datum::from(v));
                let decoder = I32ValuesDecoder;
                decoder.decode(buf, with_i32)
            }
            DatumKind::Int16 => todo!(),
            DatumKind::Int8 => todo!(),
            DatumKind::Boolean => todo!(),
            DatumKind::Date => todo!(),
            DatumKind::Time => todo!(),
        }
    }
}
#[cfg(test)]
mod tests {
    use super::*;

    fn check_encode_end_decode(column_idx: u32, datums: Vec<Datum>, datum_kind: DatumKind) {
        let encoder = ColumnarEncoder::new(column_idx);
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

        let decoder = ColumnarDecoder;
        let DecodeResult {
            column_idx: decoded_column_idx,
            datums: decoded_datums,
        } = decoder.decode(&mut buf.as_slice()).unwrap();
        assert_eq!(column_idx, decoded_column_idx);
        assert_eq!(datums, decoded_datums);
    }

    #[test]
    fn test_i32() {
        let datums = vec![
            Datum::from(10i32),
            Datum::from(1i32),
            Datum::from(2i32),
            Datum::from(18i32),
            Datum::from(38i32),
            Datum::from(48i32),
            Datum::from(80i32),
            Datum::from(81i32),
            Datum::from(82i32),
        ];

        check_encode_end_decode(10, datums, DatumKind::Int32);
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
    fn test_i64() {
        let datums = vec![
            Datum::from(10i64),
            Datum::from(1i64),
            Datum::from(2i64),
            Datum::from(18i64),
            Datum::from(38i64),
            Datum::from(48i64),
            Datum::from(80i64),
            Datum::from(81i64),
            Datum::from(82i64),
        ];

        check_encode_end_decode(10, datums, DatumKind::Int64);
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
}
