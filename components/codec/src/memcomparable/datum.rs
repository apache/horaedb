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

//! Datum comparable codec

use std::i64;

use bytes_ext::{Buf, BufMut, BytesMut, SafeBufMut};
use common_types::{
    datum::{Datum, DatumKind},
    string::StringBytes,
    time::Timestamp,
};
use snafu::ResultExt;

use crate::{
    consts,
    memcomparable::{EncodeKey, Error, MemComparable, Result, UnsupportedKind},
    DecodeTo, Encoder,
};

// TODO(yingwen): Consider collate for string.
impl Encoder<Datum> for MemComparable {
    type Error = Error;

    fn encode<B: BufMut>(&self, buf: &mut B, value: &Datum) -> Result<()> {
        match value {
            Datum::Null => buf.try_put_u8(consts::NULL_FLAG).context(EncodeKey),
            Datum::Timestamp(ts) => {
                buf.try_put_u8(consts::INT_FLAG).context(EncodeKey)?;
                self.encode(buf, &ts.as_i64())
            }
            Datum::Varbinary(v) => {
                buf.try_put_u8(consts::BYTES_FLAG).context(EncodeKey)?;
                self.encode(buf, v)
            }
            // For string, we just use same encoding method as bytes now.
            Datum::String(v) => {
                buf.try_put_u8(consts::BYTES_FLAG).context(EncodeKey)?;
                self.encode(buf, v.as_bytes())
            }
            Datum::UInt64(v) => {
                buf.try_put_u8(consts::UINT_FLAG).context(EncodeKey)?;
                self.encode(buf, v)
            }
            Datum::UInt32(v) => {
                buf.try_put_u8(consts::UINT_FLAG).context(EncodeKey)?;
                self.encode(buf, &(u64::from(*v)))
            }
            Datum::UInt16(v) => {
                buf.try_put_u8(consts::UINT_FLAG).context(EncodeKey)?;
                self.encode(buf, &(u64::from(*v)))
            }
            Datum::UInt8(v) => {
                buf.try_put_u8(consts::UINT_FLAG).context(EncodeKey)?;
                self.encode(buf, &(u64::from(*v)))
            }
            Datum::Int64(v) => {
                buf.try_put_u8(consts::INT_FLAG).context(EncodeKey)?;
                self.encode(buf, v)
            }
            Datum::Int32(v) => {
                buf.try_put_u8(consts::INT_FLAG).context(EncodeKey)?;
                self.encode(buf, &(i64::from(*v)))
            }
            Datum::Int16(v) => {
                buf.try_put_u8(consts::INT_FLAG).context(EncodeKey)?;
                self.encode(buf, &(i64::from(*v)))
            }
            Datum::Int8(v) => {
                buf.try_put_u8(consts::INT_FLAG).context(EncodeKey)?;
                self.encode(buf, &(i64::from(*v)))
            }
            Datum::Boolean(v) => {
                buf.try_put_u8(consts::UINT_FLAG).context(EncodeKey)?;
                self.encode(buf, &(u64::from(*v)))
            }
            Datum::Date(v) => {
                buf.try_put_u8(consts::INT_FLAG).context(EncodeKey)?;
                self.encode(buf, &(i64::from(*v)))
            }
            Datum::Time(v) => {
                buf.try_put_u8(consts::INT_FLAG).context(EncodeKey)?;
                self.encode(buf, v)
            }
            Datum::Double(_) => UnsupportedKind {
                kind: DatumKind::Double,
            }
            .fail(),
            Datum::Float(_) => UnsupportedKind {
                kind: DatumKind::Float,
            }
            .fail(),
        }
    }

    fn estimate_encoded_size(&self, value: &Datum) -> usize {
        match value {
            // Null takes 1 byte
            Datum::Null => 1,
            Datum::Timestamp(ts) => self.estimate_encoded_size(&ts.as_i64()),
            Datum::Varbinary(v) => self.estimate_encoded_size(v),
            Datum::String(v) => self.estimate_encoded_size(v.as_bytes()),
            Datum::UInt64(v) => self.estimate_encoded_size(v),
            Datum::UInt32(v) => self.estimate_encoded_size(&(u64::from(*v))),
            Datum::UInt16(v) => self.estimate_encoded_size(&(u64::from(*v))),
            Datum::UInt8(v) => self.estimate_encoded_size(&(u64::from(*v))),
            Datum::Int64(v) => self.estimate_encoded_size(v),
            Datum::Int32(v) => self.estimate_encoded_size(&(i64::from(*v))),
            Datum::Date(v) => self.estimate_encoded_size(&(i64::from(*v))),
            Datum::Time(v) => self.estimate_encoded_size(v),
            Datum::Int16(v) => self.estimate_encoded_size(&(i64::from(*v))),
            Datum::Int8(v) => self.estimate_encoded_size(&(i64::from(*v))),
            Datum::Boolean(v) => self.estimate_encoded_size(&(u64::from(*v))),
            // Unsupported kind, but we return 1
            Datum::Double(_) | Datum::Float(_) => 1,
        }
    }
}

macro_rules! decode_u64_into {
    ($self: ident, $v: ident, $buf: ident, $type: ty) => {{
        Self::ensure_flag($buf, consts::UINT_FLAG)?;
        let mut data = 0u64;
        $self.decode_to($buf, &mut data)?;
        *$v = data as $type;
    }};
}

macro_rules! decode_u64_into_bool {
    ($self: ident, $v: ident, $buf: ident) => {{
        Self::ensure_flag($buf, consts::UINT_FLAG)?;
        let mut data = 0u64;
        $self.decode_to($buf, &mut data)?;
        *$v = data != 0;
    }};
}

macro_rules! decode_i64_into {
    ($self: ident, $v: ident, $buf: ident, $type: ty) => {{
        Self::ensure_flag($buf, consts::INT_FLAG)?;
        let mut data = 0i64;
        $self.decode_to($buf, &mut data)?;
        *$v = data as $type;
    }};
}

impl DecodeTo<Datum> for MemComparable {
    type Error = Error;

    /// REQUIRE: The datum type should match the type in buf
    ///
    /// For string datum, the utf8 check will be skipped.
    fn decode_to<B: Buf>(&self, buf: &mut B, value: &mut Datum) -> Result<()> {
        match value {
            Datum::Null => {
                Self::ensure_flag(buf, consts::NULL_FLAG)?;
            }
            Datum::Timestamp(ts) => {
                Self::ensure_flag(buf, consts::INT_FLAG)?;
                let mut data = 0;
                self.decode_to(buf, &mut data)?;
                *ts = Timestamp::new(data);
            }
            Datum::Varbinary(v) => {
                Self::ensure_flag(buf, consts::BYTES_FLAG)?;
                let mut data = BytesMut::new();
                self.decode_to(buf, &mut data)?;
                *v = data.freeze();
            }
            Datum::String(v) => {
                Self::ensure_flag(buf, consts::BYTES_FLAG)?;
                let mut data = BytesMut::new();
                self.decode_to(buf, &mut data)?;
                // For string datum, we won't validate whether the bytes is a valid utf string
                // during decoding to improve decode performance. The encoder
                // should already done the utf8 check.
                unsafe {
                    *v = StringBytes::from_bytes_unchecked(data.freeze());
                }
            }
            Datum::UInt64(v) => {
                Self::ensure_flag(buf, consts::UINT_FLAG)?;
                self.decode_to(buf, v)?;
            }
            Datum::UInt32(v) => decode_u64_into!(self, v, buf, u32),
            Datum::UInt16(v) => decode_u64_into!(self, v, buf, u16),
            Datum::UInt8(v) => decode_u64_into!(self, v, buf, u8),
            Datum::Int64(v) => {
                Self::ensure_flag(buf, consts::INT_FLAG)?;
                self.decode_to(buf, v)?;
            }
            Datum::Int32(v) => decode_i64_into!(self, v, buf, i32),
            Datum::Date(v) => decode_i64_into!(self, v, buf, i32),
            Datum::Time(v) => {
                Self::ensure_flag(buf, consts::INT_FLAG)?;
                self.decode_to(buf, v)?;
            }
            Datum::Int16(v) => decode_i64_into!(self, v, buf, i16),
            Datum::Int8(v) => decode_i64_into!(self, v, buf, i8),
            Datum::Boolean(v) => decode_u64_into_bool!(self, v, buf),
            Datum::Double(_) => {
                return UnsupportedKind {
                    kind: DatumKind::Double,
                }
                .fail();
            }
            Datum::Float(_) => {
                return UnsupportedKind {
                    kind: DatumKind::Float,
                }
                .fail();
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use core::cmp::Ordering;

    use bytes_ext::Bytes;

    use super::*;

    #[test]
    fn test_datum_codec() {
        let data = vec![
            // (datum to encode, estimate_encoded_size)
            (Datum::Null, 1),
            (Datum::Timestamp(Timestamp::new(12345)), 9),
            (Datum::Varbinary(Bytes::from_static(b"hello world")), 18),
            (Datum::String(StringBytes::from_static("hello world")), 18),
            (Datum::UInt64(100209), 9),
            (Datum::UInt32(10020), 9),
            (Datum::UInt16(65000), 9),
            (Datum::UInt8(150), 9),
            (Datum::Int64(-100209), 9),
            (Datum::Int32(-10020), 9),
            (Datum::Int16(32500), 9),
            (Datum::Int8(-120), 9),
            (Datum::Boolean(true), 9),
            (Datum::Boolean(false), 9),
            (Datum::Date(1000), 9),
            (Datum::Time(100000000), 9),
        ];
        let mut decoded = vec![
            Datum::Null,
            Datum::Timestamp(Timestamp::new(0)),
            Datum::Varbinary(Bytes::new()),
            Datum::String(StringBytes::new()),
            Datum::UInt64(0),
            Datum::UInt32(0),
            Datum::UInt16(0),
            Datum::UInt8(0),
            Datum::Int64(0),
            Datum::Int32(0),
            Datum::Int16(0),
            Datum::Int8(0),
            Datum::Boolean(false),
            Datum::Boolean(false),
            Datum::Date(0),
            Datum::Time(0),
        ];
        let c = MemComparable;
        for (index, x) in data.iter().enumerate() {
            let mut buf = vec![];
            c.encode(&mut buf, &x.0).unwrap();
            assert_eq!(x.1, c.estimate_encoded_size(&x.0));
            c.decode_to(&mut buf.as_slice(), &mut decoded[index])
                .unwrap();
            assert_eq!(decoded[index], data[index].0);
        }
    }

    #[test]
    fn test_datum_order() {
        let data = vec![
            // (arg1, arg2, cmp order of arg1 and arg2)
            (Datum::Null, Datum::Null, Ordering::Equal),
            (
                Datum::Timestamp(Timestamp::new(12345)),
                Datum::Timestamp(Timestamp::new(123456)),
                Ordering::Less,
            ),
            (
                Datum::Varbinary(Bytes::from_static(&[
                    0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07,
                ])),
                Datum::Varbinary(Bytes::from_static(&[
                    0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08,
                ])),
                Ordering::Less,
            ),
            (
                Datum::String(StringBytes::from_static("abce123")),
                Datum::String(StringBytes::from_static("abce1234")),
                Ordering::Less,
            ),
            (Datum::UInt64(888), Datum::UInt64(889), Ordering::Less),
            (Datum::Date(1000), Datum::Date(2000), Ordering::Less),
            (
                Datum::Time(2000000000),
                Datum::Time(1000000000),
                Ordering::Greater,
            ),
        ];
        let c = MemComparable;
        for x in &data {
            let mut buf1 = vec![];
            let mut buf2 = vec![];
            c.encode(&mut buf1, &x.0).unwrap();
            c.encode(&mut buf2, &x.1).unwrap();
            assert_eq!(x.2, buf1.as_slice().cmp(buf2.as_slice()));
        }
    }
}
