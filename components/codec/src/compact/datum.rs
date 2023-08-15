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

//! Datum compact codec

use bytes_ext::{Buf, BufMut, BytesMut, SafeBufMut};
use common_types::{datum::Datum, string::StringBytes, time::Timestamp};
use snafu::ResultExt;

use crate::{
    compact::{EncodeKey, Error, MemCompactDecoder, MemCompactEncoder, Result},
    consts, DecodeTo, Encoder,
};

// For float points, we use same encoding as mem comparable encoder
impl Encoder<Datum> for MemCompactEncoder {
    type Error = Error;

    fn encode<B: BufMut>(&self, buf: &mut B, value: &Datum) -> Result<()> {
        match value {
            Datum::Null => buf.try_put_u8(consts::NULL_FLAG).context(EncodeKey),
            Datum::Timestamp(ts) => {
                buf.try_put_u8(consts::VARINT_FLAG).context(EncodeKey)?;
                self.encode(buf, &ts.as_i64())
            }
            Datum::Double(v) => {
                buf.try_put_u8(consts::FLOAT_FLAG).context(EncodeKey)?;
                self.encode(buf, v)
            }
            Datum::Float(v) => {
                buf.try_put_u8(consts::FLOAT_FLAG).context(EncodeKey)?;
                self.encode(buf, v)
            }
            Datum::Varbinary(v) => {
                buf.try_put_u8(consts::COMPACT_BYTES_FLAG)
                    .context(EncodeKey)?;
                self.encode(buf, v)
            }
            // For string, just encode/decode like bytes.
            Datum::String(v) => {
                buf.try_put_u8(consts::COMPACT_BYTES_FLAG)
                    .context(EncodeKey)?;
                self.encode(buf, v.as_bytes())
            }
            Datum::UInt64(v) => {
                buf.try_put_u8(consts::UVARINT_FLAG).context(EncodeKey)?;
                self.encode(buf, v)
            }
            Datum::UInt32(v) => {
                buf.try_put_u8(consts::UVARINT_FLAG).context(EncodeKey)?;
                self.encode(buf, &(u64::from(*v)))
            }
            Datum::UInt16(v) => {
                buf.try_put_u8(consts::UVARINT_FLAG).context(EncodeKey)?;
                self.encode(buf, &(u64::from(*v)))
            }
            Datum::UInt8(v) => {
                buf.try_put_u8(consts::UVARINT_FLAG).context(EncodeKey)?;
                self.encode(buf, &(u64::from(*v)))
            }
            Datum::Int64(v) => {
                buf.try_put_u8(consts::VARINT_FLAG).context(EncodeKey)?;
                self.encode(buf, v)
            }
            Datum::Int32(v) => {
                buf.try_put_u8(consts::VARINT_FLAG).context(EncodeKey)?;
                self.encode(buf, &(i64::from(*v)))
            }
            Datum::Int16(v) => {
                buf.try_put_u8(consts::VARINT_FLAG).context(EncodeKey)?;
                self.encode(buf, &(i64::from(*v)))
            }
            Datum::Int8(v) => {
                buf.try_put_u8(consts::VARINT_FLAG).context(EncodeKey)?;
                self.encode(buf, &(i64::from(*v)))
            }
            Datum::Boolean(v) => {
                buf.try_put_u8(consts::UVARINT_FLAG).context(EncodeKey)?;
                self.encode(buf, &(u64::from(*v)))
            }
            Datum::Date(v) => {
                buf.try_put_u8(consts::VARINT_FLAG).context(EncodeKey)?;
                self.encode(buf, &(i64::from(*v)))
            }
            Datum::Time(v) => {
                buf.try_put_u8(consts::VARINT_FLAG).context(EncodeKey)?;
                self.encode(buf, v)
            }
        }
    }

    fn estimate_encoded_size(&self, value: &Datum) -> usize {
        match value {
            // Null takes 1 byte
            Datum::Null => 1,
            Datum::Timestamp(ts) => self.estimate_encoded_size(&ts.as_i64()),
            Datum::Double(v) => self.estimate_encoded_size(v),
            Datum::Float(v) => self.estimate_encoded_size(v),
            Datum::Varbinary(v) => self.estimate_encoded_size(v),
            Datum::String(v) => self.estimate_encoded_size(v.as_bytes()),
            Datum::UInt64(v) => self.estimate_encoded_size(v),
            Datum::UInt32(v) => self.estimate_encoded_size(&(u64::from(*v))),
            Datum::UInt16(v) => self.estimate_encoded_size(&(u64::from(*v))),
            Datum::UInt8(v) => self.estimate_encoded_size(&(u64::from(*v))),
            Datum::Int64(v) => self.estimate_encoded_size(v),
            Datum::Int32(v) => self.estimate_encoded_size(&(i64::from(*v))),
            Datum::Int16(v) => self.estimate_encoded_size(&(i64::from(*v))),
            Datum::Int8(v) => self.estimate_encoded_size(&(i64::from(*v))),
            Datum::Boolean(v) => self.estimate_encoded_size(&(u64::from(*v))),
            Datum::Date(v) => self.estimate_encoded_size(&(i64::from(*v))),
            Datum::Time(v) => self.estimate_encoded_size(v),
        }
    }
}

macro_rules! decode_var_u64_into {
    ($self: ident, $v: ident, $actual: ident, $buf: ident, $type: ty) => {{
        Self::ensure_flag(consts::UVARINT_FLAG, $actual)?;
        let mut data = 0u64;
        $self.decode_to($buf, &mut data)?;
        *$v = data as $type;
    }};
}

macro_rules! decode_var_u64_into_bool {
    ($self: ident, $v: ident, $actual: ident, $buf: ident) => {{
        Self::ensure_flag(consts::UVARINT_FLAG, $actual)?;
        let mut data = 0u64;
        $self.decode_to($buf, &mut data)?;
        *$v = data != 0;
    }};
}

macro_rules! decode_var_i64_into {
    ($self: ident, $v: ident, $actual: ident, $buf: ident, $type: ty) => {{
        Self::ensure_flag(consts::VARINT_FLAG, $actual)?;
        let mut data = 0i64;
        $self.decode_to($buf, &mut data)?;
        *$v = data as $type;
    }};
}

impl DecodeTo<Datum> for MemCompactDecoder {
    type Error = Error;

    /// REQUIRE: The datum type should match the type in buf
    ///
    /// For string datum, the utf8 check will be skipped.
    fn decode_to<B: Buf>(&self, buf: &mut B, value: &mut Datum) -> Result<()> {
        let actual = match self.maybe_read_null(buf)? {
            Some(v) => v,
            None => {
                *value = Datum::Null;
                return Ok(());
            }
        };

        match value {
            Datum::Null => {
                Self::ensure_flag(consts::NULL_FLAG, actual)?;
            }
            Datum::Timestamp(ts) => {
                Self::ensure_flag(consts::VARINT_FLAG, actual)?;
                let mut data = 0;
                self.decode_to(buf, &mut data)?;
                *ts = Timestamp::new(data);
            }
            Datum::Double(v) => {
                Self::ensure_flag(consts::FLOAT_FLAG, actual)?;
                self.decode_to(buf, v)?;
            }
            Datum::Float(v) => {
                Self::ensure_flag(consts::FLOAT_FLAG, actual)?;
                self.decode_to(buf, v)?;
            }
            Datum::Varbinary(v) => {
                Self::ensure_flag(consts::COMPACT_BYTES_FLAG, actual)?;
                let mut data = BytesMut::new();
                self.decode_to(buf, &mut data)?;
                *v = data.freeze();
            }
            Datum::String(v) => {
                Self::ensure_flag(consts::COMPACT_BYTES_FLAG, actual)?;
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
                Self::ensure_flag(consts::UVARINT_FLAG, actual)?;
                self.decode_to(buf, v)?;
            }
            Datum::UInt32(v) => decode_var_u64_into!(self, v, actual, buf, u32),
            Datum::UInt16(v) => decode_var_u64_into!(self, v, actual, buf, u16),
            Datum::UInt8(v) => decode_var_u64_into!(self, v, actual, buf, u8),
            Datum::Int64(v) => {
                Self::ensure_flag(consts::VARINT_FLAG, actual)?;
                self.decode_to(buf, v)?;
            }
            Datum::Int32(v) => decode_var_i64_into!(self, v, actual, buf, i32),
            Datum::Int16(v) => decode_var_i64_into!(self, v, actual, buf, i16),
            Datum::Int8(v) => decode_var_i64_into!(self, v, actual, buf, i8),
            Datum::Boolean(v) => decode_var_u64_into_bool!(self, v, actual, buf),
            Datum::Date(v) => decode_var_i64_into!(self, v, actual, buf, i32),
            Datum::Time(v) => {
                Self::ensure_flag(consts::VARINT_FLAG, actual)?;
                self.decode_to(buf, v)?;
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use bytes_ext::Bytes;

    use super::*;

    // TODO(yingwen): Test nullable.
    #[test]
    fn test_datum_codec() {
        let data = vec![
            // (datum to encode, estimate_encoded_size)
            (Datum::Null, 1),
            (Datum::Timestamp(Timestamp::new(12345)), 10),
            (Datum::Double(10.5), 8),
            (Datum::Float(1.99), 4),
            (Datum::Varbinary(Bytes::from_static(b"hello world")), 21),
            (Datum::String(StringBytes::from_static("hello world")), 21),
            (Datum::UInt64(12345), 10),
            (Datum::UInt32(1000), 10),
            (Datum::UInt16(65000), 10),
            (Datum::UInt8(150), 10),
            (Datum::Int64(-100209), 10),
            (Datum::Int32(-10020), 10),
            (Datum::Int16(32500), 10),
            (Datum::Int8(-120), 10),
            (Datum::Boolean(true), 10),
            (Datum::Boolean(false), 10),
            (Datum::Date(1000), 10),
            (Datum::Time(1_000_000_000), 10),
        ];
        let mut decoded = vec![
            Datum::Null,
            Datum::Timestamp(Timestamp::new(0)),
            Datum::Double(0.0),
            Datum::Float(0.0),
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
        let encoder = MemCompactEncoder;
        let decoder = MemCompactDecoder;
        for (index, x) in data.iter().enumerate() {
            let mut buf = vec![];
            encoder.encode(&mut buf, &x.0).unwrap();
            assert_eq!(x.1, encoder.estimate_encoded_size(&x.0));
            decoder
                .decode_to(&mut buf.as_slice(), &mut decoded[index])
                .unwrap();
            assert_eq!(decoded[index], data[index].0);
        }
    }
}
