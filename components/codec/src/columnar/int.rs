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

use bytes_ext::{Buf, BufMut};
use snafu::{ensure, ResultExt};

use crate::{
    columnar::{
        DecodeContext, InvalidVersion, Result, ValuesDecoder, ValuesDecoderImpl, ValuesEncoder,
        ValuesEncoderImpl, Varint,
    },
    varint,
};

/// The max number of the bytes used to store a varint encoding u64/i64.
const MAX_NUM_BYTES_OF_64VARINT: usize = 10;
const VERSION: u8 = 0;
const VERSION_SIZE: usize = 1;

macro_rules! impl_int_encoding {
    ($int_type: ty, $write_method: ident, $read_method: ident) => {
        impl ValuesEncoder<$int_type> for ValuesEncoderImpl {
            fn encode<B, I>(&self, buf: &mut B, values: I) -> Result<()>
            where
                B: BufMut,
                I: Iterator<Item = $int_type>,
            {
                for v in values {
                    buf.$write_method(v);
                }

                Ok(())
            }
        }

        impl ValuesDecoder<$int_type> for ValuesDecoderImpl {
            fn decode<B, F>(&self, _ctx: DecodeContext<'_>, buf: &mut B, mut f: F) -> Result<()>
            where
                B: Buf,
                F: FnMut($int_type) -> Result<()>,
            {
                while buf.remaining() > 0 {
                    let v = buf.$read_method();
                    f(v)?;
                }

                Ok(())
            }
        }
    };
}

impl_int_encoding!(i8, put_i8, get_i8);
impl_int_encoding!(u8, put_u8, get_u8);
impl_int_encoding!(u16, put_u16, get_u16);
impl_int_encoding!(i16, put_i16, get_i16);
impl_int_encoding!(u32, put_u32, get_u32);
impl_int_encoding!(i32, put_i32, get_i32);

impl ValuesEncoder<i64> for ValuesEncoderImpl {
    fn encode<B, I>(&self, buf: &mut B, values: I) -> Result<()>
    where
        B: BufMut,
        I: Iterator<Item = i64>,
    {
        buf.put_u8(VERSION);
        for v in values {
            varint::encode_varint(buf, v).context(Varint)?;
        }

        Ok(())
    }

    fn estimated_encoded_size<I>(&self, values: I) -> usize
    where
        I: Iterator<Item = i64>,
    {
        let (lower, higher) = values.size_hint();
        let num = lower.max(higher.unwrap_or_default());
        num * MAX_NUM_BYTES_OF_64VARINT + VERSION_SIZE
    }
}

impl ValuesDecoder<i64> for ValuesDecoderImpl {
    fn decode<B, F>(&self, _ctx: DecodeContext<'_>, buf: &mut B, mut f: F) -> Result<()>
    where
        B: Buf,
        F: FnMut(i64) -> Result<()>,
    {
        let version = buf.get_u8();
        ensure!(version == VERSION, InvalidVersion { version });

        while buf.remaining() > 0 {
            let v = varint::decode_varint(buf).context(Varint)?;
            f(v)?;
        }

        Ok(())
    }
}

impl ValuesEncoder<u64> for ValuesEncoderImpl {
    fn encode<B, I>(&self, buf: &mut B, values: I) -> Result<()>
    where
        B: BufMut,
        I: Iterator<Item = u64>,
    {
        for v in values {
            varint::encode_uvarint(buf, v).context(Varint)?;
        }

        Ok(())
    }

    fn estimated_encoded_size<I>(&self, values: I) -> usize
    where
        I: Iterator<Item = u64>,
    {
        let (lower, higher) = values.size_hint();
        let num = lower.max(higher.unwrap_or_default());
        num * MAX_NUM_BYTES_OF_64VARINT + VERSION_SIZE
    }
}

impl ValuesDecoder<u64> for ValuesDecoderImpl {
    fn decode<B, F>(&self, _ctx: DecodeContext<'_>, buf: &mut B, mut f: F) -> Result<()>
    where
        B: Buf,
        F: FnMut(u64) -> Result<()>,
    {
        let version = buf.get_u8();
        ensure!(version == VERSION, InvalidVersion { version });

        while buf.remaining() > 0 {
            let v = varint::decode_uvarint(buf).context(Varint)?;
            f(v)?;
        }

        Ok(())
    }
}
