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
use snafu::ResultExt;

use crate::{
    columnar::{Result, ValuesDecoder, ValuesEncoder, Varint},
    varint,
};

pub struct I32ValuesEncoder;

impl ValuesEncoder for I32ValuesEncoder {
    type ValueType = i32;

    fn encode<B, I>(&self, buf: &mut B, values: I) -> Result<()>
    where
        B: BufMut,
        I: Iterator<Item = i32>,
    {
        for v in values {
            buf.put_i32(v);
        }

        Ok(())
    }

    fn estimated_encoded_size<I>(&self, values: I) -> usize
    where
        I: Iterator<Item = i32>,
    {
        let (lower, higher) = values.size_hint();
        let num = lower.max(higher.unwrap_or_default());
        num * std::mem::size_of::<Self::ValueType>()
    }
}

pub struct I32ValuesDecoder;

impl ValuesDecoder for I32ValuesDecoder {
    type ValueType = i32;

    fn decode<B, F>(&self, buf: &mut B, mut f: F) -> Result<()>
    where
        B: Buf,
        F: FnMut(i32) -> Result<()>,
    {
        while buf.remaining() > 0 {
            let v = buf.get_i32();
            f(v)?;
        }

        Ok(())
    }
}

pub struct I64ValuesEncoder;

impl ValuesEncoder for I64ValuesEncoder {
    type ValueType = i64;

    fn encode<B, I>(&self, buf: &mut B, values: I) -> Result<()>
    where
        B: BufMut,
        I: Iterator<Item = Self::ValueType>,
    {
        for v in values {
            varint::encode_varint(buf, v).context(Varint)?;
        }

        Ok(())
    }

    fn estimated_encoded_size<I>(&self, values: I) -> usize
    where
        I: Iterator<Item = Self::ValueType>,
    {
        let (lower, higher) = values.size_hint();
        let num = lower.max(higher.unwrap_or_default());
        num * 10
    }
}

pub struct I64ValuesDecoder;

impl ValuesDecoder for I64ValuesDecoder {
    type ValueType = i64;

    fn decode<B, F>(&self, buf: &mut B, mut f: F) -> Result<()>
    where
        B: Buf,
        F: FnMut(Self::ValueType) -> Result<()>,
    {
        while buf.remaining() > 0 {
            let v = varint::decode_varint(buf).context(Varint)?;
            f(v)?;
        }

        Ok(())
    }
}

pub struct U64ValuesEncoder;

impl ValuesEncoder for U64ValuesEncoder {
    type ValueType = u64;

    fn encode<B, I>(&self, buf: &mut B, values: I) -> Result<()>
    where
        B: BufMut,
        I: Iterator<Item = Self::ValueType>,
    {
        for v in values {
            varint::encode_uvarint(buf, v).context(Varint)?;
        }

        Ok(())
    }

    fn estimated_encoded_size<I>(&self, values: I) -> usize
    where
        I: Iterator<Item = Self::ValueType>,
    {
        let (lower, higher) = values.size_hint();
        let num = lower.max(higher.unwrap_or_default());
        num * 10
    }
}

pub struct U64ValuesDecoder;

impl ValuesDecoder for U64ValuesDecoder {
    type ValueType = u64;

    fn decode<B, F>(&self, buf: &mut B, mut f: F) -> Result<()>
    where
        B: Buf,
        F: FnMut(Self::ValueType) -> Result<()>,
    {
        while buf.remaining() > 0 {
            let v = varint::decode_uvarint(buf).context(Varint)?;
            f(v)?;
        }

        Ok(())
    }
}
