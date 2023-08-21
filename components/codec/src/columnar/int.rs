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
    columnar::{
        DecodeContext, Result, ValuesDecoder, ValuesDecoderImpl, ValuesEncoder, ValuesEncoderImpl,
        Varint,
    },
    varint,
};

/// The max number of the bytes used to store a varint encoding u64/i64.
const MAX_NUM_BYTES_OF_64VARINT: usize = 10;

impl ValuesEncoder<i32> for ValuesEncoderImpl {
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
}

impl ValuesDecoder<i32> for ValuesDecoderImpl {
    fn decode<B, F>(&self, _ctx: DecodeContext<'_>, buf: &mut B, mut f: F) -> Result<()>
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

impl ValuesEncoder<i64> for ValuesEncoderImpl {
    fn encode<B, I>(&self, buf: &mut B, values: I) -> Result<()>
    where
        B: BufMut,
        I: Iterator<Item = i64>,
    {
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
        num * MAX_NUM_BYTES_OF_64VARINT
    }
}

impl ValuesDecoder<i64> for ValuesDecoderImpl {
    fn decode<B, F>(&self, _ctx: DecodeContext<'_>, buf: &mut B, mut f: F) -> Result<()>
    where
        B: Buf,
        F: FnMut(i64) -> Result<()>,
    {
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
        num * MAX_NUM_BYTES_OF_64VARINT
    }
}

impl ValuesDecoder<u64> for ValuesDecoderImpl {
    fn decode<B, F>(&self, _ctx: DecodeContext<'_>, buf: &mut B, mut f: F) -> Result<()>
    where
        B: Buf,
        F: FnMut(u64) -> Result<()>,
    {
        while buf.remaining() > 0 {
            let v = varint::decode_uvarint(buf).context(Varint)?;
            f(v)?;
        }

        Ok(())
    }
}
