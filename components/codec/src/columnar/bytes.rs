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

use bytes_ext::{Buf, BufMut, Bytes};
use snafu::ResultExt;

use crate::{
    columnar::{
        Result, ValuesDecoder, ValuesDecoderImpl, ValuesEncoder, ValuesEncoderImpl, Varint,
    },
    varint,
};

impl<'a> ValuesEncoder<&'a [u8]> for ValuesEncoderImpl {
    fn encode<B, I>(&self, buf: &mut B, values: I) -> Result<()>
    where
        B: BufMut,
        I: Iterator<Item = &'a [u8]>,
    {
        for v in values {
            debug_assert!(v.len() < u32::MAX as usize);

            varint::encode_uvarint(buf, v.len() as u64).context(Varint)?;
            buf.put_slice(v);
        }

        Ok(())
    }

    fn estimated_encoded_size<I>(&self, values: I) -> usize
    where
        I: Iterator<Item = &'a [u8]>,
    {
        let mut total_bytes = 0;
        for v in values {
            // The length of `v` should be ensured to be smaller than [u32::MAX], that is to
            // say, at most 5 bytes will be used when do varint encoding over a u32 number.
            total_bytes += 5 + v.len();
        }
        total_bytes
    }
}

impl ValuesDecoder<Bytes> for ValuesDecoderImpl {
    fn decode<B, F>(&self, buf: &mut B, mut f: F) -> Result<()>
    where
        B: Buf,
        F: FnMut(Bytes) -> Result<()>,
    {
        while buf.remaining() > 0 {
            let str_len = varint::decode_uvarint(buf).context(Varint)? as usize;
            let v = &buf.chunk()[..str_len];
            f(Bytes::copy_from_slice(v))?;
            buf.advance(str_len);
        }

        Ok(())
    }
}
