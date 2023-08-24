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
use common_types::row::bitset::{BitSet, OneByteBitSet, RoBitSet};
use snafu::{ensure, OptionExt};

use super::{
    DecodeContext, InvalidBooleanValue, InvalidCompression, Result, ValuesDecoder,
    ValuesDecoderImpl, ValuesEncoder, ValuesEncoderImpl,
};
use crate::columnar::{InvalidBitSetBuf, InvalidVersion, NotEnoughBytes};

/// The layout for the boolean columnar encoding:
/// ```plaintext
/// +-------------+-----------------+------------+-----------------+
/// | version(u8) | num_values(u32) | data_block | compression(u8) |
/// +-------------+-----------------+------------+-----------------+
/// ```
/// Notes:
/// - If the data_block is too long, it will be compressed as bit set.
/// - The `num_values` field is optional, and it is only needed when compression
///   is enabled.
struct Encoding;

/// The compression for [`Encoding`].
///
/// It is not allowed to be modified and only allowed to be appended with a new
/// variant.
#[derive(Clone, Copy, Default)]
#[repr(C)]
enum Compression {
    #[default]
    None = 0,
    BitSet = 1,
}

impl Encoding {
    const COMPRESSION_SIZE: usize = 1;
    /// The overhead for compression is 4B, so it is not good to always enable
    /// the compression.
    const COMPRESS_THRESHOLD: usize = 10;
    const NUM_VALUES_SIZE: usize = 4;
    const VERSION: u8 = 0;
    const VERSION_SIZE: usize = 1;

    fn need_compress(num_values: usize) -> bool {
        num_values > Self::COMPRESS_THRESHOLD
    }

    fn decode_compression(flag: u8) -> Result<Compression> {
        let compression = match flag {
            0 => Compression::None,
            1 => Compression::BitSet,
            _ => InvalidCompression { flag }.fail()?,
        };

        Ok(compression)
    }

    fn encode<B, I>(&self, buf: &mut B, values: I) -> Result<()>
    where
        B: BufMut,
        I: Iterator<Item = bool> + Clone,
    {
        buf.put_u8(Self::VERSION);

        let num_values = values.clone().count();
        if Self::need_compress(num_values) {
            Self::encode_with_compression(buf, num_values, values)
        } else {
            Self::encode_without_compression(buf, values)
        }
    }

    fn estimated_encoded_size<I>(&self, values: I) -> usize
    where
        I: Iterator<Item = bool>,
    {
        let num_values = values.count();
        if Self::need_compress(num_values) {
            BitSet::num_bytes(num_values)
                + Self::COMPRESSION_SIZE
                + Self::NUM_VALUES_SIZE
                + Self::VERSION_SIZE
        } else {
            num_values + Self::VERSION_SIZE + Self::COMPRESSION_SIZE
        }
    }

    fn decode<B, F>(&self, buf: &mut B, f: F) -> Result<()>
    where
        B: Buf,
        F: FnMut(bool) -> Result<()>,
    {
        let buf = buf.chunk();
        ensure!(
            buf.len() > Self::VERSION_SIZE + Self::COMPRESSION_SIZE,
            NotEnoughBytes { len: buf.len() }
        );

        // Decode the version.
        let version = buf[0];
        ensure!(version == Self::VERSION, InvalidVersion { version });

        // Decode the compression.
        let compression_index = buf.len() - 1;
        match Self::decode_compression(buf[compression_index])? {
            Compression::None => Self::decode_without_compression(buf, f)?,
            Compression::BitSet => Self::decode_with_compression(buf, f)?,
        }

        Ok(())
    }

    fn encode_without_compression<B, I>(buf: &mut B, values: I) -> Result<()>
    where
        B: BufMut,
        I: Iterator<Item = bool>,
    {
        for v in values {
            buf.put_u8(v as u8);
        }

        buf.put_u8(Compression::None as u8);

        Ok(())
    }

    fn decode_without_compression<F>(buf: &[u8], mut f: F) -> Result<()>
    where
        F: FnMut(bool) -> Result<()>,
    {
        let data_block_start = Self::VERSION_SIZE;
        let data_block_end = buf.len() - Self::COMPRESSION_SIZE;
        let data_block = &buf[data_block_start..data_block_end];
        for v in data_block {
            match *v {
                0 => f(false),
                1 => f(true),
                _ => InvalidBooleanValue { value: *v }.fail(),
            }?
        }

        Ok(())
    }

    fn encode_with_compression<B, I>(buf: &mut B, num_values: usize, values: I) -> Result<()>
    where
        B: BufMut,
        I: Iterator<Item = bool>,
    {
        buf.put_u32(num_values as u32);

        let mut one_byte_bits = [false; 8];
        let mut offset = 0;
        for v in values {
            one_byte_bits[offset] = v;
            offset += 1;
            if offset == 8 {
                let bit_set = OneByteBitSet::from_slice(&one_byte_bits);
                buf.put_u8(bit_set.0);

                // Reset the offset and the bits buf.
                offset = 0;
                one_byte_bits = [false; 8];
            }
        }

        // Put the remaining bits.
        if offset > 0 {
            let bit_set = OneByteBitSet::from_slice(&one_byte_bits);
            buf.put_u8(bit_set.0);
        }

        buf.put_u8(Compression::BitSet as u8);
        Ok(())
    }

    fn decode_with_compression<F>(buf: &[u8], mut f: F) -> Result<()>
    where
        F: FnMut(bool) -> Result<()>,
    {
        let expected_len = Self::VERSION_SIZE + Self::NUM_VALUES_SIZE + Self::COMPRESSION_SIZE;
        ensure!(buf.len() >= expected_len, NotEnoughBytes { len: buf.len() });

        let bit_set_start = Self::VERSION_SIZE + Self::NUM_VALUES_SIZE;
        let num_values = {
            let mut num_buf = &buf[Self::VERSION_SIZE..bit_set_start];
            num_buf.get_u32() as usize
        };

        let bit_set_end = buf.len() - Self::COMPRESSION_SIZE;
        let bit_set_buf = &buf[bit_set_start..bit_set_end];
        let bit_set = RoBitSet::try_new(bit_set_buf, num_values).context(InvalidBitSetBuf)?;

        for i in 0..num_values {
            if bit_set.is_set(i).context(InvalidBitSetBuf)? {
                f(true)
            } else {
                f(false)
            }?
        }

        Ok(())
    }
}

impl ValuesEncoder<bool> for ValuesEncoderImpl {
    fn encode<B, I>(&self, buf: &mut B, values: I) -> Result<()>
    where
        B: BufMut,
        I: Iterator<Item = bool> + Clone,
    {
        Encoding.encode(buf, values)
    }

    fn estimated_encoded_size<I>(&self, values: I) -> usize
    where
        I: Iterator<Item = bool>,
    {
        Encoding.estimated_encoded_size(values)
    }
}

impl ValuesDecoder<bool> for ValuesDecoderImpl {
    fn decode<B, F>(&self, _ctx: DecodeContext<'_>, buf: &mut B, f: F) -> Result<()>
    where
        B: Buf,
        F: FnMut(bool) -> Result<()>,
    {
        Encoding.decode(buf, f)
    }
}
