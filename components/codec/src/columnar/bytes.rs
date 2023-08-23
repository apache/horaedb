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

use std::io::{Read, Write};

use bytes_ext::{Buf, BufMut, Bytes, WriterOnBufMut};
use lz4_flex::frame::{FrameDecoder as Lz4Decoder, FrameEncoder as Lz4Encoder};
use snafu::{ensure, ResultExt};

use crate::{
    columnar::{
        Compress, DecodeContext, Decompress, InvalidCompression, InvalidVersion, NotEnoughBytes,
        Result, ValuesDecoder, ValuesDecoderImpl, ValuesEncoder, ValuesEncoderImpl, Varint,
    },
    varint,
};

/// The layout for the string/bytes:
/// ```plaintext
/// +-------------+--------------+------------+-----------------------+-----------------+
/// | version(u8) | length_block | data_block | length_block_len(u32) | compression(u8) |
/// +-------------+--------------+------------+-----------------------+-----------------+
/// ```
///
/// Currently, the `compression` has two optional values:
/// - 0: No compression over the data block
/// - 1: the data block will be compressed if it is too long
///
/// And the lengths in the `length block` are encoded in varint.
/// And the reason to put `length_block_len` and `compression` at the footer is
/// to avoid one more loop when encoding.
struct Encoding;

impl Encoding {
    const COMPRESSION_SIZE: usize = 1;
    const LENGTH_BLOCK_LEN_SIZE: usize = 4;
    const VERSION: u8 = 0;
    const VERSION_SIZE: usize = 1;

    fn decide_compression(data_block_len: usize, threshold: usize) -> Compression {
        if data_block_len > threshold {
            Compression::Lz4
        } else {
            Compression::NoCompression
        }
    }

    fn decode_compression(&self, v: u8) -> Result<Compression> {
        let version = match v {
            0 => Compression::NoCompression,
            1 => Compression::Lz4,
            _ => InvalidCompression { flag: v }.fail()?,
        };

        Ok(version)
    }

    fn encode<'a, B, I>(
        &self,
        buf: &mut B,
        values: I,
        data_block_compress_threshold: usize,
    ) -> Result<()>
    where
        B: BufMut,
        I: Iterator<Item = &'a [u8]> + Clone,
    {
        // Encode the `version`.
        buf.put_u8(Self::VERSION);

        // Encode the `length_block`.
        let mut data_block_len = 0;
        let mut length_block_len = 0;
        for v in values.clone() {
            data_block_len += v.len();
            let sz = varint::encode_uvarint(buf, v.len() as u64).context(Varint)?;
            length_block_len += sz;
        }
        assert!(length_block_len < u32::MAX as usize);

        // Encode the `data_block`.
        let compression = Self::decide_compression(data_block_len, data_block_compress_threshold);
        match compression {
            Compression::NoCompression => {
                for v in values {
                    buf.put_slice(v);
                }
            }
            Compression::Lz4 => self
                .encode_with_compression(buf, values)
                .context(Compress)?,
        }

        // Encode the `data_block` offset.
        buf.put_u32(length_block_len as u32);
        buf.put_u8(compression as u8);

        Ok(())
    }

    fn estimated_encoded_size<'a, I>(&self, values: I) -> usize
    where
        I: Iterator<Item = &'a [u8]>,
    {
        let mut total_bytes =
            Self::VERSION_SIZE + Self::LENGTH_BLOCK_LEN_SIZE + Self::COMPRESSION_SIZE;

        for v in values {
            // The length of `v` should be ensured to be smaller than [u32::MAX], that is to
            // say, at most 5 bytes will be used when do varint encoding over a u32 number.
            total_bytes += 5 + v.len();
        }
        total_bytes
    }

    /// The layout can be referred to the docs of [`Encoding`].
    fn decode<B, F>(&self, ctx: DecodeContext<'_>, buf: &mut B, f: F) -> Result<()>
    where
        B: Buf,
        F: FnMut(Bytes) -> Result<()>,
    {
        let chunk = buf.chunk();
        let footer_len = Self::LENGTH_BLOCK_LEN_SIZE + Self::COMPRESSION_SIZE;
        ensure!(
            chunk.len() > footer_len + Self::VERSION_SIZE,
            NotEnoughBytes {
                len: footer_len + Self::VERSION_SIZE
            }
        );

        // Read and check the version.
        let version = chunk[0];
        ensure!(version == Self::VERSION, InvalidVersion { version });

        // Read and decode the compression flag.
        let compression_offset = chunk.len() - Self::COMPRESSION_SIZE;
        let compression = self.decode_compression(chunk[compression_offset])?;

        // Extract the `length_block` and `data_block` for decoding.
        let length_block_len_offset = chunk.len() - footer_len;
        let length_block_end = {
            let mut len_buf = &chunk[length_block_len_offset..compression_offset];
            len_buf.get_u32() as usize + Self::VERSION_SIZE
        };
        let mut length_block = &chunk[Self::VERSION_SIZE..length_block_end];
        let data_block = &chunk[length_block_end..length_block_len_offset];

        match compression {
            Compression::NoCompression => {
                self.decode_without_compression(&mut length_block, data_block, f)
            }
            Compression::Lz4 => self.decode_with_compression(length_block, data_block, ctx.buf, f),
        }
    }

    /// Encode the values into the `buf`, and the compress the encoded payload.
    fn encode_with_compression<'a, B, I>(&self, buf: &mut B, values: I) -> std::io::Result<()>
    where
        B: BufMut,
        I: Iterator<Item = &'a [u8]>,
    {
        let writer = WriterOnBufMut { buf };
        let mut enc = Lz4Encoder::new(writer);
        for v in values {
            enc.write_all(v)?;
        }
        enc.finish()?;

        Ok(())
    }

    /// Decode the uncompressed data block.
    fn decode_without_compression<B, F>(
        &self,
        length_block_buf: &mut B,
        data_block_buf: &[u8],
        mut f: F,
    ) -> Result<()>
    where
        B: Buf,
        F: FnMut(Bytes) -> Result<()>,
    {
        let mut offset = 0;
        while length_block_buf.remaining() > 0 {
            let length = varint::decode_uvarint(length_block_buf).context(Varint)? as usize;
            let b = Bytes::copy_from_slice(&data_block_buf[offset..offset + length]);
            f(b)?;
            offset += length;
        }

        Ok(())
    }

    /// Decode the compressed data block.
    fn decode_with_compression<F>(
        &self,
        mut length_block_buf: &[u8],
        compressed_data_block_buf: &[u8],
        reused_buf: &mut Vec<u8>,
        f: F,
    ) -> Result<()>
    where
        F: FnMut(Bytes) -> Result<()>,
    {
        let mut decoder = Lz4Decoder::new(compressed_data_block_buf);
        decoder.read_to_end(reused_buf).context(Decompress)?;
        self.decode_without_compression(&mut length_block_buf, &reused_buf[..], f)
    }
}

/// The compression for [`Encoding`].
///
/// It is not allowed to be modified and only allowed to be appended with a new
/// variant.
#[derive(Clone, Copy, Default)]
#[repr(C)]
enum Compression {
    #[default]
    NoCompression = 0,
    Lz4 = 1,
}

impl<'a> ValuesEncoder<&'a [u8]> for ValuesEncoderImpl {
    /// The layout can be referred to the docs of [`Encoding`].
    fn encode<B, I>(&self, buf: &mut B, values: I) -> Result<()>
    where
        B: BufMut,
        I: Iterator<Item = &'a [u8]> + Clone,
    {
        let encoding = Encoding;
        encoding.encode(buf, values, self.bytes_compress_threshold)
    }

    fn estimated_encoded_size<I>(&self, values: I) -> usize
    where
        I: Iterator<Item = &'a [u8]>,
    {
        let encoding = Encoding;
        encoding.estimated_encoded_size(values)
    }
}

impl ValuesDecoder<Bytes> for ValuesDecoderImpl {
    /// The layout can be referred to the docs of [`Encoding`].
    fn decode<B, F>(&self, ctx: DecodeContext<'_>, buf: &mut B, f: F) -> Result<()>
    where
        B: Buf,
        F: FnMut(Bytes) -> Result<()>,
    {
        let encoding = Encoding;
        encoding.decode(ctx, buf, f)
    }
}
