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
use snafu::{ensure, ResultExt};
use zstd::{Decoder as ZstdStreamDecoder, Encoder as ZstdStreamEncoder};

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
/// - 1: the data block is compressed by ZSTD
///
/// And the lengths in the `length block` are encoded in varint.
/// And the reason to put `length_block_len` and `compression` at the footer is
/// for friendly decode.
struct Encoding;

impl Encoding {
    const COMPRESSION_SIZE: usize = 1;
    /// If the data block exceeds this threshold, it will be compressed.
    const COMPRESS_BYTES_THRESHOLD: usize = 256;
    const LENGTH_BLOCK_LEN_SIZE: usize = 4;
    const VERSION: u8 = 0;
    const VERSION_SIZE: usize = 1;
    const ZSTD_LEVEL: i32 = 3;

    fn decide_compression(data_block_len: usize) -> Compression {
        if data_block_len > Self::COMPRESS_BYTES_THRESHOLD {
            Compression::Zstd
        } else {
            Compression::NoCompression
        }
    }

    fn decode_compression(v: u8) -> Result<Compression> {
        let version = match v {
            0 => Compression::NoCompression,
            1 => Compression::Zstd,
            _ => InvalidCompression { flag: v }.fail()?,
        };

        Ok(version)
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
    Zstd = 1,
}

impl<'a> ValuesEncoder<&'a [u8]> for ValuesEncoderImpl {
    /// The layout can be referred to the docs of [`Encoding`].
    fn encode<B, I>(&self, buf: &mut B, values: I) -> Result<()>
    where
        B: BufMut,
        I: Iterator<Item = &'a [u8]> + Clone,
    {
        // Encode the `version`.
        buf.put_u8(Encoding::VERSION);

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
        let compression = Encoding::decide_compression(data_block_len);
        match compression {
            Compression::NoCompression => {
                for v in values {
                    buf.put_slice(v);
                }
            }
            Compression::Zstd => encode_with_compress(buf, values).context(Compress)?,
        }

        // Encode the `data_block` offset.
        buf.put_u32(length_block_len as u32);
        buf.put_u8(compression as u8);

        Ok(())
    }

    fn estimated_encoded_size<I>(&self, values: I) -> usize
    where
        I: Iterator<Item = &'a [u8]>,
    {
        let mut total_bytes =
            Encoding::VERSION_SIZE + Encoding::LENGTH_BLOCK_LEN_SIZE + Encoding::COMPRESSION_SIZE;

        for v in values {
            // The length of `v` should be ensured to be smaller than [u32::MAX], that is to
            // say, at most 5 bytes will be used when do varint encoding over a u32 number.
            total_bytes += 5 + v.len();
        }
        total_bytes
    }
}

fn encode_with_compress<'a, B, I>(buf: &mut B, values: I) -> std::io::Result<()>
where
    B: BufMut,
    I: Iterator<Item = &'a [u8]>,
{
    let writer = WriterOnBufMut { buf };
    let mut zstd_enc = ZstdStreamEncoder::new(writer, Encoding::ZSTD_LEVEL)?;
    for v in values {
        zstd_enc.write_all(v)?;
    }
    zstd_enc.finish()?;

    Ok(())
}

fn decode_without_compression<B, F>(
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

fn decode_with_compression<F>(
    mut length_block_buf: &[u8],
    compressed_data_block_buf: &[u8],
    reused_buf: &mut Vec<u8>,
    f: F,
) -> Result<()>
where
    F: FnMut(Bytes) -> Result<()>,
{
    let mut decoder = ZstdStreamDecoder::new(compressed_data_block_buf).context(Decompress)?;
    decoder.read_to_end(reused_buf).context(Decompress)?;
    decode_without_compression(&mut length_block_buf, &reused_buf[..], f)
}

impl ValuesDecoder<Bytes> for ValuesDecoderImpl {
    /// The layout can be referred to the docs of [`Encoding`].
    fn decode<B, F>(&self, ctx: DecodeContext<'_>, buf: &mut B, f: F) -> Result<()>
    where
        B: Buf,
        F: FnMut(Bytes) -> Result<()>,
    {
        let chunk = buf.chunk();
        let footer_len = Encoding::LENGTH_BLOCK_LEN_SIZE + Encoding::COMPRESSION_SIZE;
        ensure!(
            chunk.len() > footer_len + Encoding::VERSION_SIZE,
            NotEnoughBytes {
                len: footer_len + Encoding::VERSION_SIZE
            }
        );

        let version = chunk[0];
        ensure!(version == Encoding::VERSION, InvalidVersion { version });

        let compression_offset = chunk.len() - Encoding::COMPRESSION_SIZE;
        let compression = Encoding::decode_compression(chunk[compression_offset])?;

        let length_block_len_offset = chunk.len() - footer_len;
        let length_block_end = {
            let mut len_buf = &chunk[length_block_len_offset..compression_offset];
            len_buf.get_u32() as usize + Encoding::VERSION_SIZE
        };
        let mut length_block = &chunk[Encoding::VERSION_SIZE..length_block_end];
        let data_block = &chunk[length_block_end..length_block_len_offset];

        match compression {
            Compression::NoCompression => {
                decode_without_compression(&mut length_block, data_block, f)
            }
            Compression::Zstd => decode_with_compression(length_block, data_block, ctx.buf, f),
        }
    }
}
