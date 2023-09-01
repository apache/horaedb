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
use common_types::time::Timestamp;
use snafu::{ensure, ResultExt};

use crate::{
    bits::{Bit, BufferedReader, BufferedWriter},
    columnar::{
        DecodeContext, InvalidVersion, NotEnoughDatums, ReadEncode, Result, ValuesDecoder,
        ValuesDecoderImpl, ValuesEncoder, ValuesEncoderImpl,
    },
};

/// We use delta-of-delta algorithm to compress timestamp, for details, please refer to this paper http://www.vldb.org/pvldb/vol8/p1816-teller.pdf
/// The layout for the timestamp:
/// ```plaintext
/// +--------------+------------------------+-------------------+--------------------------+------------------+------------------+--------------------+-------------------+
/// | version(u8) | first_timestamp(64bits) | control_bit(1bit) | delta_of_delta(1~36bits) | delta_of_delta...| end_mask(36bits) | timestamp_len(u32) | timestamp(i64)... |
/// +-------------+-------------------------+-------------------+--------------------------+------------------+------------------+--------------------+-------------------+

const ENCODE_VERSION: u8 = 1;
const NUM_BYTES_ENCODE_VERSION_LEN: usize = 1;

/// The max number of the bytes used to store a delta of delta encoding of
/// first timestamp.
const NUM_BYTES_OF_FIRST_TS_ENCODE: usize = 8;

/// The max number of the bytes used to store a delta of delta encoding of
/// dod result.
const MAX_NUM_BYTES_OF_DOD_ENCODE: usize = 5;

/// END_MARKER is a special bit sequence used to indicate the end of the
/// stream
const END_MARKER: u64 = 0b1111_0000_0000_0000_0000_0000_0000_0000_0000;
/// END_MARKER_LEN is the length, in bits, of END_MARKER
const NUM_BITS_END_MARKER_LEN: usize = 36;
const NUM_BYTES_END_MARKER_LEN: usize = 5;

// Encode the timestamp using the delta-of-delta algorithm
struct TimestampEncoder {}

struct TimestampDecoder {}

enum DeltaOfDeltaEncodeMasks {
    ZeroEncode(),
    NormalEncode(u8, u8, u8),
}

impl TimestampEncoder {
    fn encode_delta_of_delta_masks(dod: i64) -> DeltaOfDeltaEncodeMasks {
        // max support 2^31/3600/1000/24/2=12 day
        assert!(dod <= std::i32::MAX as i64);
        let dod = dod as i32;

        // store the delta of delta using variable length encoding
        match dod {
            0 => DeltaOfDeltaEncodeMasks::ZeroEncode(),
            -63..=64 => DeltaOfDeltaEncodeMasks::NormalEncode(0b10, 2, 7),
            -255..=256 => DeltaOfDeltaEncodeMasks::NormalEncode(0b110, 3, 9),
            -2047..=2048 => DeltaOfDeltaEncodeMasks::NormalEncode(0b1110, 4, 12),
            _ => DeltaOfDeltaEncodeMasks::NormalEncode(0b1111, 4, 32),
        }
    }

    pub fn encode<B, I>(buf: &mut B, values: I) -> Result<()>
    where
        B: BufMut,
        I: Iterator<Item = i64>,
    {
        let (lower, higher) = values.size_hint();
        let num = lower.max(higher.unwrap_or_default());
        ensure!(
            num >= 1,
            NotEnoughDatums {
                expect: 1usize,
                found: 0usize
            }
        );

        let mut writer = BufferedWriter::with_capacity(0);

        // write version
        writer.write_bits(
            ENCODE_VERSION as u64,
            (8 * NUM_BYTES_ENCODE_VERSION_LEN) as u32,
        );

        let mut first = true;
        let mut control_bit_flag = true;
        let mut last_ts: i64 = 0;
        let mut last_delta: i64 = 0;
        for v in values {
            if first {
                // store the first timestamp exactly
                writer.write_bits(v as u64, 64);
                first = false;
                last_ts = v;
                continue;
            }
            if !first && control_bit_flag {
                // remove according to the paper
                // // write one control bit so we can distinguish a stream which contains only
                // an initial // timestamp, this assumes the first bit of the
                // END_MARKER is 1
                writer.write_bit(Bit(1));
                control_bit_flag = false;
            }
            let cur_delta = v - last_ts;
            let delta_of_delta = cur_delta - last_delta;
            match TimestampEncoder::encode_delta_of_delta_masks(delta_of_delta) {
                DeltaOfDeltaEncodeMasks::ZeroEncode() => writer.write_bit(Bit(0)),
                DeltaOfDeltaEncodeMasks::NormalEncode(
                    control_bis,
                    control_bits_len,
                    dod_bits_len,
                ) => {
                    writer.write_bits(control_bis as u64, control_bits_len as u32);
                    writer.write_bits(delta_of_delta as u64, dod_bits_len as u32);
                }
            };
            last_delta = cur_delta;
            last_ts = v;
        }

        // write control bit when only the first timestamp exists
        if control_bit_flag {
            writer.write_bit(Bit(0));
        }
        // write end mask
        if !first {
            writer.write_bits(END_MARKER, NUM_BITS_END_MARKER_LEN as u32);
        }

        buf.put_slice(writer.close().as_ref());
        Ok(())
    }
}

impl TimestampDecoder {
    fn encode_delta_of_delta_masks(dod_control_bits_size: &mut i32) -> DeltaOfDeltaEncodeMasks {
        match dod_control_bits_size {
            0 => DeltaOfDeltaEncodeMasks::ZeroEncode(),
            1 => DeltaOfDeltaEncodeMasks::NormalEncode(0b10, 2, 7),
            2 => DeltaOfDeltaEncodeMasks::NormalEncode(0b110, 3, 9),
            3 => DeltaOfDeltaEncodeMasks::NormalEncode(0b1110, 4, 12),
            4 => DeltaOfDeltaEncodeMasks::NormalEncode(0b1111, 4, 32),
            _ => unreachable!(),
        }
    }

    pub fn decode<B, F>(buf: &mut B, mut f: F) -> Result<()>
    where
        B: Buf,
        F: FnMut(Timestamp) -> Result<()>,
    {
        let mut reader = BufferedReader::new(buf.chunk());

        let version = reader
            .next_bits((NUM_BYTES_ENCODE_VERSION_LEN * 8) as u32)
            .context(ReadEncode)? as u8;
        ensure!(version == ENCODE_VERSION, InvalidVersion { version });

        let first_timestamp = reader.next_bits(64).context(ReadEncode)?;
        f(Timestamp::new(first_timestamp as i64))?;

        let control_bit = reader.next_bit().context(ReadEncode)?;
        if control_bit == Bit(0) {
            return Ok(());
        }

        let mut last_delta: u64 = 0;
        let mut last_timestamp = first_timestamp;
        loop {
            let mut dod_control_bits_size = 0;
            // 0,10,110,1110,1111
            for _ in 0..4 {
                let bit = reader.next_bit().context(ReadEncode)?;

                if bit == Bit(1) {
                    dod_control_bits_size += 1;
                } else {
                    break;
                }
            }

            let dod_masks = Self::encode_delta_of_delta_masks(&mut dod_control_bits_size);

            let dod = match dod_masks {
                DeltaOfDeltaEncodeMasks::ZeroEncode() => 0,
                DeltaOfDeltaEncodeMasks::NormalEncode(_, _, dod_bits_len) => {
                    let mut dod = reader.next_bits(dod_bits_len as u32).context(ReadEncode)?;

                    if dod_bits_len == 32 {
                        // need to sign extend negative numbers
                        // negative numbers   -1:ffffffffffffffff -2:fffffffffffffffe
                        // -63:ffffffffffffffc1 example: 7bit dod=100 0001
                        // dod:i32=-63. after add mask:dod=ffffffffffffffc1
                        // add ffff... ahead of dod
                        // same as ( dod as i32 as u64 )
                        // if dod > (1 << (32 - 1)) {
                        //     let mask = u64::max_value() << 32;
                        //     dod |= mask;
                        // }
                        dod as i32 as u64
                    } else {
                        // need to sign extend negative numbers
                        // negative numbers   -1:ffffffffffffffff -2:fffffffffffffffe
                        // -63:ffffffffffffffc1 example: 7bit dod=100 0001 dod:i32=-63.
                        // after add mask:dod=ffffffffffffffc1 add ffff... ahead of dod
                        if dod > (1 << (dod_bits_len - 1)) {
                            let mask = u64::max_value() << dod_bits_len;
                            dod |= mask;
                        }
                        dod
                    }
                }
            };

            // Reach end mask
            if dod == 0 && dod_control_bits_size == 4 {
                return Ok(());
            }

            let cur_delta = last_delta.wrapping_add(dod);
            let cur_timestamp = last_timestamp.wrapping_add(cur_delta);

            f(Timestamp::new(cur_timestamp as i64))?;

            last_delta = cur_delta;
            last_timestamp = cur_timestamp;
        }
    }
}

impl ValuesEncoder<Timestamp> for ValuesEncoderImpl {
    fn encode<B, I>(&self, buf: &mut B, values: I) -> Result<()>
    where
        B: BufMut,
        I: Iterator<Item = Timestamp>,
    {
        TimestampEncoder::encode(buf, values.map(|v| v.as_i64()))
    }

    fn estimated_encoded_size<I>(&self, values: I) -> usize
    where
        I: Iterator<Item = Timestamp>,
    {
        let (lower, higher) = values.size_hint();
        let num = lower.max(higher.unwrap_or_default());
        NUM_BYTES_ENCODE_VERSION_LEN
            + NUM_BYTES_OF_FIRST_TS_ENCODE
            + 1
            + (num - 1) * MAX_NUM_BYTES_OF_DOD_ENCODE
            + NUM_BYTES_END_MARKER_LEN
    }
}

impl ValuesDecoder<Timestamp> for ValuesDecoderImpl {
    fn decode<B, F>(&self, _ctx: DecodeContext<'_>, buf: &mut B, f: F) -> Result<()>
    where
        B: Buf,
        F: FnMut(Timestamp) -> Result<()>,
    {
        TimestampDecoder::decode(buf, f)
    }
}
