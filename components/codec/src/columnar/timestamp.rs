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
use log::debug;
use snafu::{ensure, OptionExt};

use crate::{
    bits::{Bit, BufferedReader, BufferedWriter},
    columnar::{
        DecodeContext, InvalidVersion, NotEnoughDatums, ReadEncode, Result, ValuesDecoder,
        ValuesDecoderImpl, ValuesEncoder, ValuesEncoderImpl,
    },
};

/// We use delta-of-delta algorithm to compress timestamp, for details, please refer to this paper http://www.vldb.org/pvldb/vol8/p1816-teller.pdf
/// The layout for the encoded payload:
/// ```plaintext
/// +----------------+-------------------------+-------------------+--------------------------+------------------+------------------+-----------------------+----------------------+
/// | version(8bits) | first_timestamp(64bits) | control_bit(1bit) | delta_of_delta(1~36bits) | delta_of_delta...| end_mask(36bits) | timestamp_len(32bits) | timestamp(64bits)... |
/// +----------------+-------------------------+-------------------+--------------------------+------------------+------------------+-----------------------+----------------------+

const ENCODE_VERSION: u8 = 1;
const NUM_BYTES_ENCODE_VERSION_LEN: usize = 1;

const NUM_BYTES_CONTROL_BIT_LEN: usize = 1;

const NUM_BYTES_TIMESTAMP_LEN: usize = 4;
const NUM_BYTES_TIMESTAMP: usize = 8;

/// END_MARKER is a special bit sequence used to indicate the end of the
/// stream
const END_MARKER: u64 = 0b1111_0000_0000_0000_0000_0000_0000_0000_0000;
/// END_MARKER_LEN is the length, in bits, of END_MARKER
const NUM_BITS_END_MARKER_LEN: usize = 36;

// Encode the timestamp using the delta-of-delta algorithm
struct TimestampEncoder {}

struct TimestampDecoder {}

enum DeltaOfDeltaEncodeMasks {
    Zero(),
    Normal {
        // control_bits represents several encoding type of timestamp in dod encoding
        control_bits: u8,
        // control_bits_len represents the length of control_bits it self
        control_bits_len: u8,
        // dod_bits_len represents the data length of the compressed timestamp in dod encoding
        dod_bits_len: u8,
    },
    None(),
}

impl From<i32> for DeltaOfDeltaEncodeMasks {
    fn from(dod_control_bits_size: i32) -> Self {
        match dod_control_bits_size {
            0 => DeltaOfDeltaEncodeMasks::Zero(),
            1 => DeltaOfDeltaEncodeMasks::Normal {
                control_bits: 0b10,
                control_bits_len: 2,
                dod_bits_len: 7,
            },
            2 => DeltaOfDeltaEncodeMasks::Normal {
                control_bits: 0b110,
                control_bits_len: 3,
                dod_bits_len: 9,
            },
            3 => DeltaOfDeltaEncodeMasks::Normal {
                control_bits: 0b1110,
                control_bits_len: 4,
                dod_bits_len: 12,
            },
            4 => DeltaOfDeltaEncodeMasks::Normal {
                control_bits: 0b1111,
                control_bits_len: 4,
                dod_bits_len: 32,
            },
            _ => unreachable!(),
        }
    }
}

impl From<i64> for DeltaOfDeltaEncodeMasks {
    fn from(dod: i64) -> Self {
        // max support 2^31/3600/1000/24/2=12 day
        if dod > std::i32::MAX as i64 {
            debug!(
                "dod reach max value, fallback to origin timestamp encode, dod:{:?}",
                dod
            );
            return DeltaOfDeltaEncodeMasks::None();
        }
        let dod = dod as i32;

        // store the delta of delta using variable length encoding
        match dod {
            0 => DeltaOfDeltaEncodeMasks::Zero(),
            -63..=64 => DeltaOfDeltaEncodeMasks::Normal {
                control_bits: 0b10,
                control_bits_len: 2,
                dod_bits_len: 7,
            },
            -255..=256 => DeltaOfDeltaEncodeMasks::Normal {
                control_bits: 0b110,
                control_bits_len: 3,
                dod_bits_len: 9,
            },
            -2047..=2048 => DeltaOfDeltaEncodeMasks::Normal {
                control_bits: 0b1110,
                control_bits_len: 4,
                dod_bits_len: 12,
            },
            _ => DeltaOfDeltaEncodeMasks::Normal {
                control_bits: 0b1111,
                control_bits_len: 4,
                dod_bits_len: 32,
            },
        }
    }
}

impl TimestampEncoder {
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
        writer.write_byte(ENCODE_VERSION);

        // variables used for none encode timestamp
        let mut need_fallback = false;
        let mut dod_encode_ts_len = 1;

        // variables used to encode dod
        let mut first = true;
        let mut prev_ts: i64 = 0;
        let mut prev_delta: i64 = 0;

        for v in values {
            if !need_fallback {
                if first {
                    // store the first timestamp exactly
                    writer.write_bits(v as u64, 64);
                    first = false;
                    prev_ts = v;
                    if num == 1 {
                        writer.write_bit(Bit(0))
                    } else {
                        writer.write_bit(Bit(1))
                    }
                    continue;
                }
                let cur_delta = v - prev_ts;
                let delta_of_delta = cur_delta - prev_delta;
                match DeltaOfDeltaEncodeMasks::from(delta_of_delta) {
                    DeltaOfDeltaEncodeMasks::Zero() => {
                        writer.write_bit(Bit(0));
                        dod_encode_ts_len += 1;
                    }
                    DeltaOfDeltaEncodeMasks::Normal {
                        control_bits,
                        control_bits_len,
                        dod_bits_len,
                    } => {
                        writer.write_bits(control_bits as u64, control_bits_len as u32);
                        writer.write_bits(delta_of_delta as u64, dod_bits_len as u32);
                        dod_encode_ts_len += 1;
                    }
                    DeltaOfDeltaEncodeMasks::None() => {
                        writer.write_bits(END_MARKER, NUM_BITS_END_MARKER_LEN as u32);

                        let fallback_ts_num = num - dod_encode_ts_len;
                        writer.write_bits(fallback_ts_num as u64, 32);
                        writer.write_bits(v as u64, 64);
                        need_fallback = true;
                    }
                };
                prev_delta = cur_delta;
                prev_ts = v;

                if num == dod_encode_ts_len {
                    // write end mask
                    writer.write_bits(END_MARKER, NUM_BITS_END_MARKER_LEN as u32);
                    writer.write_bits(0, 32);
                    continue;
                }
            } else {
                // Unable to encode with dod, break out of loop and fallback to i64 storage
                writer.write_bits(v as u64, 64);
            }
        }

        buf.put_slice(writer.close().as_ref());
        Ok(())
    }
}

impl TimestampDecoder {
    pub fn decode<B, F>(buf: &mut B, mut f: F) -> Result<()>
    where
        B: Buf,
        F: FnMut(Timestamp) -> Result<()>,
    {
        let version = buf.get_u8();
        ensure!(version == ENCODE_VERSION, InvalidVersion { version });

        let mut reader = BufferedReader::new(buf.chunk());

        let first_timestamp = reader.next_bits(64).context(ReadEncode)?;
        f(Timestamp::new(first_timestamp as i64))?;

        let control_bit = reader.next_bit().context(ReadEncode)?;
        if control_bit == Bit(0) {
            return Ok(());
        }

        let mut prev_delta: u64 = 0;
        let mut prev_timestamp = first_timestamp;
        loop {
            let mut dod_control_bits_size = 0;
            // control bits includes the following situations:
            // 0,10,110,1110,1111.
            // we judge which one is specific by the number of 1.
            for _ in 0..4 {
                let bit = reader.next_bit().context(ReadEncode)?;

                if bit == Bit(1) {
                    dod_control_bits_size += 1;
                } else {
                    break;
                }
            }

            let dod_masks = DeltaOfDeltaEncodeMasks::from(dod_control_bits_size);

            let dod = match dod_masks {
                DeltaOfDeltaEncodeMasks::Zero() => 0,
                DeltaOfDeltaEncodeMasks::Normal {
                    control_bits: _control_bits,
                    control_bits_len: _control_bits_len,
                    dod_bits_len,
                } => {
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
                _ => {
                    panic!("none encode dod could not be decoded")
                }
            };

            // Reach end mask
            if dod == 0 && dod_control_bits_size == 4 {
                break;
            }

            let cur_delta = prev_delta.wrapping_add(dod);
            let cur_timestamp = prev_timestamp.wrapping_add(cur_delta);

            f(Timestamp::new(cur_timestamp as i64))?;

            prev_delta = cur_delta;
            prev_timestamp = cur_timestamp;
        }

        // decode unencoded timestamps
        let timestamp_len = reader.next_bits(32).context(ReadEncode)?;
        for _ in 0..timestamp_len {
            let timestamp = reader.next_bits(64).context(ReadEncode)?;
            f(Timestamp::new(timestamp as i64))?;
        }

        Ok(())
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
            + NUM_BYTES_CONTROL_BIT_LEN
            + NUM_BITS_END_MARKER_LEN
            + NUM_BYTES_TIMESTAMP_LEN
            + NUM_BYTES_TIMESTAMP * num
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
