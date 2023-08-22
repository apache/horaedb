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

use bytes_ext::{Buf, BufMut, SafeBufMut};

use crate::columnar::{Result, ValuesDecoder, ValuesEncoder};

pub struct TimestampValuesEncoder;

// Encode the timestamp using the delta-of-delta algorithm
impl ValuesEncoder<i64> for TimestampValuesEncoder {
    //
    fn encode<B, I>(&self, buf: &mut B, values: I) -> Result<()>
    where
        B: BufMut,
        I: Iterator<Item = i64>,
    {
        // let firstTs = match values.find_or_first() {
        //     Some(a) => a,
        //     None() => Err(),
        // };

        let mut last_ts: i64 = 0;
        let mut last_delta: i64 = 0;
        for v in values {
            let cur_delta = v - last_ts;
            let delta_of_delta = cur_delta - last_delta;
            let encode_result = encode_delta_of_delta(delta_of_delta);
            last_delta = cur_delta;
        }

        Ok(())
    }

    fn estimated_encoded_size<I>(&self, values: I) -> usize
    where
        I: Iterator<Item = i64>,
    {
        let (lower, higher) = values.size_hint();
        let num = lower.max(higher.unwrap_or_default());
        num
    }
}

const DELTA_MAST_7: i32 = 0x02 << 7;

// TODO: delta_of_delta bigger than i32?
fn encode_delta_of_delta(dod: i64) -> u32 {
    // let bit_required = 32 - std::num::NonZeroI32::new(dod -
    // 1).unwrap().leading_zeros();

    return if dod == 0 {
        0b0
    } else if dod >= -63 && dod <= 64 {
        0b10 << 7 + dod
    } else if dod >= -255 && dod <= 256 {
        0b110 << 9 + dod
    } else if dod >= -2047 && dod <= 2048 {
        0b1110 << 12 + dod
    } else {
        0b1111 << 32 + dod
    };
}

// fn compress_encode_results(results: &[i64], buf: &mut B) -> Result<()> {
//     todo!()
// }

#[test]
fn test_delta_of_delta_encode() {
    assert!(0, encode_delta_of_delta(0));
    assert!(0, encode_delta_of_delta(10));
    assert!(0, encode_delta_of_delta(-10));
    assert!(0, encode_delta_of_delta(64));
    assert!(0, encode_delta_of_delta(-64));
    assert!(0, encode_delta_of_delta(256));
    assert!(0, encode_delta_of_delta(-256));
    assert!(0, encode_delta_of_delta(2048));
    assert!(0, encode_delta_of_delta(-2048));
    assert!(0, encode_delta_of_delta(5000));
}

#[test]
fn test_delta_of_delta_decode() {
    todo!()
}

pub struct TimestampValuesDecoder;

impl ValuesDecoder<i64> for TimestampValuesDecoder {
    fn decode<B, F>(&self, buf: &mut B, mut f: F) -> Result<()>
    where
        B: Buf,
        F: FnMut(i64) -> Result<()>,
    {
        todo!()
    }
}
