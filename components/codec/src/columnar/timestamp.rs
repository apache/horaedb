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

use common_types::time::Timestamp;
use snafu::{ensure, OptionExt, ResultExt};

use super::{DecodeContext, Overflow, Result, ValuesDecoder, ValuesDecoderImpl, Varint};
use crate::{
    columnar::{InvalidVersion, ValuesEncoder, ValuesEncoderImpl},
    consts::MAX_VARINT_BYTES,
    varint,
};

/// The layout for the timestamp values:
/// ```plaintext
/// +-------------+----------------------+--------+
/// | version(u8) | first_timestamp(i64) | deltas |
/// +-------------+----------------------+--------+
/// ```
///
/// This encoding assume the timestamps are have very small differences between
/// each other, so we just store the deltas from the first timestamp in varint.
struct Encoding;

impl Encoding {
    const VERSION: u8 = 0;
    const VERSION_SIZE: usize = 1;
}

impl ValuesEncoder<Timestamp> for ValuesEncoderImpl {
    fn encode<B, I>(&self, buf: &mut B, mut values: I) -> Result<()>
    where
        B: bytes_ext::BufMut,
        I: Iterator<Item = Timestamp> + Clone,
    {
        buf.put_u8(Encoding::VERSION);

        let first_ts = match values.next() {
            Some(v) => v.as_i64(),
            None => return Ok(()),
        };

        buf.put_i64(first_ts);

        for value in values {
            let ts = value.as_i64();
            let delta = ts.checked_sub(first_ts).with_context(|| Overflow {
                msg: format!("first timestamp:{ts}, current timestamp:{first_ts}"),
            })?;
            varint::encode_varint(buf, delta).context(Varint)?;
        }

        Ok(())
    }

    fn estimated_encoded_size<I>(&self, values: I) -> usize
    where
        I: Iterator<Item = Timestamp>,
    {
        let (lower, higher) = values.size_hint();
        let num = lower.max(higher.unwrap_or_default());
        num * MAX_VARINT_BYTES + Encoding::VERSION_SIZE
    }
}

impl ValuesDecoder<Timestamp> for ValuesDecoderImpl {
    fn decode<B, F>(&self, _ctx: DecodeContext<'_>, buf: &mut B, mut f: F) -> Result<()>
    where
        B: bytes_ext::Buf,
        F: FnMut(Timestamp) -> Result<()>,
    {
        let version = buf.get_u8();

        ensure!(version == Encoding::VERSION, InvalidVersion { version });

        if buf.remaining() == 0 {
            return Ok(());
        }

        let first_ts = buf.get_i64();
        f(Timestamp::new(first_ts))?;

        while buf.remaining() > 0 {
            let delta = varint::decode_varint(buf).context(Varint)?;
            let ts = first_ts.checked_add(delta).with_context(|| Overflow {
                msg: format!("first timestamp:{first_ts}, delta:{delta}"),
            })?;
            f(Timestamp::new(ts))?;
        }

        Ok(())
    }
}
