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

use crate::columnar::{
    DecodeContext, Result, ValuesDecoder, ValuesDecoderImpl, ValuesEncoder, ValuesEncoderImpl,
};

impl ValuesEncoder<f64> for ValuesEncoderImpl {
    fn encode<B, I>(&self, buf: &mut B, values: I) -> Result<()>
    where
        B: BufMut,
        I: Iterator<Item = f64>,
    {
        for v in values {
            buf.put_f64(v);
        }

        Ok(())
    }
}

impl ValuesDecoder<f64> for ValuesDecoderImpl {
    fn decode<B, F>(&self, _ctx: DecodeContext<'_>, buf: &mut B, mut f: F) -> Result<()>
    where
        B: Buf,
        F: FnMut(f64) -> Result<()>,
    {
        while buf.remaining() > 0 {
            let v = buf.get_f64();
            f(v)?;
        }

        Ok(())
    }
}
