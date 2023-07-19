// Copyright 2022-2023 CeresDB Project Authors. Licensed under Apache-2.0.

use std::mem;

use bytes_ext::{SafeBuf, SafeBufMut};
use snafu::ResultExt;

use crate::{
    compact::{DecodeValue, EncodeValue, Error, MemCompactDecoder, MemCompactEncoder, Result},
    DecodeTo, Encoder,
};

impl Encoder<f64> for MemCompactEncoder {
    type Error = Error;

    fn encode<B: SafeBufMut>(&self, buf: &mut B, value: &f64) -> Result<()> {
        buf.try_put_f64(*value).context(EncodeValue)?;
        Ok(())
    }

    fn estimate_encoded_size(&self, _value: &f64) -> usize {
        mem::size_of::<f64>()
    }
}

impl DecodeTo<f64> for MemCompactDecoder {
    type Error = Error;

    fn decode_to<B: SafeBuf>(&self, buf: &mut B, value: &mut f64) -> Result<()> {
        *value = buf.try_get_f64().context(DecodeValue)?;
        Ok(())
    }
}

impl Encoder<f32> for MemCompactEncoder {
    type Error = Error;

    fn encode<B: SafeBufMut>(&self, buf: &mut B, value: &f32) -> Result<()> {
        buf.try_put_f32(*value).context(EncodeValue)?;
        Ok(())
    }

    fn estimate_encoded_size(&self, _value: &f32) -> usize {
        mem::size_of::<f32>()
    }
}

impl DecodeTo<f32> for MemCompactDecoder {
    type Error = Error;

    fn decode_to<B: SafeBuf>(&self, buf: &mut B, value: &mut f32) -> Result<()> {
        *value = buf.try_get_f32().context(DecodeValue)?;
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use super::*;

    struct TestF64 {
        data: f64,
        estimate_encoded_size: usize,
    }

    #[test]
    fn test_compact_f64_codec() {
        let data = vec![
            TestF64 {
                data: 162132470.5,
                estimate_encoded_size: 8,
            },
            TestF64 {
                data: f64::MIN,
                estimate_encoded_size: 8,
            },
            TestF64 {
                data: f64::MAX,
                estimate_encoded_size: 8,
            },
        ];

        let encoder = MemCompactEncoder;
        let mut buf = vec![];
        for x in &data {
            encoder.encode(&mut buf, &x.data).unwrap();
            assert_eq!(
                x.estimate_encoded_size,
                encoder.estimate_encoded_size(&x.data)
            );
        }

        let decoder = MemCompactDecoder;
        let mut buf = &buf[..];
        for x in &data {
            let mut d = 0.0;
            decoder.decode_to(&mut buf, &mut d).unwrap();
            assert!((d - x.data).abs() < f64::EPSILON);
        }
    }
}
