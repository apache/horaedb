// Copyright 2022-2023 CeresDB Project Authors. Licensed under Apache-2.0.

//! Number format

use bytes_ext::{Buf, SafeBufMut};
use snafu::ResultExt;

use crate::{
    compact::{DecodeVarint, EncodeVarint, Error, MemCompactDecoder, MemCompactEncoder, Result},
    consts, varint, DecodeTo, Encoder,
};

impl Encoder<i64> for MemCompactEncoder {
    type Error = Error;

    fn encode<B: SafeBufMut>(&self, buf: &mut B, value: &i64) -> Result<()> {
        varint::encode_varint(buf, *value).context(EncodeVarint)?;
        Ok(())
    }

    fn estimate_encoded_size(&self, _value: &i64) -> usize {
        consts::MAX_VARINT_BYTES
    }
}

impl DecodeTo<i64> for MemCompactDecoder {
    type Error = Error;

    fn decode_to<B: Buf>(&self, buf: &mut B, value: &mut i64) -> Result<()> {
        *value = varint::decode_varint(buf).context(DecodeVarint)?;
        Ok(())
    }
}

impl Encoder<u64> for MemCompactEncoder {
    type Error = Error;

    fn encode<B: SafeBufMut>(&self, buf: &mut B, value: &u64) -> Result<()> {
        varint::encode_uvarint(buf, *value).context(EncodeVarint)?;
        Ok(())
    }

    fn estimate_encoded_size(&self, _value: &u64) -> usize {
        consts::MAX_UVARINT_BYTES
    }
}

impl DecodeTo<u64> for MemCompactDecoder {
    type Error = Error;

    fn decode_to<B: Buf>(&self, buf: &mut B, value: &mut u64) -> Result<()> {
        *value = varint::decode_uvarint(buf).context(DecodeVarint)?;
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use super::*;

    struct TestI64 {
        data: i64,
        estimate_encoded_size: usize,
    }
    #[test]
    fn test_compact_i64_codec() {
        let data = vec![
            TestI64 {
                data: 1621324705,
                estimate_encoded_size: 10,
            },
            TestI64 {
                data: 1621324705000,
                estimate_encoded_size: 10,
            },
            TestI64 {
                data: 1521324705,
                estimate_encoded_size: 10,
            },
            TestI64 {
                data: 1621324705123,
                estimate_encoded_size: 10,
            },
            TestI64 {
                data: i64::MIN,
                estimate_encoded_size: 10,
            },
            TestI64 {
                data: i64::MIN + 1,
                estimate_encoded_size: 10,
            },
            TestI64 {
                data: 0,
                estimate_encoded_size: 10,
            },
            TestI64 {
                data: i64::MAX,
                estimate_encoded_size: 10,
            },
            TestI64 {
                data: (1 << 47) - 1,
                estimate_encoded_size: 10,
            },
            TestI64 {
                data: -1 << 47,
                estimate_encoded_size: 10,
            },
            TestI64 {
                data: (1 << 23) - 1,
                estimate_encoded_size: 10,
            },
            TestI64 {
                data: -1 << 23,
                estimate_encoded_size: 10,
            },
            TestI64 {
                data: (1 << 33) - 1,
                estimate_encoded_size: 10,
            },
            TestI64 {
                data: -1 << 33,
                estimate_encoded_size: 10,
            },
            TestI64 {
                data: (1 << 55) - 1,
                estimate_encoded_size: 10,
            },
            TestI64 {
                data: -1 << 55,
                estimate_encoded_size: 10,
            },
            TestI64 {
                data: 1,
                estimate_encoded_size: 10,
            },
            TestI64 {
                data: -1,
                estimate_encoded_size: 10,
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
            let mut d = -1;
            decoder.decode_to(&mut buf, &mut d).unwrap();
            assert_eq!(d, x.data);
        }
    }
}
