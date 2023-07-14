// Copyright 2022-2023 CeresDB Project Authors. Licensed under Apache-2.0.

//! Bytes format

use std::convert::TryFrom;

use common_types::bytes::{Buf, BufMut, Bytes, BytesMut, SafeBuf, SafeBufMut};
use snafu::{ensure, ResultExt};

use crate::{
    compact::{
        DecodeEmptyValue, DecodeValue, DecodeVarint, EncodeValue, EncodeVarint, Error,
        MemCompactDecoder, MemCompactEncoder, Result, SkipDecodedValue, TryIntoUsize,
    },
    consts, varint, DecodeTo, Encoder,
};

impl Encoder<[u8]> for MemCompactEncoder {
    type Error = Error;

    // EncodeCompactBytes joins bytes with its length into a byte slice. It is more
    // efficient in both space and time compare to EncodeBytes. Note that the
    // encoded result is not memcomparable.
    fn encode<B: BufMut>(&self, buf: &mut B, value: &[u8]) -> Result<()> {
        varint::encode_varint(buf, value.len() as i64).context(EncodeVarint)?;
        buf.try_put(value).context(EncodeValue)?;
        Ok(())
    }

    fn estimate_encoded_size(&self, value: &[u8]) -> usize {
        consts::MAX_VARINT_BYTES + value.len()
    }
}

impl Encoder<Bytes> for MemCompactEncoder {
    type Error = Error;

    fn encode<B: BufMut>(&self, buf: &mut B, value: &Bytes) -> Result<()> {
        self.encode(buf, &value[..])
    }

    fn estimate_encoded_size(&self, value: &Bytes) -> usize {
        self.estimate_encoded_size(&value[..])
    }
}

impl DecodeTo<BytesMut> for MemCompactDecoder {
    type Error = Error;

    fn decode_to<B: Buf>(&self, buf: &mut B, value: &mut BytesMut) -> Result<()> {
        let v = usize::try_from(varint::decode_varint(buf).context(DecodeVarint)?)
            .context(TryIntoUsize)?;
        ensure!(buf.remaining() >= v, DecodeEmptyValue);
        value.try_put(&buf.chunk()[..v]).context(DecodeValue)?;
        buf.try_advance(v).context(SkipDecodedValue)?;
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use super::*;

    struct BytesTest {
        data: Bytes,
        estimate_encoded_size: usize,
    }

    #[test]
    fn test_compact_bytes_codec() {
        let data = vec![
            BytesTest {
                data: Bytes::from_static(b""),
                estimate_encoded_size: 10,
            },
            BytesTest {
                data: Bytes::from_static(b"hello1"),
                estimate_encoded_size: 16,
            },
            BytesTest {
                data: Bytes::from_static(b"hello2"),
                estimate_encoded_size: 16,
            },
            BytesTest {
                data: Bytes::from_static(b"hello3"),
                estimate_encoded_size: 16,
            },
            BytesTest {
                data: Bytes::from_static(&[0x00, 0x01]),
                estimate_encoded_size: 12,
            },
            BytesTest {
                data: Bytes::from_static(&[0xff, 0xff]),
                estimate_encoded_size: 12,
            },
            BytesTest {
                data: Bytes::from_static(&[0x01, 0x00]),
                estimate_encoded_size: 12,
            },
            BytesTest {
                data: Bytes::from_static(b"abc"),
                estimate_encoded_size: 13,
            },
            BytesTest {
                data: Bytes::from_static(b"hello world"),
                estimate_encoded_size: 21,
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
            let mut d = BytesMut::new();
            decoder.decode_to(&mut buf, &mut d).unwrap();
            assert_eq!(d, x.data);
        }
    }
}
