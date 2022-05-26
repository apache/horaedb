// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

//! Bytes format

use common_types::bytes::{Bytes, BytesMut, MemBuf, MemBufMut};
use snafu::{ensure, ResultExt};

use crate::codec::{
    memcomparable::{
        DecodeValueGroup, DecodeValueMarker, DecodeValuePadding, EncodeValue, Error, MemComparable,
        Result,
    },
    DecodeTo, Encoder,
};

const ENC_GROUP_SIZE: usize = 8;
const ENC_MARKER: u8 = 0xFF;
const ENC_PAD: u8 = 0x0;
const PADS: [u8; ENC_GROUP_SIZE] = [0; ENC_GROUP_SIZE];

impl Encoder<[u8]> for MemComparable {
    type Error = Error;

    // encode Bytes guarantees the encoded value is in ascending order for
    // comparison, encoding with the following rule:
    //  [group1][marker1]...[groupN][markerN]
    //  group is 8 bytes slice which is padding with 0.
    //  marker is `0xFF - padding 0 count`
    // For example:
    //
    // ```
    //   [] -> [0, 0, 0, 0, 0, 0, 0, 0, 247]
    //   [1, 2, 3] -> [1, 2, 3, 0, 0, 0, 0, 0, 250]
    //   [1, 2, 3, 0] -> [1, 2, 3, 0, 0, 0, 0, 0, 251]
    //   [1, 2, 3, 4, 5, 6, 7, 8] -> [1, 2, 3, 4, 5, 6, 7, 8, 255, 0, 0, 0, 0, 0, 0, 0, 0, 247]
    // ```
    //
    // Refer: https://github.com/facebook/mysql-5.6/wiki/MyRocks-record-format#memcomparable-format
    fn encode<B: MemBufMut>(&self, buf: &mut B, value: &[u8]) -> Result<()> {
        let value_len = value.len();
        for idx in (0..=value_len).step_by(ENC_GROUP_SIZE) {
            let remain = value_len - idx;
            let mut pad_count = 0;
            if remain >= ENC_GROUP_SIZE {
                buf.write_slice(&value[idx..idx + ENC_GROUP_SIZE])
                    .context(EncodeValue)?;
            } else {
                pad_count = ENC_GROUP_SIZE - remain;
                buf.write_slice(&value[idx..]).context(EncodeValue)?;
                buf.write_slice(&PADS[..pad_count]).context(EncodeValue)?;
            }
            let marker = ENC_MARKER - pad_count as u8;
            buf.write_u8(marker).context(EncodeValue)?;
        }
        Ok(())
    }

    // Allocate more space to avoid unnecessary slice growing.
    // Assume that the byte slice size is about `(len(data) / encGroupSize + 1) *
    // (encGroupSize + 1)` bytes, that is `(len(data) / 8 + 1) * 9` in our
    // implement.
    fn estimate_encoded_size(&self, value: &[u8]) -> usize {
        (value.len() / ENC_GROUP_SIZE + 1) * (ENC_GROUP_SIZE + 1)
    }
}

impl Encoder<Bytes> for MemComparable {
    type Error = Error;

    fn encode<B: MemBufMut>(&self, buf: &mut B, value: &Bytes) -> Result<()> {
        self.encode(buf, &value[..])
    }

    fn estimate_encoded_size(&self, value: &Bytes) -> usize {
        self.estimate_encoded_size(&value[..])
    }
}

impl DecodeTo<BytesMut> for MemComparable {
    type Error = Error;

    // decode Bytes which is encoded by encode Bytes before,
    // returns the leftover bytes and decoded value if no error.
    fn decode_to<B: MemBuf>(&self, buf: &mut B, value: &mut BytesMut) -> Result<()> {
        loop {
            let b = buf.remaining_slice();
            ensure!(b.len() > ENC_GROUP_SIZE, DecodeValueGroup);

            let group_bytes = &b[..ENC_GROUP_SIZE + 1];
            let group = &group_bytes[..ENC_GROUP_SIZE];
            let marker = group_bytes[ENC_GROUP_SIZE];
            let pad_count = usize::from(ENC_MARKER - marker);
            ensure!(
                pad_count <= ENC_GROUP_SIZE,
                DecodeValueMarker { group_bytes }
            );

            let real_group_size = ENC_GROUP_SIZE - pad_count;
            value
                .write_slice(&group[..real_group_size])
                .context(EncodeValue)?;

            if pad_count != 0 {
                // Check validity of padding bytes.
                for v in &group[real_group_size..] {
                    ensure!(*v == ENC_PAD, DecodeValuePadding { group_bytes });
                }
                buf.must_advance(ENC_GROUP_SIZE + 1);

                break;
            }
            buf.must_advance(ENC_GROUP_SIZE + 1);
        }
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use core::cmp::Ordering;

    use super::*;

    struct BytesTest {
        data: Bytes,
        estimate_encoded_size: usize,
    }

    #[test]
    fn test_bytes_codec() {
        let data = vec![
            BytesTest {
                data: Bytes::from_static(b""),
                estimate_encoded_size: 9,
            },
            BytesTest {
                data: Bytes::from_static(b"hello1"),
                estimate_encoded_size: 9,
            },
            BytesTest {
                data: Bytes::from_static(b"hello2"),
                estimate_encoded_size: 9,
            },
            BytesTest {
                data: Bytes::from_static(b"hello3"),
                estimate_encoded_size: 9,
            },
            BytesTest {
                data: Bytes::from_static(&[0x00, 0x01]),
                estimate_encoded_size: 9,
            },
            BytesTest {
                data: Bytes::from_static(&[0xff, 0xff]),
                estimate_encoded_size: 9,
            },
            BytesTest {
                data: Bytes::from_static(&[0x01, 0x00]),
                estimate_encoded_size: 9,
            },
            BytesTest {
                data: Bytes::from_static(b"abc"),
                estimate_encoded_size: 9,
            },
            BytesTest {
                data: Bytes::from_static(b"hello world"),
                estimate_encoded_size: 18,
            },
        ];

        let c = MemComparable;
        let mut buf = vec![];
        for x in &data {
            c.encode(&mut buf, &x.data).unwrap();
            assert_eq!(x.estimate_encoded_size, c.estimate_encoded_size(&x.data));
        }

        let mut buf = &buf[..];
        for x in &data {
            let mut d = BytesMut::new();
            c.decode_to(&mut buf, &mut d).unwrap();
            assert_eq!(d, x.data);
        }
    }

    struct TbBytes {
        arg1: Bytes,
        arg2: Bytes,
        ret: Ordering,
    }

    #[test]
    fn test_bytes_order() {
        let data = vec![
            TbBytes {
                arg1: Bytes::new(),
                arg2: Bytes::from_static(&[0x00]),
                ret: Ordering::Less,
            },
            TbBytes {
                arg1: Bytes::from_static(&[0x00]),
                arg2: Bytes::from_static(&[0x00]),
                ret: Ordering::Equal,
            },
            TbBytes {
                arg1: Bytes::from_static(&[0xFF]),
                arg2: Bytes::from_static(&[0x00]),
                ret: Ordering::Greater,
            },
            TbBytes {
                arg1: Bytes::from_static(&[0xFF]),
                arg2: Bytes::from_static(&[0xFF, 0x00]),
                ret: Ordering::Less,
            },
            TbBytes {
                arg1: Bytes::from_static(b"a"),
                arg2: Bytes::from_static(b"b"),
                ret: Ordering::Less,
            },
            TbBytes {
                arg1: Bytes::from_static(b"a"),
                arg2: Bytes::from_static(&[0x00]),
                ret: Ordering::Greater,
            },
            TbBytes {
                arg1: Bytes::from_static(&[0x00]),
                arg2: Bytes::from_static(&[0x01]),
                ret: Ordering::Less,
            },
            TbBytes {
                arg1: Bytes::from_static(&[0x00, 0x01]),
                arg2: Bytes::from_static(&[0x00, 0x00]),
                ret: Ordering::Greater,
            },
            TbBytes {
                arg1: Bytes::from_static(&[0x00, 0x00, 0x00]),
                arg2: Bytes::from_static(&[0x00, 0x00]),
                ret: Ordering::Greater,
            },
            TbBytes {
                arg1: Bytes::from_static(&[0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00]),
                arg2: Bytes::from_static(&[0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00]),
                ret: Ordering::Less,
            },
            TbBytes {
                arg1: Bytes::from_static(&[0x01, 0x02, 0x03, 0x00]),
                arg2: Bytes::from_static(&[0x01, 0x02, 0x03]),
                ret: Ordering::Greater,
            },
            TbBytes {
                arg1: Bytes::from_static(&[0x01, 0x03, 0x03, 0x04]),
                arg2: Bytes::from_static(&[0x01, 0x03, 0x03, 0x05]),
                ret: Ordering::Less,
            },
            TbBytes {
                arg1: Bytes::from_static(&[0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07]),
                arg2: Bytes::from_static(&[0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08]),
                ret: Ordering::Less,
            },
            TbBytes {
                arg1: Bytes::from_static(&[0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09]),
                arg2: Bytes::from_static(&[0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08]),
                ret: Ordering::Greater,
            },
            TbBytes {
                arg1: Bytes::from_static(&[0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x00]),
                arg2: Bytes::from_static(&[0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08]),
                ret: Ordering::Greater,
            },
        ];
        let c = MemComparable;
        for x in &data {
            let mut buf1 = vec![];
            let mut buf2 = vec![];
            c.encode(&mut buf1, &x.arg1).unwrap();
            c.encode(&mut buf2, &x.arg2).unwrap();
            assert_eq!(x.ret, buf1.as_slice().cmp(buf2.as_slice()));
        }
    }
}
