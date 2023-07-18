// Copyright 2022-2023 CeresDB Project Authors. Licensed under Apache-2.0.

//! Number format

use common_types::bytes::{SafeBuf, SafeBufMut};
use snafu::ResultExt;

use crate::{
    consts,
    memcomparable::{DecodeValue, EncodeValue, Error, MemComparable, Result},
    DecodeTo, Encoder,
};

impl Encoder<i64> for MemComparable {
    type Error = Error;

    fn encode<B: SafeBufMut>(&self, buf: &mut B, value: &i64) -> Result<()> {
        buf.try_put_u64(encode_int_to_cmp_uint(*value))
            .context(EncodeValue)?;
        Ok(())
    }

    fn estimate_encoded_size(&self, _value: &i64) -> usize {
        // flag + u64
        9
    }
}

impl DecodeTo<i64> for MemComparable {
    type Error = Error;

    fn decode_to<B: SafeBuf>(&self, buf: &mut B, value: &mut i64) -> Result<()> {
        *value = decode_cmp_uint_to_int(buf.try_get_u64().context(DecodeValue)?);
        Ok(())
    }
}

// encode_int_to_cmp_uint make int v to comparable uint type
fn encode_int_to_cmp_uint(v: i64) -> u64 {
    (v as u64) ^ consts::SIGN_MASK
}

// decode_cmp_uint_to_int decodes the u that encoded by encode_int_to_cmp_uint
fn decode_cmp_uint_to_int(u: u64) -> i64 {
    (u ^ consts::SIGN_MASK) as i64
}

impl Encoder<u64> for MemComparable {
    type Error = Error;

    fn encode<B: SafeBufMut>(&self, buf: &mut B, value: &u64) -> Result<()> {
        buf.try_put_u64(*value).context(EncodeValue)?;
        Ok(())
    }

    fn estimate_encoded_size(&self, _value: &u64) -> usize {
        // flag + u64
        9
    }
}

impl DecodeTo<u64> for MemComparable {
    type Error = Error;

    fn decode_to<B: SafeBuf>(&self, buf: &mut B, value: &mut u64) -> Result<()> {
        *value = buf.try_get_u64().context(DecodeValue)?;
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use core::cmp::Ordering;

    use super::*;

    struct TestI64 {
        data: i64,
        estimate_encoded_size: usize,
    }

    impl TestI64 {
        fn new(data: i64) -> Self {
            Self {
                data,
                estimate_encoded_size: 9,
            }
        }
    }

    #[test]
    fn test_i64_codec() {
        let data = vec![
            TestI64::new(1621324705),
            TestI64::new(1621324705000),
            TestI64::new(1521324705),
            TestI64::new(1621324705123),
            TestI64::new(i64::MIN),
            TestI64::new(i64::MIN + 1),
            TestI64::new(0),
            TestI64::new(i64::MAX),
            TestI64::new((1 << 47) - 1),
            TestI64::new(-1 << 47),
            TestI64::new((1 << 23) - 1),
            TestI64::new(-1 << 23),
            TestI64::new((1 << 33) - 1),
            TestI64::new(-1 << 33),
            TestI64::new((1 << 55) - 1),
            TestI64::new(-1 << 55),
            TestI64::new(1),
            TestI64::new(-1),
        ];
        let c = MemComparable;
        let mut buf = vec![];
        for x in &data {
            c.encode(&mut buf, &x.data).unwrap();
            assert_eq!(x.estimate_encoded_size, c.estimate_encoded_size(&x.data));
        }

        let mut buf = &buf[..];
        for x in &data {
            let mut d = -1;
            c.decode_to(&mut buf, &mut d).unwrap();
            assert_eq!(d, x.data);
        }
    }

    struct TestU64 {
        data: u64,
        estimate_encoded_size: usize,
    }

    impl TestU64 {
        fn new(data: u64) -> Self {
            Self {
                data,
                estimate_encoded_size: 9,
            }
        }
    }

    #[test]
    fn test_u64_codec() {
        let data = vec![
            TestU64::new(0),
            TestU64::new(u64::from(u8::MAX)),
            TestU64::new(u64::from(u16::MAX)),
            TestU64::new(u64::from(u32::MAX)),
            TestU64::new(u64::MAX),
            TestU64::new((1 << 24) - 1),
            TestU64::new((1 << 48) - 1),
            TestU64::new((1 << 56) - 1),
            TestU64::new(1),
            TestU64::new(i8::MAX as u64),
            TestU64::new(i16::MAX as u64),
            TestU64::new(i32::MAX as u64),
            TestU64::new(i64::MAX as u64),
        ];
        let c = MemComparable;
        let mut buf = vec![];
        for x in &data {
            c.encode(&mut buf, &x.data).unwrap();
            assert_eq!(x.estimate_encoded_size, c.estimate_encoded_size(&x.data));
        }

        let mut buf = &buf[..];
        for x in &data {
            let mut d = 0;
            c.decode_to(&mut buf, &mut d).unwrap();
            assert_eq!(d, x.data);
        }
    }

    struct TblI64 {
        arg1: i64,
        arg2: i64,
        ret: Ordering,
    }

    #[test]
    fn test_i64_order() {
        let data = vec![
            TblI64 {
                arg1: -1,
                arg2: 1,
                ret: Ordering::Less,
            },
            TblI64 {
                arg1: i64::MAX,
                arg2: i64::MIN,
                ret: Ordering::Greater,
            },
            TblI64 {
                arg1: i64::MAX,
                arg2: i32::MAX as i64,
                ret: Ordering::Greater,
            },
            TblI64 {
                arg1: i32::MIN as i64,
                arg2: i16::MAX as i64,
                ret: Ordering::Less,
            },
            TblI64 {
                arg1: i64::MIN,
                arg2: i8::MAX as i64,
                ret: Ordering::Less,
            },
            TblI64 {
                arg1: 0,
                arg2: i8::MAX as i64,
                ret: Ordering::Less,
            },
            TblI64 {
                arg1: i8::MIN as i64,
                arg2: 0,
                ret: Ordering::Less,
            },
            TblI64 {
                arg1: i16::MIN as i64,
                arg2: i16::MAX as i64,
                ret: Ordering::Less,
            },
            TblI64 {
                arg1: 1,
                arg2: -1,
                ret: Ordering::Greater,
            },
            TblI64 {
                arg1: 1,
                arg2: 0,
                ret: Ordering::Greater,
            },
            TblI64 {
                arg1: -1,
                arg2: 0,
                ret: Ordering::Less,
            },
            TblI64 {
                arg1: 0,
                arg2: 0,
                ret: Ordering::Equal,
            },
            TblI64 {
                arg1: i16::MAX as i64,
                arg2: i16::MAX as i64,
                ret: Ordering::Equal,
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

    struct TblU64 {
        arg1: u64,
        arg2: u64,
        ret: Ordering,
    }

    #[test]
    fn test_u64_order() {
        let data = vec![
            TblU64 {
                arg1: 0,
                arg2: 0,
                ret: Ordering::Equal,
            },
            TblU64 {
                arg1: 1,
                arg2: 0,
                ret: Ordering::Greater,
            },
            TblU64 {
                arg1: 0,
                arg2: 1,
                ret: Ordering::Less,
            },
            TblU64 {
                arg1: i8::MAX as u64,
                arg2: i16::MAX as u64,
                ret: Ordering::Less,
            },
            TblU64 {
                arg1: u32::MAX as u64,
                arg2: i32::MAX as u64,
                ret: Ordering::Greater,
            },
            TblU64 {
                arg1: u8::MAX as u64,
                arg2: i8::MAX as u64,
                ret: Ordering::Greater,
            },
            TblU64 {
                arg1: u16::MAX as u64,
                arg2: i32::MAX as u64,
                ret: Ordering::Less,
            },
            TblU64 {
                arg1: u64::MAX,
                arg2: i64::MAX as u64,
                ret: Ordering::Greater,
            },
            TblU64 {
                arg1: i64::MAX as u64,
                arg2: u32::MAX as u64,
                ret: Ordering::Greater,
            },
            TblU64 {
                arg1: u64::MAX,
                arg2: 0,
                ret: Ordering::Greater,
            },
            TblU64 {
                arg1: 0,
                arg2: u64::MAX,
                ret: Ordering::Less,
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
