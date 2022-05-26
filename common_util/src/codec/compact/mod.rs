// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

//! Mem compact format codec

// Implementation reference:
// https://github.com/pingcap/tidb/blob/bd011d3c9567c506d8d4343ade03edf77fcd5b56/util/codec/codec.go
mod bytes;
mod datum;
mod float;
mod number;

use common_types::bytes::MemBuf;
use snafu::{ensure, Backtrace, ResultExt, Snafu};

use crate::codec::consts;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Failed to encode flag, err:{}", source))]
    EncodeKey { source: common_types::bytes::Error },

    #[snafu(display("Failed to encode value, err:{}", source))]
    EncodeValue { source: common_types::bytes::Error },

    #[snafu(display("Failed to encode varint, err:{}", source))]
    EncodeVarint { source: crate::codec::varint::Error },

    #[snafu(display("Failed to decode varint, err:{}", source))]
    DecodeVarint { source: crate::codec::varint::Error },

    #[snafu(display("Failed to decode key, err:{}", source))]
    DecodeKey { source: common_types::bytes::Error },

    #[snafu(display("Insufficient bytes to decode value.\nBacktrace:\n{}", backtrace))]
    DecodeEmptyValue { backtrace: Backtrace },

    #[snafu(display(
        "Invalid flag, expect:{}, actual:{}.\nBacktrace:\n{}",
        expect,
        actual,
        backtrace
    ))]
    InvalidKeyFlag {
        expect: u8,
        actual: u8,
        backtrace: Backtrace,
    },

    #[snafu(display("Insufficient bytes to decode value, err:{}", source))]
    DecodeValue { source: common_types::bytes::Error },

    #[snafu(display("Try into usize error:{}.\nBacktrace:\n{}", source, backtrace))]
    TryIntoUsize {
        source: std::num::TryFromIntError,
        backtrace: Backtrace,
    },

    #[snafu(display("Failed to decode string, err:{}", source))]
    DecodeString { source: common_types::string::Error },

    #[snafu(display("Datum cannot be null.\nBacktrace:\n{}", backtrace))]
    NullDatum { backtrace: Backtrace },
}

define_result!(Error);

/// Mem compact encoder
pub struct MemCompactEncoder;

/// Mem compact decoder
pub struct MemCompactDecoder;

impl MemCompactDecoder {
    /// Returns None if we need to return null datum, otherwise return the flag.
    fn maybe_read_null<B: MemBuf>(&self, buf: &mut B) -> Result<Option<u8>> {
        let actual = buf.read_u8().context(DecodeKey)?;
        // If actual flag is null, need to check whether this datum is nullable.
        if actual == consts::NULL_FLAG {
            // The decoder need to return null datum.
            return Ok(None);
        }

        Ok(Some(actual))
    }

    #[inline]
    fn ensure_flag(expect: u8, actual: u8) -> Result<()> {
        // Actual flag is not null.
        ensure!(expect == actual, InvalidKeyFlag { expect, actual });
        Ok(())
    }
}
