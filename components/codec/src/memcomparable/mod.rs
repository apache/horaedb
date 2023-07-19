// Copyright 2022-2023 CeresDB Project Authors. Licensed under Apache-2.0.

//! Mem comparable format codec

// Implementation reference:
// https://github.com/pingcap/tidb/blob/bd011d3c9567c506d8d4343ade03edf77fcd5b56/util/codec/codec.go

mod bytes;
mod datum;
mod number;

use bytes_ext::{BytesMut, SafeBuf};
use common_types::datum::DatumKind;
use macros::define_result;
use snafu::{ensure, Backtrace, ResultExt, Snafu};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Failed to encode flag, err:{}", source))]
    EncodeKey { source: bytes_ext::Error },

    #[snafu(display("Failed to encode value, err:{}", source))]
    EncodeValue { source: bytes_ext::Error },

    #[snafu(display("Failed to decode key, err:{}", source))]
    DecodeKey { source: bytes_ext::Error },

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

    #[snafu(display(
        "Unsupported datum kind to compare in mem, kind :{}.\nBacktrace:\n{}",
        kind,
        backtrace
    ))]
    UnsupportedKind {
        kind: DatumKind,
        backtrace: Backtrace,
    },

    #[snafu(display("Insufficient bytes to decode value, err:{}", source))]
    DecodeValue { source: bytes_ext::Error },

    #[snafu(display("Insufficient bytes to decode value group.\nBacktrace:\n{}", backtrace))]
    DecodeValueGroup { backtrace: Backtrace },

    #[snafu(display(
        "Invalid marker byte, group bytes: {:?}.\nBacktrace:\n{}",
        group_bytes,
        backtrace
    ))]
    DecodeValueMarker {
        group_bytes: BytesMut,
        backtrace: Backtrace,
    },

    #[snafu(display(
        "Invalid padding byte, group bytes: {:?}.\nBacktrace:\n{}",
        group_bytes,
        backtrace
    ))]
    DecodeValuePadding {
        group_bytes: BytesMut,
        backtrace: Backtrace,
    },

    #[snafu(display("Failed to skip padding bytes, err:{}.", source))]
    SkipPadding { source: bytes_ext::Error },

    #[snafu(display("Failed to decode string, err:{}", source))]
    DecodeString { source: common_types::string::Error },
}

define_result!(Error);

/// Mem comparable codec
pub struct MemComparable;

impl MemComparable {
    fn ensure_flag<B: SafeBuf>(buf: &mut B, flag: u8) -> Result<()> {
        let actual = buf.try_get_u8().context(DecodeKey)?;
        ensure!(
            flag == actual,
            InvalidKeyFlag {
                expect: flag,
                actual
            }
        );
        Ok(())
    }
}
