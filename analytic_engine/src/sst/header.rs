// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

// The header parser for one sst.

use bytes::Bytes;
use common_util::define_result;
use object_store::{ObjectStoreRef, Path};
use parquet::data_type::AsBytes;
use snafu::{Backtrace, ResultExt, Snafu};

use crate::table_options::StorageFormat;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Failed to read header bytes, err:{}", source,))]
    ReadHeaderBytes {
        source: object_store::ObjectStoreError,
    },
    #[snafu(display(
        "Unknown header, header value:{:?}.\nBacktrace:\n{}",
        header_value,
        backtrace
    ))]
    UnknownHeader {
        header_value: Bytes,
        backtrace: Backtrace,
    },
}

define_result!(Error);

/// A parser for decoding the header of SST.
///
/// Assume that every SST shares the same encoding format:
///
/// +------------+----------------------+
/// | 4B(header) |       Payload        |
/// +------------+----------------------+
pub struct HeaderParser<'a> {
    path: &'a Path,
    store: &'a ObjectStoreRef,
}

impl<'a> HeaderParser<'a> {
    const HEADER_LEN: usize = 4;
    const PARQUET: &'static [u8] = b"PAR1";

    pub fn new(path: &'a Path, store: &'a ObjectStoreRef) -> HeaderParser<'a> {
        Self { path, store }
    }

    pub async fn parse(&self) -> Result<StorageFormat> {
        let header_value = self
            .store
            .get_range(self.path, 0..Self::HEADER_LEN)
            .await
            .context(ReadHeaderBytes)?;

        match header_value.as_bytes() {
            Self::PARQUET => Ok(StorageFormat::Columnar),
            _ => UnknownHeader { header_value }.fail(),
        }
    }
}
