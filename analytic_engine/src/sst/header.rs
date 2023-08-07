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

// The header parser for one sst.

use bytes_ext::Bytes;
use macros::define_result;
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

    /// Detect the storage format by parsing header of the sst.
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
