// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

//! Remote table engine

pub mod model;

use async_trait::async_trait;
use common_util::define_result;
use model::{ReadRequest, WriteRequest};
use snafu::{Backtrace, Snafu};

use crate::stream::SendableRecordBatchStream;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Empty table identifier.\nBacktrace:\n{}", backtrace))]
    EmptyTableIdentifier { backtrace: Backtrace },

    #[snafu(display("Empty table read request.\nBacktrace:\n{}", backtrace))]
    EmptyTableReadRequest { backtrace: Backtrace },

    #[snafu(display("Empty table schema.\nBacktrace:\n{}", backtrace))]
    EmptyTableSchema { backtrace: Backtrace },

    #[snafu(display("Empty row group.\nBacktrace:\n{}", backtrace))]
    EmptyRowGroup { backtrace: Backtrace },

    #[snafu(display("Failed to covert table read request, err:{}", source))]
    ConvertTableReadRequest {
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display("Failed to covert table schema, err:{}", source))]
    ConvertTableSchema {
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display("Failed to covert row group, err:{}", source))]
    ConvertRowGroup {
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display(
        "Failed to covert row group, encode version:{}.\nBacktrace:\n{}",
        version,
        backtrace
    ))]
    UnsupportedConvertRowGroup { version: u32, backtrace: Backtrace },
}

define_result!(Error);

/// Remote table engine interface
#[async_trait]
pub trait RemoteEngine {
    /// Read from the remote engine
    async fn read(&self, request: ReadRequest) -> Result<SendableRecordBatchStream>;

    /// Write to the remote engine
    async fn write(&self, request: WriteRequest) -> Result<usize>;
}
