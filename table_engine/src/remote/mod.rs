// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

//! Remote table engine

pub mod model;

use async_trait::async_trait;
use common_util::define_result;
use model::{ReadRequest, WriteRequest};
use snafu::Snafu;

use crate::stream::SendableRecordBatchStream;

#[derive(Debug, Snafu)]
#[snafu(visibility(pub))]
pub enum Error {
    #[snafu(display("Failed to read from remote, err:{}", source))]
    Read {
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display("Failed to write to remote, err:{}", source))]
    Write {
        source: Box<dyn std::error::Error + Send + Sync>,
    },
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
