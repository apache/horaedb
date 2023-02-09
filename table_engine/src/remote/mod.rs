// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

//! Remote table engine

pub mod model;

use std::sync::Arc;

use async_trait::async_trait;
use common_util::{define_result, error::GenericError};
use model::{ReadRequest, WriteRequest};
use snafu::Snafu;

use crate::stream::SendableRecordBatchStream;

#[derive(Debug, Snafu)]
#[snafu(visibility(pub))]
pub enum Error {
    #[snafu(display("Failed to read from remote, err:{}", source))]
    Read { source: GenericError },

    #[snafu(display("Failed to write to remote, err:{}", source))]
    Write { source: GenericError },
}

define_result!(Error);

/// Remote table engine interface
#[async_trait]
pub trait RemoteEngine: Send + Sync {
    /// Read from the remote engine.
    async fn read(&self, request: ReadRequest) -> Result<SendableRecordBatchStream>;

    /// Write to the remote engine.
    async fn write(&self, request: WriteRequest) -> Result<usize>;
}

/// Remote engine reference
pub type RemoteEngineRef = Arc<dyn RemoteEngine>;
