// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

//! Remote table engine

pub mod model;

use async_trait::async_trait;
use common_util::define_result;
use model::{ReadRequest, WriteRequest};
use snafu::Snafu;

use crate::stream::SendableRecordBatchStream;

#[derive(Debug, Snafu)]
pub enum Error {}

define_result!(Error);

/// Remote table engine interface
#[async_trait]
pub trait RemoteEngine {
    /// Read from the remote engine
    async fn read(&self, request: ReadRequest) -> Result<SendableRecordBatchStream>;

    /// Write to the remote engine
    async fn write(&self, request: WriteRequest) -> Result<usize>;
}
