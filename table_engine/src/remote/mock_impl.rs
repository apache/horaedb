// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

//! Mock impl for remote table engine

use async_trait::async_trait;
pub struct MockImpl;

#[async_trait]
impl RemoteEngine for MockImpl {
    /// Read from the remote engine
    async fn read(&self, request: ReadRequest) -> Result<SendableRecordBatchStream> {
        todo!()
    }

    /// Write to the remote engine
    async fn write(&self, request: WriteRequest) -> Result<usize> {
        todo!()
    }
}
