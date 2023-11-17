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

use async_trait::async_trait;
use common_types::SequenceNumber;

use crate::{
    log_batch::LogWriteBatch,
    manager::{
        BatchLogIteratorAdapter, ReadContext, ReadRequest, RegionId, Result, ScanContext,
        ScanRequest, WalLocation, WalManager, WriteContext,
    },
};

#[derive(Debug)]
pub struct DoNothing;

#[async_trait]
impl WalManager for DoNothing {
    /// Get current sequence number.
    async fn sequence_num(&self, _location: WalLocation) -> Result<SequenceNumber> {
        Ok(0)
    }

    /// Mark the entries whose sequence number is in [0, `sequence_number`] to
    /// be deleted in the future.
    async fn mark_delete_entries_up_to(
        &self,
        _location: WalLocation,
        _sequence_num: SequenceNumber,
    ) -> Result<()> {
        Ok(())
    }

    /// Close a region.
    async fn close_region(&self, _region: RegionId) -> Result<()> {
        Ok(())
    }

    /// Close the wal gracefully.
    async fn close_gracefully(&self) -> Result<()> {
        Ok(())
    }

    /// Provide iterator on necessary entries according to `ReadRequest`.
    async fn read_batch(
        &self,
        _ctx: &ReadContext,
        _req: &ReadRequest,
    ) -> Result<BatchLogIteratorAdapter> {
        Ok(BatchLogIteratorAdapter::empty())
    }

    async fn write(&self, _ctx: &WriteContext, _batch: &LogWriteBatch) -> Result<SequenceNumber> {
        Ok(0)
    }

    async fn scan(
        &self,
        _ctx: &ScanContext,
        _req: &ScanRequest,
    ) -> Result<BatchLogIteratorAdapter> {
        Ok(BatchLogIteratorAdapter::empty())
    }

    async fn get_statistics(&self) -> Option<String> {
        None
    }
}
