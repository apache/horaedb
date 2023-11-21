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
use common_types::{SequenceNumber, MIN_SEQUENCE_NUMBER};

use crate::{
    log_batch::LogWriteBatch,
    manager::{
        BatchLogIteratorAdapter, ReadContext, ReadRequest, RegionId, Result, ScanContext,
        ScanRequest, WalLocation, WalManager, WriteContext,
    },
};

/// This is a special wal manager which does nothing.
/// It could be used for testing or when latest data is allowed to lost.
#[derive(Debug)]
pub struct DoNothing;

#[async_trait]
impl WalManager for DoNothing {
    async fn sequence_num(&self, _location: WalLocation) -> Result<SequenceNumber> {
        // Since this wal will not persist any data, so we will always return
        // MIN_SEQUENCE_NUMBER to indicate this is a special wal.
        Ok(MIN_SEQUENCE_NUMBER)
    }

    async fn mark_delete_entries_up_to(
        &self,
        _location: WalLocation,
        _sequence_num: SequenceNumber,
    ) -> Result<()> {
        Ok(())
    }

    async fn close_region(&self, _region: RegionId) -> Result<()> {
        Ok(())
    }

    async fn close_gracefully(&self) -> Result<()> {
        Ok(())
    }

    async fn read_batch(
        &self,
        _ctx: &ReadContext,
        _req: &ReadRequest,
    ) -> Result<BatchLogIteratorAdapter> {
        Ok(BatchLogIteratorAdapter::empty())
    }

    async fn write(&self, _ctx: &WriteContext, _batch: &LogWriteBatch) -> Result<SequenceNumber> {
        Ok(MIN_SEQUENCE_NUMBER)
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
