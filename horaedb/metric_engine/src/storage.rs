// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use arrow::{array::RecordBatch, datatypes::Schema};
use async_trait::async_trait;
use datafusion::logical_expr::Expr;

use crate::{
    manifest::Manifest,
    sst::SSTable,
    types::{ObjectStoreRef, SendableRecordBatchStream, TimeRange},
    Result,
};

pub struct WriteRequest {
    batch: RecordBatch,
}

pub struct ScanRequest {
    range: TimeRange,
    predicate: Expr,
    /// `None` means all columns.
    projections: Option<Vec<usize>>,
}

pub struct CompactRequest {}

/// Time-aware merge storage interface.
#[async_trait]
pub trait TimeMergeStorage {
    fn schema(&self) -> Result<&Schema>;

    async fn write(&self, req: WriteRequest) -> Result<()>;

    /// Implementation shoule ensure that the returned stream is sorted by time,
    /// from old to latest.
    async fn scan(&self, req: ScanRequest) -> Result<SendableRecordBatchStream>;

    async fn compact(&self, req: CompactRequest) -> Result<()>;
}

/// TMStorage implementation using cloud object storage.
pub struct CloudObjectStorage {
    name: String,
    id: u64,
    store: ObjectStoreRef,
    sstables: Vec<SSTable>,
    manifest: Manifest,
}

impl CloudObjectStorage {
    pub fn new(name: String, id: u64, store: ObjectStoreRef) -> Self {
        Self {
            name,
            id,
            store,
            sstables: Vec::new(),
            manifest: Manifest::new(id),
        }
    }
}

#[async_trait]
impl TimeMergeStorage for CloudObjectStorage {
    fn schema(&self) -> Result<&Schema> {
        todo!()
    }

    async fn write(&self, req: WriteRequest) -> Result<()> {
        todo!()
    }

    async fn scan(&self, req: ScanRequest) -> Result<SendableRecordBatchStream> {
        todo!()
    }

    async fn compact(&self, req: CompactRequest) -> Result<()> {
        todo!()
    }
}
