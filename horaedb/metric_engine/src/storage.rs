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

use anyhow::Context;
use arrow::{
    array::{Int64Array, RecordBatch},
    datatypes::SchemaRef,
};
use async_trait::async_trait;
use datafusion::logical_expr::Expr;
use macros::ensure;
use object_store::path::Path;
use parquet::{
    arrow::{async_writer::ParquetObjectWriter, AsyncArrowWriter},
    file::properties::WriterProperties,
};

use crate::{
    manifest::Manifest,
    sst::{allocate_id, FileId, FileMeta},
    types::{ObjectStoreRef, SendableRecordBatchStream, TimeRange, Timestamp},
    Result,
};

pub struct WriteRequest {
    batch: RecordBatch,
    props: Option<WriterProperties>,
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
    fn schema(&self) -> &SchemaRef;

    async fn write(&self, req: WriteRequest) -> Result<()>;

    /// Implementation shoule ensure that the returned stream is sorted by time,
    /// from old to latest.
    async fn scan(&self, req: ScanRequest) -> Result<SendableRecordBatchStream>;

    async fn compact(&self, req: CompactRequest) -> Result<()>;
}

/// `TimeMergeStorage` implementation using cloud object storage.
pub struct CloudObjectStorage {
    path: String,
    store: ObjectStoreRef,
    arrow_schema: SchemaRef,
    timestamp_index: usize,
    manifest: Manifest,
}

/// It will organize the data in the following way:
/// ```plaintext
/// {root_path}/manifest/snapshot
/// {root_path}/manifest/timestamp1
/// {root_path}/manifest/timestamp2
/// {root_path}/manifest/...
/// {root_path}/data/timestamp_a.sst
/// {root_path}/data/timestamp_b.sst
/// {root_path}/data/...
/// ```
impl CloudObjectStorage {
    pub async fn try_new(
        root_path: String,
        store: ObjectStoreRef,
        arrow_schema: SchemaRef,
        timestamp_index: usize,
    ) -> Result<Self> {
        let manifest_prefix = crate::manifest::PREFIX_PATH;
        let manifest =
            Manifest::try_new(format!("{root_path}/{manifest_prefix}"), store.clone()).await?;
        Ok(Self {
            path: root_path,
            timestamp_index,
            store,
            arrow_schema,
            manifest,
        })
    }

    fn build_file_path(&self, id: FileId) -> String {
        let root = &self.path;
        let prefix = crate::sst::PREFIX_PATH;
        format!("{root}/{prefix}/{id}")
    }

    async fn write_batch(&self, req: WriteRequest) -> Result<FileId> {
        let file_id = allocate_id();
        let file_path = self.build_file_path(file_id);
        let object_store_writer =
            ParquetObjectWriter::new(self.store.clone(), Path::from(file_path));
        let mut writer =
            AsyncArrowWriter::try_new(object_store_writer, self.schema().clone(), req.props)
                .context("create arrow writer")?;

        // TODO: sort record batch according to primary key columns.
        writer
            .write(&req.batch)
            .await
            .context("write arrow batch")?;
        writer.close().await.context("close arrow writer")?;

        Ok(file_id)
    }
}

#[async_trait]
impl TimeMergeStorage for CloudObjectStorage {
    fn schema(&self) -> &SchemaRef {
        &self.arrow_schema
    }

    async fn write(&self, req: WriteRequest) -> Result<()> {
        ensure!(req.batch.schema_ref().eq(self.schema()), "schema not match");

        let num_rows = req.batch.num_rows();
        let time_column = req
            .batch
            .column(self.timestamp_index)
            .as_any()
            .downcast_ref::<Int64Array>()
            .context("timestamp column should be int64")?;

        let mut start = Timestamp::MAX;
        let mut end = Timestamp::MIN;
        for v in time_column.values() {
            start = start.min(*v);
            end = end.max(*v);
        }
        let time_range = TimeRange {
            start,
            end: end + 1,
        };
        let file_id = self.write_batch(req).await?;
        let file_meta = FileMeta {
            max_sequence: file_id, // Since file_id in increasing order, we can use it as sequence.
            num_rows: num_rows as u32,
            time_range,
        };
        self.manifest.add_file(file_id, file_meta).await?;

        Ok(())
    }

    async fn scan(&self, req: ScanRequest) -> Result<SendableRecordBatchStream> {
        todo!()
    }

    async fn compact(&self, req: CompactRequest) -> Result<()> {
        todo!()
    }
}
