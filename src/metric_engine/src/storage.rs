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

use std::{sync::Arc, time::Duration, vec};

use anyhow::Context;
use arrow::{
    array::{RecordBatch, UInt64Array},
    datatypes::SchemaRef,
};
use arrow_schema::{DataType, Field, Schema};
use async_trait::async_trait;
use datafusion::{
    self,
    common::DFSchema,
    execution::{context::ExecutionProps, SendableRecordBatchStream},
    logical_expr::Expr,
    physical_expr::LexOrdering,
    physical_plan::{
        execute_stream, memory::MemoryExec, sorts::sort::SortExec, union::UnionExec,
        EmptyRecordBatchStream,
    },
    physical_planner::create_physical_sort_exprs,
    prelude::{ident, SessionContext},
};
use futures::StreamExt;
use itertools::Itertools;
use object_store::path::Path;
use parquet::{
    arrow::{async_writer::ParquetObjectWriter, AsyncArrowWriter},
    file::properties::WriterProperties,
    format::SortingColumn,
    schema::types::ColumnPath,
};
use tokio::runtime::Runtime;

use crate::{
    compaction::CompactionScheduler,
    config::{StorageConfig, WriteConfig},
    ensure,
    manifest::{Manifest, ManifestRef},
    read::ParquetReader,
    sst::{FileMeta, SstFile, SstPathGenerator},
    types::{ObjectStoreRef, StorageSchema, TimeRange, WriteResult, SEQ_COLUMN_NAME},
    Result,
};

pub struct WriteRequest {
    pub batch: RecordBatch,
    pub time_range: TimeRange,
    // Check data is valid if it's true.
    pub enable_check: bool,
}

pub struct ScanRequest {
    pub range: TimeRange,
    pub predicate: Vec<Expr>,
    /// `None` means all columns.
    pub projections: Option<Vec<usize>>,
}

#[derive(Default)]
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

pub type TimeMergeStorageRef = Arc<(dyn TimeMergeStorage + Send + Sync)>;

#[derive(Clone)]
pub struct StorageRuntimes {
    manifest_compact_runtime: Arc<Runtime>,
    sst_compact_runtime: Arc<Runtime>,
}

impl StorageRuntimes {
    pub fn new(manifest_compact_runtime: Arc<Runtime>, sst_compact_runtime: Arc<Runtime>) -> Self {
        Self {
            manifest_compact_runtime,
            sst_compact_runtime,
        }
    }
}

/// `TimeMergeStorage` implementation using cloud object storage, it will split
/// data into different segments(aka `segment_duration`) based time range.
///
/// Compaction will be done by merging segments within a segment, and segment
/// will make it easy to support expiration.
#[allow(dead_code)]
pub struct CloudObjectStorage {
    segment_duration: Duration,
    path: String,
    store: ObjectStoreRef,
    schema: StorageSchema,
    manifest: ManifestRef,
    runtimes: StorageRuntimes,
    parquet_reader: Arc<ParquetReader>,
    write_props: WriterProperties,
    sst_path_gen: Arc<SstPathGenerator>,
    compact_scheduler: CompactionScheduler,
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
/// `root_path` is composed of `path` and `segment_duration`.
impl CloudObjectStorage {
    pub async fn try_new(
        path: String,
        segment_duration: Duration,
        store: ObjectStoreRef,
        arrow_schema: SchemaRef,
        num_primary_keys: usize,
        storage_opts: StorageConfig,
        runtimes: StorageRuntimes,
    ) -> Result<Self> {
        let schema = {
            let value_idxes = (num_primary_keys..arrow_schema.fields.len()).collect::<Vec<_>>();
            ensure!(!value_idxes.is_empty(), "no value column found");

            let mut new_fields = arrow_schema.fields.clone().to_vec();
            new_fields.push(Arc::new(Field::new(
                SEQ_COLUMN_NAME,
                DataType::UInt64,
                true,
            )));
            let seq_idx = new_fields.len() - 1;
            let arrow_schema = Arc::new(Schema::new_with_metadata(
                new_fields,
                arrow_schema.metadata.clone(),
            ));
            let update_mode = storage_opts.update_mode;
            StorageSchema {
                arrow_schema,
                num_primary_keys,
                seq_idx,
                value_idxes,
                update_mode,
            }
        };
        let manifest = Manifest::try_new(
            path.clone(),
            store.clone(),
            runtimes.manifest_compact_runtime.clone(),
            storage_opts.manifest,
        )
        .await?;
        let manifest = Arc::new(manifest);
        let write_props = Self::build_write_props(storage_opts.write, num_primary_keys);
        let sst_path_gen = Arc::new(SstPathGenerator::new(path.clone()));
        let parquet_reader = Arc::new(ParquetReader::new(
            store.clone(),
            schema.clone(),
            sst_path_gen.clone(),
        ));
        let compact_scheduler = CompactionScheduler::new(
            runtimes.sst_compact_runtime.clone(),
            manifest.clone(),
            store.clone(),
            schema.clone(),
            segment_duration,
            sst_path_gen.clone(),
            parquet_reader.clone(),
            storage_opts.scheduler,
            write_props.clone(),
        );
        Ok(Self {
            path,
            schema,
            segment_duration,
            store,
            manifest,
            parquet_reader,
            runtimes,
            write_props,
            sst_path_gen,
            compact_scheduler,
        })
    }

    async fn write_batch(&self, batch: RecordBatch) -> Result<WriteResult> {
        let file_id = SstFile::allocate_id();
        let file_path = self.sst_path_gen.generate(file_id);
        let file_path = Path::from(file_path);
        let object_store_writer = ParquetObjectWriter::new(self.store.clone(), file_path.clone());
        let mut writer = AsyncArrowWriter::try_new(
            object_store_writer,
            self.schema().clone(),
            Some(self.write_props.clone()),
        )
        .context("create arrow writer")?;

        // sort record batch
        let mut batches = self.sort_batch(batch).await?;
        while let Some(batch) = batches.next().await {
            let batch = batch.context("get sorted batch")?;
            let batch_with_seq = {
                let mut new_cols = batch.columns().to_vec();
                // Since file_id in increasing order, we can use it as sequence.
                let seq_column = Arc::new(UInt64Array::from(vec![file_id; batch.num_rows()]));
                new_cols.push(seq_column);
                RecordBatch::try_new(self.schema.arrow_schema.clone(), new_cols)
                    .context("construct record batch with seq column")?
            };
            writer
                .write(&batch_with_seq)
                .await
                .context("write arrow batch")?;
        }
        writer.close().await.context("close arrow writer")?;
        let object_meta = self
            .store
            .head(&file_path)
            .await
            .context("get object meta")?;

        Ok(WriteResult {
            id: file_id,
            seq: file_id,
            size: object_meta.size,
        })
    }

    fn build_sort_exprs(&self, df_schema: &DFSchema, sort_seq: bool) -> Result<LexOrdering> {
        let mut sort_exprs = (0..self.schema.num_primary_keys)
            .map(|i| {
                ident(self.schema().field(i).name())
                    .sort(true /* asc */, true /* nulls_first */)
            })
            .collect::<Vec<_>>();
        if sort_seq {
            sort_exprs.push(ident(SEQ_COLUMN_NAME).sort(true, true));
        }
        let sort_exprs =
            create_physical_sort_exprs(&sort_exprs, df_schema, &ExecutionProps::default())
                .context("create physical sort exprs")?;

        Ok(sort_exprs)
    }

    async fn sort_batch(&self, batch: RecordBatch) -> Result<SendableRecordBatchStream> {
        let ctx = SessionContext::default();
        let schema = batch.schema();
        let df_schema = DFSchema::try_from(self.schema().clone()).context("build DFSchema")?;
        let sort_exprs = self.build_sort_exprs(&df_schema, false /* sort_seq */)?;
        let batch_plan =
            MemoryExec::try_new(&[vec![batch]], schema, None).context("build batch plan")?;
        let physical_plan = Arc::new(SortExec::new(sort_exprs, Arc::new(batch_plan)));

        let res =
            execute_stream(physical_plan, ctx.task_ctx()).context("execute sort physical plan")?;
        Ok(res)
    }

    fn build_write_props(write_options: WriteConfig, num_primary_key: usize) -> WriterProperties {
        let sorting_columns = write_options.enable_sorting_columns.then(|| {
            (0..num_primary_key)
                .map(|i| {
                    SortingColumn::new(i as i32, false /* desc */, true /* nulls_first */)
                })
                .collect::<Vec<_>>()
        });

        let mut builder = WriterProperties::builder()
            .set_max_row_group_size(write_options.max_row_group_size)
            .set_write_batch_size(write_options.write_bacth_size)
            .set_sorting_columns(sorting_columns)
            .set_dictionary_enabled(write_options.enable_dict)
            .set_bloom_filter_enabled(write_options.enable_bloom_filter)
            .set_encoding(write_options.encoding.into())
            .set_compression(write_options.compression.into());

        if write_options.column_options.is_none() {
            return builder.build();
        }

        for (col_name, col_opt) in write_options.column_options.unwrap() {
            let col_path = ColumnPath::new(vec![col_name.to_string()]);
            if let Some(enable_dict) = col_opt.enable_dict {
                builder = builder.set_column_dictionary_enabled(col_path.clone(), enable_dict);
            }
            if let Some(enable_bloom_filter) = col_opt.enable_bloom_filter {
                builder =
                    builder.set_column_bloom_filter_enabled(col_path.clone(), enable_bloom_filter);
            }
            if let Some(encoding) = col_opt.encoding {
                builder = builder.set_column_encoding(col_path.clone(), encoding.into());
            }
            if let Some(compression) = col_opt.compression {
                builder = builder.set_column_compression(col_path, compression.into());
            }
        }

        builder.build()
    }
}

#[async_trait]
impl TimeMergeStorage for CloudObjectStorage {
    fn schema(&self) -> &SchemaRef {
        &self.schema.arrow_schema
    }

    async fn write(&self, req: WriteRequest) -> Result<()> {
        if req.enable_check {
            let segment_duration = self.segment_duration.as_millis() as i64;
            ensure!(
                req.time_range.start.0 / segment_duration
                    == (req.time_range.end.0 - 1) / segment_duration,
                "time range can't cross segment, value:{:?}",
                &req.time_range
            );
        }

        let num_rows = req.batch.num_rows();
        let WriteResult {
            id: file_id,
            seq,
            size: file_size,
        } = self.write_batch(req.batch).await?;
        let file_meta = FileMeta {
            max_sequence: seq,
            num_rows: num_rows as u32,
            size: file_size as u32,
            time_range: req.time_range,
        };
        self.manifest.add_file(file_id, file_meta).await?;

        Ok(())
    }

    async fn scan(&self, req: ScanRequest) -> Result<SendableRecordBatchStream> {
        let total_ssts = self.manifest.find_ssts(&req.range).await;
        if total_ssts.is_empty() {
            return Ok(Box::pin(EmptyRecordBatchStream::new(
                self.schema.arrow_schema.clone(),
            )));
        }

        let ssts_by_segment = total_ssts.into_iter().group_by(|file| {
            file.meta().time_range.start.0 / self.segment_duration.as_millis() as i64
        });

        let mut plan_for_all_segments = Vec::new();
        for (_, ssts) in ssts_by_segment.sorted_by(|a, b| a.0.cmp(&b.0)) {
            let plan = self.parquet_reader.build_df_plan(
                ssts,
                req.projections.clone(),
                req.predicate.clone(),
                false, // keep_sequence
            )?;

            plan_for_all_segments.push(plan);
        }

        let ctx = SessionContext::default();
        if plan_for_all_segments.len() == 1 {
            let res = execute_stream(plan_for_all_segments.remove(0), ctx.task_ctx())
                .context("execute stream")?;
            return Ok(res);
        }

        let union_exec = Arc::new(UnionExec::new(plan_for_all_segments));
        let res = execute_stream(union_exec, ctx.task_ctx()).context("execute stream")?;
        return Ok(res);
    }

    async fn compact(&self, _req: CompactRequest) -> Result<()> {
        self.compact_scheduler.trigger_compaction()
    }
}

#[cfg(test)]
mod tests {
    use object_store::local::LocalFileSystem;
    use test_log::test;

    use super::*;
    use crate::{arrow_schema, record_batch, test_util::check_stream, types::Timestamp};

    fn build_runtimes() -> StorageRuntimes {
        let rt = Arc::new(Runtime::new().unwrap());
        StorageRuntimes::new(rt.clone(), rt)
    }

    #[test(test)]
    fn test_storage_write_and_scan() {
        let schema = arrow_schema!(("pk1", UInt8), ("pk2", UInt8), ("value", Int64));
        let root_dir = temp_dir::TempDir::new().unwrap();
        let store = Arc::new(LocalFileSystem::new());
        let runtimes = build_runtimes();
        runtimes.sst_compact_runtime.clone().block_on(async move {
            let storage = CloudObjectStorage::try_new(
                root_dir.path().to_string_lossy().to_string(),
                Duration::from_hours(2),
                store,
                schema.clone(),
                2, // num_primary_keys
                StorageConfig::default(),
                runtimes,
            )
            .await
            .unwrap();

            let batch = record_batch!(
                ("pk1", UInt8, vec![11, 11, 9, 10, 5]),
                ("pk2", UInt8, vec![100, 100, 1, 2, 3]),
                ("value", Int64, vec![2, 7, 4, 6, 1])
            )
            .unwrap();
            storage
                .write(WriteRequest {
                    batch,
                    time_range: (1..10).into(),
                    enable_check: true,
                })
                .await
                .unwrap();

            let batch = record_batch!(
                ("pk1", UInt8, vec![11, 11, 9, 10]),
                ("pk2", UInt8, vec![100, 99, 1, 2]),
                ("value", Int64, vec![22, 77, 44, 66])
            )
            .unwrap();
            storage
                .write(WriteRequest {
                    batch,
                    time_range: (10..20).into(),
                    enable_check: true,
                })
                .await
                .unwrap();

            let result_stream = storage
                .scan(ScanRequest {
                    range: TimeRange::new(Timestamp(0), Timestamp::MAX),
                    predicate: vec![],
                    projections: None,
                })
                .await
                .unwrap();
            let expected_batch = [
                record_batch!(
                    ("pk1", UInt8, vec![5, 9, 10, 11]),
                    ("pk2", UInt8, vec![3, 1, 2, 99]),
                    ("value", Int64, vec![1, 44, 66, 77])
                )
                .unwrap(),
                record_batch!(
                    ("pk1", UInt8, vec![11]),
                    ("pk2", UInt8, vec![100]),
                    ("value", Int64, vec![22])
                )
                .unwrap(),
            ];

            check_stream(result_stream, expected_batch).await;
        });
    }

    #[test]
    fn test_storage_sort_batch() {
        let schema = arrow_schema!(("a", UInt8), ("b", UInt8), ("c", UInt8), ("c", UInt8));
        let root_dir = temp_dir::TempDir::new().unwrap();
        let store = Arc::new(LocalFileSystem::new());
        let runtimes = build_runtimes();
        runtimes.sst_compact_runtime.clone().block_on(async move {
            let storage = CloudObjectStorage::try_new(
                root_dir.path().to_string_lossy().to_string(),
                Duration::from_hours(2),
                store,
                schema.clone(),
                1,
                StorageConfig::default(),
                runtimes,
            )
            .await
            .unwrap();
            let batch = record_batch!(
                ("a", UInt8, vec![2, 1, 3, 4, 8, 6, 5, 7]),
                ("b", UInt8, vec![1, 3, 4, 8, 2, 6, 5, 7]),
                ("c", UInt8, vec![8, 6, 2, 4, 3, 1, 5, 7]),
                ("d", UInt8, vec![2, 7, 4, 6, 1, 3, 5, 8])
            )
            .unwrap();

            let mut sorted_batches = storage.sort_batch(batch).await.unwrap();
            let expected_bacth = record_batch!(
                ("a", UInt8, vec![1, 2, 3, 4, 5, 6, 7, 8]),
                ("b", UInt8, vec![3, 1, 4, 8, 5, 6, 7, 2]),
                ("c", UInt8, vec![6, 8, 2, 4, 5, 1, 7, 3]),
                ("d", UInt8, vec![7, 2, 4, 6, 5, 3, 8, 1])
            )
            .unwrap();
            let mut offset = 0;
            while let Some(sorted_batch) = sorted_batches.next().await {
                let sorted_batch = sorted_batch.unwrap();
                let length = sorted_batch.num_rows();
                let batch = expected_bacth.slice(offset, length);
                assert_eq!(sorted_batch, batch);
                offset += length;
            }
        });
    }
}
