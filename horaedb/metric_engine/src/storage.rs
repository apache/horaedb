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
    common::DFSchema,
    datasource::{
        listing::PartitionedFile,
        physical_plan::{FileScanConfig, ParquetExec},
    },
    execution::{context::ExecutionProps, object_store::ObjectStoreUrl, SendableRecordBatchStream},
    logical_expr::{utils::conjunction, Expr},
    physical_expr::{create_physical_expr, LexOrdering},
    physical_plan::{
        execute_stream,
        memory::MemoryExec,
        sorts::{sort::SortExec, sort_preserving_merge::SortPreservingMergeExec},
        ExecutionPlan,
    },
    physical_planner::create_physical_sort_exprs,
    prelude::{ident, SessionContext},
};
use futures::StreamExt;
use itertools::Itertools;
use macros::ensure;
use object_store::path::Path;
use parquet::{
    arrow::{async_writer::ParquetObjectWriter, AsyncArrowWriter},
    file::properties::WriterProperties,
    format::SortingColumn,
    schema::types::ColumnPath,
};

use crate::{
    manifest::Manifest,
    read::DefaultParquetFileReaderFactory,
    sst::{allocate_id, FileId, FileMeta, SstFile},
    types::{ObjectStoreRef, TimeRange, WriteOptions, WriteResult},
    Result,
};

pub struct WriteRequest {
    pub batch: RecordBatch,
    pub time_range: TimeRange,
    // Check data is valid if it's true.
    pub enable_check: bool,
}

pub struct ScanRequest {
    range: TimeRange,
    predicate: Vec<Expr>,
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

/// `TimeMergeStorage` implementation using cloud object storage, it will split
/// data into different segments(aka `segment_duration`) based time range.
///
/// Compaction will be done by merging segments within a segment, and segment
/// will make it easy to support expiration.
pub struct CloudObjectStorage {
    segment_duration: Duration,
    path: String,
    store: ObjectStoreRef,
    arrow_schema: SchemaRef,
    num_primary_keys: usize,
    manifest: Manifest,

    df_schema: DFSchema,
    write_props: WriterProperties,
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
    // seq column is appended to the end of schema.
    const SEQ_COLUMN_NAME: &'static str = "__seq__";

    pub async fn try_new(
        path: String,
        segment_duration: Duration,
        store: ObjectStoreRef,
        arrow_schema: SchemaRef,
        num_primary_keys: usize,
        write_options: WriteOptions,
    ) -> Result<Self> {
        let manifest_prefix = crate::manifest::PREFIX_PATH;
        let manifest =
            Manifest::try_new(format!("{path}/{manifest_prefix}"), store.clone()).await?;
        let mut new_fields = arrow_schema.fields.clone().to_vec();
        new_fields.push(Arc::new(Field::new(
            Self::SEQ_COLUMN_NAME,
            DataType::UInt64,
            true,
        )));
        let arrow_schema = Arc::new(Schema::new_with_metadata(
            new_fields,
            arrow_schema.metadata.clone(),
        ));
        let df_schema = DFSchema::try_from(arrow_schema.clone()).context("build DFSchema")?;
        let write_props = Self::build_write_props(write_options, num_primary_keys);
        Ok(Self {
            path,
            num_primary_keys,
            segment_duration,
            store,
            arrow_schema,
            manifest,
            df_schema,
            write_props,
        })
    }

    fn build_file_path(&self, id: FileId) -> String {
        let root = &self.path;
        let prefix = crate::sst::PREFIX_PATH;
        format!("{root}/{prefix}/{id}")
    }

    async fn write_batch(&self, batch: RecordBatch) -> Result<WriteResult> {
        let file_id = allocate_id();
        let file_path = self.build_file_path(file_id);
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
                RecordBatch::try_new(self.arrow_schema.clone(), new_cols)
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

    fn build_sort_exprs(&self, sort_seq: bool) -> Result<LexOrdering> {
        let mut sort_exprs = (0..self.num_primary_keys)
            .map(|i| {
                ident(self.schema().field(i).name())
                    .sort(true /* asc */, true /* nulls_first */)
            })
            .collect::<Vec<_>>();
        if sort_seq {
            sort_exprs.push(ident(Self::SEQ_COLUMN_NAME).sort(true, true));
        }
        let sort_exprs =
            create_physical_sort_exprs(&sort_exprs, &self.df_schema, &ExecutionProps::default())
                .context("create physical sort exprs")?;

        Ok(sort_exprs)
    }

    async fn sort_batch(&self, batch: RecordBatch) -> Result<SendableRecordBatchStream> {
        let ctx = SessionContext::default();
        let schema = batch.schema();
        let sort_exprs = self.build_sort_exprs(false /* sort_seq */)?;
        let batch_plan =
            MemoryExec::try_new(&[vec![batch]], schema, None).context("build batch plan")?;
        let physical_plan = Arc::new(SortExec::new(sort_exprs, Arc::new(batch_plan)));

        let res =
            execute_stream(physical_plan, ctx.task_ctx()).context("execute sort physical plan")?;
        Ok(res)
    }

    fn build_write_props(write_options: WriteOptions, num_primary_key: usize) -> WriterProperties {
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
            .set_encoding(write_options.encoding)
            .set_compression(write_options.compression);

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
                builder = builder.set_column_encoding(col_path.clone(), encoding);
            }
            if let Some(compression) = col_opt.compression {
                builder = builder.set_column_compression(col_path, compression);
            }
        }

        builder.build()
    }

    async fn build_scan_plan(
        &self,
        ssts: Vec<SstFile>,
        projections: Option<Vec<usize>>,
        predicates: Vec<Expr>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        // we won't use url for selecting object_store.
        let dummy_url = ObjectStoreUrl::parse("empty://").unwrap();
        let sort_exprs = self.build_sort_exprs(true /* sort_seq */)?;

        let file_groups = ssts
            .into_iter()
            .map(|f| {
                vec![PartitionedFile::new(
                    self.build_file_path(f.id),
                    f.meta.size as u64,
                )]
            })
            .collect::<Vec<_>>();
        let scan_config = FileScanConfig::new(dummy_url, self.schema().clone())
            .with_output_ordering(vec![sort_exprs.clone(); file_groups.len()])
            .with_file_groups(file_groups)
            .with_projection(projections);

        let mut builder = ParquetExec::builder(scan_config).with_parquet_file_reader_factory(
            Arc::new(DefaultParquetFileReaderFactory::new(self.store.clone())),
        );
        if let Some(expr) = conjunction(predicates) {
            let filters = create_physical_expr(&expr, &self.df_schema, &ExecutionProps::new())
                .context("create physical expr")?;
            builder = builder.with_predicate(filters);
        }

        // TODO: fetch using multiple threads since read from parquet will incur CPU
        // when convert between arrow and parquet.
        let parquet_exec = builder.build();
        let sort_exec = SortPreservingMergeExec::new(sort_exprs, Arc::new(parquet_exec))
            // TODO: make fetch size configurable.
            .with_fetch(Some(1024))
            .with_round_robin_repartition(true);

        // TODO: Add a new plan dedup record batch based on primary keys and sequence
        // number.
        Ok(Arc::new(sort_exec))
    }

    async fn scan_one_segment(
        &self,
        ssts: Vec<SstFile>,
        projections: Option<Vec<usize>>,
        predicates: Vec<Expr>,
    ) -> Result<SendableRecordBatchStream> {
        let plan = self.build_scan_plan(ssts, projections, predicates).await?;
        let ctx = SessionContext::default();
        let res = execute_stream(plan, ctx.task_ctx()).context("execute sort physical plan")?;

        Ok(res)
    }
}

#[async_trait]
impl TimeMergeStorage for CloudObjectStorage {
    fn schema(&self) -> &SchemaRef {
        &self.arrow_schema
    }

    async fn write(&self, req: WriteRequest) -> Result<()> {
        if req.enable_check {
            ensure!(
                self.arrow_schema.contains(req.batch.schema_ref()),
                "schema not match"
            );
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

    #[allow(clippy::never_loop)]
    async fn scan(&self, req: ScanRequest) -> Result<SendableRecordBatchStream> {
        let total_ssts = self.manifest.find_ssts(&req.range).await;
        let ssts_by_segment = total_ssts.into_iter().group_by(|file| {
            file.meta.time_range.start.0 / self.segment_duration.as_millis() as i64
        });

        for (_, ssts) in ssts_by_segment {
            return self
                .scan_one_segment(ssts, req.projections.clone(), req.predicate.clone())
                .await;
        }

        // TODO: currently only return records within one segment, we should merge
        // them.
        todo!("Merge stream from different segment")
    }

    async fn compact(&self, req: CompactRequest) -> Result<()> {
        todo!()
    }
}

#[cfg(test)]
mod tests {
    use arrow::array::{self as arrow_array};
    use datafusion::common::record_batch;
    use object_store::local::LocalFileSystem;

    use super::*;
    use crate::{arrow_schema, types::Timestamp};

    #[tokio::test]
    #[ignore = "Depend on MergeExec"]
    async fn test_build_scan_plan() {
        let schema = arrow_schema!(("pk1", UInt8));
        let store = Arc::new(LocalFileSystem::new());
        let storage = CloudObjectStorage::try_new(
            "mock".to_string(),
            Duration::from_hours(2),
            store,
            schema.clone(),
            1, // num_primary_keys
            WriteOptions::default(),
        )
        .await
        .unwrap();
        let plan = storage
            .build_scan_plan(
                (100..103)
                    .map(|id| SstFile {
                        id,
                        meta: FileMeta {
                            max_sequence: id,
                            num_rows: 1,
                            size: 1,
                            time_range: (1..10).into(),
                        },
                    })
                    .collect(),
                None,
                vec![],
            )
            .await
            .unwrap();
        let display_plan =
            datafusion::physical_plan::display::DisplayableExecutionPlan::new(plan.as_ref())
                .indent(true);
        assert_eq!(
            r#"SortPreservingMergeExec: [pk1@0 ASC], fetch=1024
  ParquetExec: file_groups={3 groups: [[mock/data/100], [mock/data/101], [mock/data/102]]}, projection=[pk1], output_orderings=[[pk1@0 ASC], [pk1@0 ASC], [pk1@0 ASC]]
"#,
            format!("{display_plan}")
        );
    }

    #[tokio::test]
    #[ignore = "Depend on MergeExec"]
    async fn test_storage_write_and_scan() {
        let schema = arrow_schema!(("pk1", UInt8), ("pk2", UInt8), ("value", Int64));
        let root_dir = temp_dir::TempDir::new().unwrap();
        let store = Arc::new(LocalFileSystem::new());
        let storage = CloudObjectStorage::try_new(
            root_dir.path().to_string_lossy().to_string(),
            Duration::from_hours(2),
            store,
            schema.clone(),
            2, // num_primary_keys
            WriteOptions::default(),
        )
        .await
        .unwrap();

        let batch = record_batch!(
            ("pk1", UInt8, vec![11, 11, 9, 10, 5]),
            ("pk2", UInt8, vec![100, 99, 1, 2, 3]),
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
            ("pk1", UInt8, vec![1, 8, 9]),
            ("pk2", UInt8, vec![100, 99, 98]),
            ("value", Int64, vec![2, 7, 4])
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

        let mut result_stream = storage
            .scan(ScanRequest {
                range: TimeRange::new(Timestamp(0), Timestamp::MAX),
                predicate: vec![],
                projections: None,
            })
            .await
            .unwrap();
        let expected_batch = record_batch!(
            ("pk1", UInt8, vec![1, 5, 8, 9, 9, 10, 11, 11]),
            ("pk2", UInt8, vec![100, 3, 99, 1, 98, 2, 99, 100]),
            ("value", Int64, vec![2, 1, 7, 4, 4, 6, 7, 2])
        )
        .unwrap();
        while let Some(batch) = result_stream.next().await {
            let batch = batch.unwrap();
            assert_eq!(expected_batch, batch);
        }
    }

    #[tokio::test]
    async fn test_storage_sort_batch() {
        let schema = arrow_schema!(("a", UInt8), ("b", UInt8), ("c", UInt8), ("c", UInt8));
        let root_dir = temp_dir::TempDir::new().unwrap();
        let store = Arc::new(LocalFileSystem::new());
        let storage = CloudObjectStorage::try_new(
            root_dir.path().to_string_lossy().to_string(),
            Duration::from_hours(2),
            store,
            schema.clone(),
            1,
            WriteOptions::default(),
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
    }
}
