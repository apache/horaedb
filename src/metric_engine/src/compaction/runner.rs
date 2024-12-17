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

use std::sync::Arc;

use anyhow::Context;
use arrow::array::{RecordBatch, UInt64Array};
use async_scoped::TokioScope;
use datafusion::{execution::TaskContext, physical_plan::execute_stream};
use futures::StreamExt;
use object_store::path::Path;
use parquet::{
    arrow::{async_writer::ParquetObjectWriter, AsyncArrowWriter},
    file::properties::WriterProperties,
};
use tracing::error;

use crate::{
    compaction::Task,
    manifest::ManifestRef,
    read::ParquetReader,
    sst::{allocate_id, FileMeta, SstPathGenerator},
    types::{ObjectStoreRef, StorageSchema},
    Result,
};

#[derive(Clone)]
pub struct Runner {
    store: ObjectStoreRef,
    schema: StorageSchema,
    manifest: ManifestRef,
    sst_path_gen: Arc<SstPathGenerator>,
    parquet_reader: Arc<ParquetReader>,
    write_props: WriterProperties,
}

impl Runner {
    pub fn new(
        store: ObjectStoreRef,
        schema: StorageSchema,
        manifest: ManifestRef,
        sst_path_gen: Arc<SstPathGenerator>,
        parquet_reader: Arc<ParquetReader>,
        write_props: WriterProperties,
    ) -> Self {
        Self {
            store,
            schema,
            manifest,
            sst_path_gen,
            parquet_reader,
            write_props,
        }
    }

    // TODO: Merge input sst files into one new sst file
    // and delete the expired sst files
    pub async fn do_compaction(&self, task: Task) -> Result<()> {
        assert!(!task.inputs.is_empty());
        for f in &task.inputs {
            assert!(f.is_compaction());
        }
        for f in &task.expireds {
            assert!(f.is_compaction());
        }
        let mut time_range = task.inputs[0].meta().time_range.clone();
        for f in &task.inputs[1..] {
            time_range.merge(&f.meta().time_range);
        }
        let plan = self
            .parquet_reader
            .build_df_plan(task.inputs.clone(), None, Vec::new())?;
        let mut stream = execute_stream(plan, Arc::new(TaskContext::default()))
            .context("execute datafusion plan")?;

        let file_id = allocate_id();
        let file_path = self.sst_path_gen.generate(file_id);
        let file_path = Path::from(file_path);
        let object_store_writer = ParquetObjectWriter::new(self.store.clone(), file_path.clone());
        let mut writer = AsyncArrowWriter::try_new(
            object_store_writer,
            self.schema.arrow_schema.clone(),
            Some(self.write_props.clone()),
        )
        .context("create arrow writer")?;
        let mut num_rows = 0;
        // TODO: support multi-part write
        while let Some(batch) = stream.next().await {
            let batch = batch.context("execute plan")?;
            num_rows += batch.num_rows();
            let batch_with_seq = {
                let mut new_cols = batch.columns().to_vec();
                // Since file_id in increasing order, we can use it as sequence.
                let seq_column = Arc::new(UInt64Array::from(vec![file_id; batch.num_rows()]));
                new_cols.push(seq_column);
                RecordBatch::try_new(self.schema.arrow_schema.clone(), new_cols)
                    .context("construct record batch with seq column")?
            };

            writer.write(&batch_with_seq).await.context("write batch")?;
        }
        writer.close().await.context("close writer")?;
        let object_meta = self
            .store
            .head(&file_path)
            .await
            .context("get object meta")?;
        let file_meta = FileMeta {
            max_sequence: file_id,
            num_rows: num_rows as u32,
            size: object_meta.size as u32,
            time_range: time_range.clone(),
        };
        // First add new sst to manifest, then delete expired/old sst
        self.manifest.add_file(file_id, file_meta).await?;
        self.manifest
            .add_tombstone_files(task.expireds.clone())
            .await?;
        self.manifest
            .add_tombstone_files(task.inputs.clone())
            .await?;

        let (_, results) = TokioScope::scope_and_block(|scope| {
            for file in task.expireds {
                let path = Path::from(self.sst_path_gen.generate(file.id()));
                scope.spawn(async move {
                    self.store
                        .delete(&path)
                        .await
                        .with_context(|| format!("failed to delete file, path:{path}"))
                });
            }
            for file in task.inputs {
                let path = Path::from(self.sst_path_gen.generate(file.id()));
                scope.spawn(async move {
                    self.store
                        .delete(&path)
                        .await
                        .with_context(|| format!("failed to delete file, path:{path}"))
                });
            }
        });
        for res in results {
            match res {
                Err(e) => {
                    error!("Failed to join delete task, err:{e}")
                }
                Ok(v) => {
                    if let Err(e) = v {
                        error!("Failed to delete sst, err:{e}")
                    }
                }
            }
        }

        Ok(())
    }
}
