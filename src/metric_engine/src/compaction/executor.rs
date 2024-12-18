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

use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
};

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
    ensure,
    manifest::{ManifestRef, ManifestUpdate},
    read::ParquetReader,
    sst::{allocate_id, FileMeta, SstFile, SstPathGenerator},
    types::{ObjectStoreRef, RuntimeRef, StorageSchema},
    Result,
};

#[derive(Clone)]
pub struct Executor {
    inner: Arc<Inner>,
}

struct Inner {
    runtime: RuntimeRef,
    store: ObjectStoreRef,
    schema: StorageSchema,
    manifest: ManifestRef,
    sst_path_gen: Arc<SstPathGenerator>,
    parquet_reader: Arc<ParquetReader>,
    write_props: WriterProperties,
    inused_memory: AtomicU64,
    mem_limit: u64,
}

impl Executor {
    pub fn new(
        runtime: RuntimeRef,
        store: ObjectStoreRef,
        schema: StorageSchema,
        manifest: ManifestRef,
        sst_path_gen: Arc<SstPathGenerator>,
        parquet_reader: Arc<ParquetReader>,
        write_props: WriterProperties,
        mem_limit: u64,
    ) -> Self {
        let inner = Inner {
            runtime,
            store,
            schema,
            manifest,
            sst_path_gen,
            parquet_reader,
            write_props,
            mem_limit,
            inused_memory: AtomicU64::new(0),
        };
        Self {
            inner: Arc::new(inner),
        }
    }

    fn pre_check(&self, task: &Task) -> Result<()> {
        assert!(!task.inputs.is_empty());
        for f in &task.inputs {
            assert!(f.is_compaction());
        }
        for f in &task.expireds {
            assert!(f.is_compaction());
        }

        let task_size = task.input_size();
        let inused = self.inner.inused_memory.load(Ordering::Relaxed);
        let mem_limit = self.inner.mem_limit;
        ensure!(
            inused + task_size > mem_limit,
            "Compaction memory usage too high, inused:{inused}, task_size:{task_size}, limit:{mem_limit}"
        );

        self.inner
            .inused_memory
            .fetch_add(task.input_size(), Ordering::Relaxed);
        Ok(())
    }

    pub fn on_success(&self, task: &Task) {
        let task_size = task.input_size();
        self.inner
            .inused_memory
            .fetch_add(task_size, Ordering::Relaxed);
    }

    pub fn on_failure(&self, task: &Task) {
        let task_size = task.input_size();
        self.inner
            .inused_memory
            .fetch_sub(task_size, Ordering::Relaxed);

        // When task execution fails, unmark sst so they can be
        // reschduled.
        for sst in &task.inputs {
            sst.unmark_compaction();
        }
        for sst in &task.expireds {
            sst.unmark_compaction();
        }
    }

    pub fn submit(&self, task: Task) {
        let runnable = Runnable {
            executor: self.clone(),
            task,
        };
        runnable.run()
    }

    // TODO: Merge input sst files into one new sst file
    // and delete the expired sst files
    pub async fn do_compaction(&self, task: &Task) -> Result<()> {
        self.pre_check(task)?;

        let mut time_range = task.inputs[0].meta().time_range.clone();
        for f in &task.inputs[1..] {
            time_range.merge(&f.meta().time_range);
        }
        let plan =
            self.inner
                .parquet_reader
                .build_df_plan(task.inputs.clone(), None, Vec::new())?;
        let mut stream = execute_stream(plan, Arc::new(TaskContext::default()))
            .context("execute datafusion plan")?;

        let file_id = allocate_id();
        let file_path = self.inner.sst_path_gen.generate(file_id);
        let file_path = Path::from(file_path);
        let object_store_writer =
            ParquetObjectWriter::new(self.inner.store.clone(), file_path.clone());
        let mut writer = AsyncArrowWriter::try_new(
            object_store_writer,
            self.inner.schema.arrow_schema.clone(),
            Some(self.inner.write_props.clone()),
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
                RecordBatch::try_new(self.inner.schema.arrow_schema.clone(), new_cols)
                    .context("construct record batch with seq column")?
            };

            writer.write(&batch_with_seq).await.context("write batch")?;
        }
        writer.close().await.context("close writer")?;
        let object_meta = self
            .inner
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
        let to_adds = vec![SstFile::new(file_id, file_meta)];
        let to_deletes = task
            .expireds
            .iter()
            .map(|f| f.id())
            .chain(task.inputs.iter().map(|f| f.id()))
            .collect();
        self.inner
            .manifest
            .update(ManifestUpdate::new(to_adds, to_deletes))
            .await?;

        // From now on, no error should be returned!
        // Because we have already updated manifest.

        let (_, results) = TokioScope::scope_and_block(|scope| {
            for file in &task.expireds {
                let path = Path::from(self.inner.sst_path_gen.generate(file.id()));
                scope.spawn(async move {
                    self.inner
                        .store
                        .delete(&path)
                        .await
                        .with_context(|| format!("failed to delete file, path:{path}"))
                });
            }
            for file in &task.inputs {
                let path = Path::from(self.inner.sst_path_gen.generate(file.id()));
                scope.spawn(async move {
                    self.inner
                        .store
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

pub struct Runnable {
    executor: Executor,
    task: Task,
}

impl Runnable {
    fn run(self) {
        let rt = self.executor.inner.runtime.clone();
        rt.spawn(async move {
            if let Err(e) = self.executor.do_compaction(&self.task).await {
                error!("Do compaction failed, err:{e}");
                self.executor.on_failure(&self.task);
            } else {
                self.executor.on_success(&self.task);
            }
        });
    }
}
