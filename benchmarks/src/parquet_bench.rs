// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

//! Parquet bench.

use std::{io::Cursor, sync::Arc, time::Instant};

use analytic_engine::sst::meta_data::cache::MetaCacheRef;
use common_types::schema::Schema;
use common_util::runtime::Runtime;
use futures::StreamExt;
use log::info;
use object_store::{LocalFileSystem, ObjectStoreRef, Path};
use parquet::arrow::{
    arrow_reader::ParquetRecordBatchReaderBuilder, ParquetRecordBatchStreamBuilder,
};
use table_engine::predicate::PredicateRef;

use crate::{config::SstBenchConfig, util};

pub struct ParquetBench {
    store: ObjectStoreRef,
    pub sst_file_name: String,
    max_projections: usize,
    projection: Vec<usize>,
    _schema: Schema,
    _predicate: PredicateRef,
    runtime: Arc<Runtime>,
    is_async: bool,
    batch_size: usize,
}

impl ParquetBench {
    pub fn new(config: SstBenchConfig) -> Self {
        let store = Arc::new(LocalFileSystem::new_with_prefix(&config.store_path).unwrap()) as _;

        let runtime = util::new_runtime(config.runtime_thread_num);

        let sst_path = Path::from(config.sst_file_name.clone());
        let meta_cache: Option<MetaCacheRef> = None;
        let schema = runtime.block_on(util::schema_from_sst(&store, &sst_path, &meta_cache));

        ParquetBench {
            store,
            sst_file_name: config.sst_file_name,
            max_projections: config.max_projections,
            projection: Vec::new(),
            _schema: schema,
            _predicate: config.predicate.into_predicate(),
            runtime: Arc::new(runtime),
            is_async: config.is_async,
            batch_size: config.num_rows_per_row_group,
        }
    }

    pub fn num_benches(&self) -> usize {
        // One test reads all columns and `max_projections` tests read with projection.
        1 + self.max_projections
    }

    pub fn init_for_bench(&mut self, i: usize) {
        let projection = if i < self.max_projections {
            (0..i + 1).collect()
        } else {
            Vec::new()
        };

        self.projection = projection;
    }

    pub fn run_bench(&self) {
        if self.is_async {
            return self.run_async_bench();
        }

        self.run_sync_bench()
    }

    pub fn run_sync_bench(&self) {
        let sst_path = Path::from(self.sst_file_name.clone());

        self.runtime.block_on(async {
            let open_instant = Instant::now();
            let get_result = self.store.get(&sst_path).await.unwrap();
            let bytes = get_result.bytes().await.unwrap();
            let open_cost = open_instant.elapsed();

            let filter_begin_instant = Instant::now();
            let arrow_reader = ParquetRecordBatchReaderBuilder::try_new(bytes)
                .unwrap()
                .with_batch_size(self.batch_size)
                .build()
                .unwrap();
            let filter_cost = filter_begin_instant.elapsed();

            let iter_begin_instant = Instant::now();
            let mut total_rows = 0;
            let mut batch_num = 0;
            for record_batch in arrow_reader {
                let num_rows = record_batch.unwrap().num_rows();
                total_rows += num_rows;
                batch_num += 1;
            }

            info!(
                "\nParquetBench Sync total rows of sst:{}, total batch num:{},
                open cost:{:?}, filter cost:{:?}, iter cost:{:?}",
                total_rows,
                batch_num,
                open_cost,
                filter_cost,
                iter_begin_instant.elapsed(),
            );
        });
    }

    pub fn run_async_bench(&self) {
        let sst_path = Path::from(self.sst_file_name.clone());
        self.runtime.block_on(async {
            let open_instant = Instant::now();
            let get_result = self.store.get(&sst_path).await.unwrap();
            let bytes = get_result.bytes().await.unwrap();
            let cursor = Cursor::new(bytes);
            let open_cost = open_instant.elapsed();

            let filter_begin_instant = Instant::now();
            let mut stream = ParquetRecordBatchStreamBuilder::new(cursor)
                .await
                .unwrap()
                .with_batch_size(self.batch_size)
                .build()
                .unwrap();
            let filter_cost = filter_begin_instant.elapsed();

            let mut total_rows = 0;
            let mut batch_num = 0;
            let iter_begin_instant = Instant::now();
            while let Some(record_batch) = stream.next().await {
                let num_rows = record_batch.unwrap().num_rows();
                total_rows += num_rows;
                batch_num += 1;
            }

            info!(
                "\nParquetBench Async total rows of sst:{}, total batch num:{},
                open cost:{:?}, filter cost:{:?}, iter cost:{:?}",
                total_rows,
                batch_num,
                open_cost,
                filter_cost,
                iter_begin_instant.elapsed(),
            );
        });
    }
}
