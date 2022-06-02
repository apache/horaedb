// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

//! Parquet bench.

use std::{sync::Arc, time::Instant};

use arrow_deps::parquet::{
    arrow::{ArrowReader, ParquetFileArrowReader},
    file::{
        metadata::RowGroupMetaData, reader::FileReader, serialized_reader::SerializedFileReader,
    },
};
use common_types::schema::Schema;
use common_util::runtime::Runtime;
use log::info;
use object_store::{LocalFileSystem, ObjectStore, Path};
use parquet::{DataCacheRef, MetaCacheRef, SliceableCursor};
use table_engine::predicate::PredicateRef;

use crate::{config::SstBenchConfig, util};

type RowGroupPredicate = Box<dyn Fn(&RowGroupMetaData, usize) -> bool + 'static>;

pub struct ParquetBench {
    store: LocalFileSystem,
    pub sst_file_name: String,
    max_projections: usize,
    projection: Vec<usize>,
    schema: Schema,
    predicate: PredicateRef,
    batch_size: usize,
    runtime: Arc<Runtime>,
}

impl ParquetBench {
    pub fn new(config: SstBenchConfig) -> Self {
        let store = LocalFileSystem::new_with_prefix(config.store_path).unwrap();

        let runtime = util::new_runtime(config.runtime_thread_num);

        let sst_path = Path::from(config.sst_file_name.clone());
        let meta_cache: Option<MetaCacheRef> = None;
        let data_cache: Option<DataCacheRef> = None;

        let schema = runtime.block_on(util::schema_from_sst(
            &store,
            &sst_path,
            &meta_cache,
            &data_cache,
        ));

        let predicate = Arc::new(config.predicate.into_predicate());

        ParquetBench {
            store,
            sst_file_name: config.sst_file_name,
            max_projections: config.max_projections,
            projection: Vec::new(),
            schema,
            predicate,
            batch_size: config.read_batch_row_num,
            runtime: Arc::new(runtime),
        }
    }

    pub fn num_benches(&self) -> usize {
        // One test reads all columns and `max_projections` tests read with projection.
        1 + self.max_projections
    }

    pub fn init_for_bench(&mut self, i: usize) {
        let projection = if i < self.max_projections {
            (0..i + 1).into_iter().collect()
        } else {
            Vec::new()
        };

        self.projection = projection;
    }

    pub fn run_bench(&self) {
        let sst_path = Path::from(self.sst_file_name.clone());

        self.runtime.block_on(async {
            let open_instant = Instant::now();
            let get_result = self.store.get(&sst_path).await.unwrap();
            let cursor = SliceableCursor::new(Arc::new(get_result.bytes().await.unwrap().to_vec()));
            let mut file_reader = SerializedFileReader::new(cursor).unwrap();
            let open_cost = open_instant.elapsed();

            let filter_begin_instant = Instant::now();
            let row_group_predicate = self.build_row_group_predicate(&file_reader);
            let mut arrow_reader = {
                file_reader.filter_row_groups(&row_group_predicate);
                ParquetFileArrowReader::new(Arc::new(file_reader))
            };
            let filter_cost = filter_begin_instant.elapsed();

            let record_reader = if self.projection.is_empty() {
                arrow_reader.get_record_reader(self.batch_size).unwrap()
            } else {
                arrow_reader
                    .get_record_reader_by_columns(self.projection.clone(), self.batch_size)
                    .unwrap()
            };

            let iter_begin_instant = Instant::now();
            let mut total_rows = 0;
            let mut batch_num = 0;
            for record_batch in record_reader {
                let num_rows = record_batch.unwrap().num_rows();
                total_rows += num_rows;
                batch_num += 1;
            }

            info!(
                "\nParquetBench total rows of sst: {}, total batch num: {}, open cost: {:?}, filter cost: {:?}, iter cost: {:?}",
                total_rows,
                batch_num,
                open_cost,
                filter_cost,
                iter_begin_instant.elapsed(),
            );
        });
    }

    fn build_row_group_predicate(
        &self,
        file_reader: &SerializedFileReader<SliceableCursor>,
    ) -> RowGroupPredicate {
        let row_groups = file_reader.metadata().row_groups();
        let filter_results = self.predicate.filter_row_groups(&self.schema, row_groups);

        Box::new(move |_, idx: usize| filter_results[idx])
    }
}
