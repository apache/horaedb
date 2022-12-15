// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

//! SST bench.

use std::{cmp, sync::Arc, time::Instant};

use analytic_engine::sst::{
    factory::{Factory, FactoryImpl, SstReaderOptions, SstType},
    meta_cache::{MetaCache, MetaCacheRef},
};
use common_types::{projected_schema::ProjectedSchema, schema::Schema};
use common_util::runtime::Runtime;
use futures::stream::StreamExt;
use log::info;
use object_store::{LocalFileSystem, ObjectStoreRef, Path};

use crate::{config::SstBenchConfig, util};

pub struct SstBench {
    store: ObjectStoreRef,
    pub sst_file_name: String,
    max_projections: usize,
    schema: Schema,
    sst_reader_options: SstReaderOptions,
    runtime: Arc<Runtime>,
}

impl SstBench {
    pub fn new(config: SstBenchConfig) -> Self {
        let runtime = Arc::new(util::new_runtime(config.runtime_thread_num));

        let store = Arc::new(LocalFileSystem::new_with_prefix(config.store_path).unwrap()) as _;
        let sst_path = Path::from(config.sst_file_name.clone());
        let meta_cache: Option<MetaCacheRef> = config
            .sst_meta_cache_cap
            .map(|cap| Arc::new(MetaCache::new(cap)));
        let schema = runtime.block_on(util::schema_from_sst(&store, &sst_path, &meta_cache));
        let predicate = config.predicate.into_predicate();
        let projected_schema = ProjectedSchema::no_projection(schema.clone());
        let sst_reader_options = SstReaderOptions {
            sst_type: SstType::Parquet,
            read_batch_row_num: config.read_batch_row_num,
            reverse: config.reverse,
            projected_schema,
            predicate,
            meta_cache,
            runtime: runtime.clone(),
            background_read_parallelism: 1,
            num_rows_per_row_group: config.read_batch_row_num,
        };
        let max_projections = cmp::min(config.max_projections, schema.num_columns());

        SstBench {
            store,
            sst_file_name: config.sst_file_name,
            max_projections,
            schema,
            sst_reader_options,
            runtime,
        }
    }

    pub fn num_benches(&self) -> usize {
        // One test reads all columns and `max_projections` tests read with projection.
        1 + self.max_projections
    }

    pub fn init_for_bench(&mut self, i: usize) {
        let projected_schema =
            util::projected_schema_by_number(&self.schema, i, self.max_projections);

        self.sst_reader_options.projected_schema = projected_schema;
    }

    pub fn run_bench(&self) {
        let sst_path = Path::from(self.sst_file_name.clone());

        let sst_factory = FactoryImpl;
        let mut sst_reader = sst_factory
            .new_sst_reader(&self.sst_reader_options, &sst_path, &self.store)
            .unwrap();

        self.runtime.block_on(async {
            let begin_instant = Instant::now();
            let mut sst_stream = sst_reader.read().await.unwrap();

            let mut total_rows = 0;
            let mut batch_num = 0;
            while let Some(batch) = sst_stream.next().await {
                let num_rows = batch.unwrap().num_rows();
                total_rows += num_rows;
                batch_num += 1;
            }

            info!(
                "\nSstBench total rows of sst: {}, total batch num: {}, cost: {:?}",
                total_rows,
                batch_num,
                begin_instant.elapsed(),
            );
        });
    }
}
