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

//! Scan memtable bench.

use std::{collections::Bound, sync::Arc};

use analytic_engine::{
    memtable::{
        factory::{Factory as MemTableFactory, Options},
        skiplist::factory::SkiplistMemTableFactory,
        MemTableRef, ScanContext, ScanRequest,
    },
    sst::meta_data::cache::MetaCacheRef,
};
use arena::NoopCollector;
use common_types::projected_schema::ProjectedSchema;
use log::info;
use object_store::{LocalFileSystem, Path};

use crate::{config::ScanMemTableBenchConfig, util};

pub struct ScanMemTableBench {
    memtable: MemTableRef,
    projected_schema: ProjectedSchema,
    max_projections: usize,
}

impl ScanMemTableBench {
    pub fn new(config: ScanMemTableBenchConfig) -> Self {
        let store = Arc::new(LocalFileSystem::new_with_prefix(config.store_path).unwrap()) as _;

        let runtime = Arc::new(util::new_runtime(config.runtime_thread_num));
        let meta_cache: Option<MetaCacheRef> = None;
        let sst_path = Path::from(config.sst_file_name);
        let schema = runtime.block_on(util::schema_from_sst(&store, &sst_path, &meta_cache));

        let projected_schema = ProjectedSchema::no_projection(schema.clone());

        let memtable_factory = SkiplistMemTableFactory;
        let memtable_opts = Options {
            collector: Arc::new(NoopCollector {}),
            schema: schema.clone(),
            arena_block_size: config.arena_block_size.0 as u32,
            creation_sequence: crate::INIT_SEQUENCE,
        };
        let memtable = memtable_factory.create_memtable(memtable_opts).unwrap();

        runtime.block_on(util::load_sst_to_memtable(
            &store,
            &sst_path,
            &schema,
            &memtable,
            runtime.clone(),
        ));

        info!(
            "\nScanMemTableBench memtable loaded, memory used: {}",
            memtable.approximate_memory_usage()
        );

        Self {
            memtable,
            projected_schema,
            max_projections: config.max_projections,
        }
    }

    pub fn num_benches(&self) -> usize {
        // One test reads all columns and `max_projections` tests read with projection.
        1 + self.max_projections
    }

    pub fn init_for_bench(&mut self, i: usize) {
        let projected_schema =
            util::projected_schema_by_number(self.memtable.schema(), i, self.max_projections);

        self.projected_schema = projected_schema;
    }

    pub fn run_bench(&self) {
        let scan_ctx = ScanContext::default();
        let scan_req = ScanRequest {
            start_user_key: Bound::Unbounded,
            end_user_key: Bound::Unbounded,
            sequence: common_types::MAX_SEQUENCE_NUMBER,
            projected_schema: self.projected_schema.clone(),
            need_dedup: true,
            reverse: false,
            metrics_collector: None,
        };

        let iter = self.memtable.scan(scan_ctx, scan_req).unwrap();

        let mut total_rows = 0;
        let mut batch_num = 0;
        for batch in iter {
            let num_rows = batch.unwrap().num_rows();
            total_rows += num_rows;
            batch_num += 1;
        }

        info!(
            "\nScanMemTableBench total rows of memtable: {}, total batch num: {}",
            total_rows, batch_num,
        );
    }
}
