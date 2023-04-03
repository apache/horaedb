// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

//! Merge memtable bench.

use std::{cmp, sync::Arc, time::Instant};

use analytic_engine::{
    memtable::{
        factory::{Factory as MemTableFactory, Options},
        skiplist::factory::SkiplistMemTableFactory,
    },
    row_iter::{
        dedup::DedupIterator,
        merge::{MergeBuilder, MergeConfig},
        IterOptions, RecordBatchWithKeyIterator,
    },
    space::SpaceId,
    sst::{
        factory::{
            FactoryImpl, FactoryRef as SstFactoryRef, ObjectStorePickerRef, ReadFrequency,
            ScanOptions, SstReadOptions,
        },
        meta_data::cache::MetaCacheRef,
    },
    table::{
        sst_util,
        version::{MemTableState, MemTableVec},
    },
};
use arena::NoopCollector;
use common_types::{
    projected_schema::ProjectedSchema, request_id::RequestId, schema::Schema, time::TimeRange,
};
use common_util::runtime::Runtime;
use log::info;
use object_store::{LocalFileSystem, ObjectStoreRef};
use table_engine::{predicate::Predicate, table::TableId};

use crate::{config::MergeMemTableBenchConfig, util};

pub struct MergeMemTableBench {
    store: ObjectStoreRef,
    memtables: MemTableVec,
    max_projections: usize,
    schema: Schema,
    projected_schema: ProjectedSchema,
    runtime: Arc<Runtime>,
    space_id: SpaceId,
    table_id: TableId,
    dedup: bool,
    sst_read_options: SstReadOptions,
}

impl MergeMemTableBench {
    pub fn new(config: MergeMemTableBenchConfig) -> Self {
        assert!(!config.sst_file_ids.is_empty());

        let store = Arc::new(LocalFileSystem::new_with_prefix(config.store_path).unwrap()) as _;
        let runtime = Arc::new(util::new_runtime(config.runtime_thread_num));
        let space_id = config.space_id;
        let table_id = config.table_id;

        let meta_cache: Option<MetaCacheRef> = None;
        // Use first sst's schema.
        let sst_path = sst_util::new_sst_file_path(space_id, table_id, config.sst_file_ids[0]);
        let schema = runtime.block_on(util::schema_from_sst(&store, &sst_path, &meta_cache));

        let projected_schema = ProjectedSchema::no_projection(schema.clone());
        let max_projections = cmp::min(config.max_projections, schema.num_columns());

        let mut memtables = Vec::with_capacity(config.sst_file_ids.len());
        for id in &config.sst_file_ids {
            let sst_path = sst_util::new_sst_file_path(space_id, table_id, *id);

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
                "MergeMemTableBench memtable loaded, memory used:{}",
                memtable.approximate_memory_usage()
            );

            memtables.push(MemTableState {
                mem: memtable,
                time_range: TimeRange::min_to_max(),
                id: *id,
            });
        }
        let sst_read_options = mock_sst_read_options(projected_schema.clone(), runtime.clone());

        MergeMemTableBench {
            store,
            memtables,
            max_projections,
            schema,
            projected_schema,
            runtime,
            space_id,
            table_id,
            dedup: true,
            sst_read_options,
        }
    }

    pub fn num_benches(&self) -> usize {
        // One test reads all columns and `max_projections` tests read with projection.
        1 + self.max_projections
    }

    pub fn init_for_bench(&mut self, i: usize, dedup: bool) {
        let projected_schema =
            util::projected_schema_by_number(&self.schema, i, self.max_projections);

        self.projected_schema = projected_schema;
        self.dedup = dedup;
    }

    pub fn run_bench(&self) {
        let space_id = self.space_id;
        let table_id = self.table_id;
        let sequence = u64::MAX;
        let projected_schema = self.projected_schema.clone();
        let sst_factory: SstFactoryRef = Arc::new(FactoryImpl::default());
        let iter_options = IterOptions {
            batch_size: self.sst_read_options.num_rows_per_row_group,
        };

        let request_id = RequestId::next_id();
        let store_picker: ObjectStorePickerRef = Arc::new(self.store.clone());
        let mut builder = MergeBuilder::new(MergeConfig {
            request_id,
            metrics_collector: None,
            deadline: None,
            space_id,
            table_id,
            sequence,
            projected_schema,
            predicate: Arc::new(Predicate::empty()),
            sst_factory: &sst_factory,
            sst_read_options: self.sst_read_options.clone(),
            store_picker: &store_picker,
            merge_iter_options: iter_options.clone(),
            need_dedup: true,
            reverse: false,
        });

        builder.mut_memtables().extend_from_slice(&self.memtables);

        self.runtime.block_on(async {
            let begin_instant = Instant::now();

            let mut merge_iter = builder.build().await.unwrap();
            let mut total_rows = 0;
            let mut batch_num = 0;

            if self.dedup {
                let mut dedup_iter = DedupIterator::new(request_id, merge_iter, iter_options);
                while let Some(batch) = dedup_iter.next_batch().await.unwrap() {
                    let num_rows = batch.num_rows();
                    total_rows += num_rows;
                    batch_num += 1;
                }
            } else {
                while let Some(batch) = merge_iter.next_batch().await.unwrap() {
                    let num_rows = batch.num_rows();
                    total_rows += num_rows;
                    batch_num += 1;
                }
            }

            info!(
                "MergeMemTableBench total rows of sst:{}, total batch num:{}, cost:{:?}",
                total_rows,
                batch_num,
                begin_instant.elapsed(),
            );
        });
    }
}

fn mock_sst_read_options(
    projected_schema: ProjectedSchema,
    runtime: Arc<Runtime>,
) -> SstReadOptions {
    let scan_options = ScanOptions {
        background_read_parallelism: 1,
        max_record_batches_in_flight: 1024,
    };
    SstReadOptions {
        reverse: false,
        frequency: ReadFrequency::Frequent,
        num_rows_per_row_group: 500,
        projected_schema,
        predicate: Arc::new(Predicate::empty()),
        meta_cache: None,
        scan_options,
        runtime,
    }
}
