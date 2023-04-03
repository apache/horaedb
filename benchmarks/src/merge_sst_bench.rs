// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

//! Merge SST bench.

use std::{cmp, sync::Arc, time::Instant};

use analytic_engine::{
    row_iter::{
        chain,
        chain::ChainConfig,
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
        file::{FileHandle, FilePurgeQueue, Request},
        meta_data::cache::MetaCacheRef,
    },
    table::sst_util,
};
use common_types::{projected_schema::ProjectedSchema, request_id::RequestId, schema::Schema};
use common_util::runtime::Runtime;
use log::info;
use object_store::{LocalFileSystem, ObjectStoreRef};
use table_engine::{predicate::Predicate, table::TableId};
use tokio::sync::mpsc::{self, UnboundedReceiver};

use crate::{config::MergeSstBenchConfig, util};

pub struct MergeSstBench {
    store: ObjectStoreRef,
    max_projections: usize,
    schema: Schema,
    sst_read_options: SstReadOptions,
    runtime: Arc<Runtime>,
    space_id: SpaceId,
    table_id: TableId,
    file_handles: Vec<FileHandle>,
    _receiver: UnboundedReceiver<Request>,
    dedup: bool,
}

impl MergeSstBench {
    pub fn new(config: MergeSstBenchConfig) -> Self {
        assert!(!config.sst_file_ids.is_empty());

        let store = Arc::new(LocalFileSystem::new_with_prefix(config.store_path).unwrap()) as _;
        let runtime = Arc::new(util::new_runtime(config.runtime_thread_num));
        let space_id = config.space_id;
        let table_id = config.table_id;

        let sst_path = sst_util::new_sst_file_path(space_id, table_id, config.sst_file_ids[0]);
        let meta_cache: Option<MetaCacheRef> = None;

        let schema = runtime.block_on(util::schema_from_sst(&store, &sst_path, &meta_cache));

        let predicate = config.predicate.into_predicate();
        let projected_schema = ProjectedSchema::no_projection(schema.clone());
        let scan_options = ScanOptions {
            background_read_parallelism: 1,
            max_record_batches_in_flight: 1024,
        };
        let sst_read_options = SstReadOptions {
            reverse: false,
            frequency: ReadFrequency::Frequent,
            num_rows_per_row_group: config.num_rows_per_row_group,
            projected_schema,
            predicate,
            meta_cache: meta_cache.clone(),
            scan_options,
            runtime: runtime.clone(),
        };
        let max_projections = cmp::min(config.max_projections, schema.num_columns());

        let (tx, rx) = mpsc::unbounded_channel();
        let purge_queue = FilePurgeQueue::new(space_id, table_id, tx);

        let file_handles = runtime.block_on(util::file_handles_from_ssts(
            &store,
            space_id,
            table_id,
            &config.sst_file_ids,
            purge_queue,
            &meta_cache,
        ));

        MergeSstBench {
            store,
            max_projections,
            schema,
            sst_read_options,
            runtime,
            space_id,
            table_id,
            file_handles,
            _receiver: rx,
            dedup: true,
        }
    }

    pub fn num_benches(&self) -> usize {
        // One test reads all columns and `max_projections` tests read with projection.
        1 + self.max_projections
    }

    pub fn init_for_bench(&mut self, i: usize, dedup: bool) {
        let projected_schema =
            util::projected_schema_by_number(&self.schema, i, self.max_projections);

        self.sst_read_options.projected_schema = projected_schema;
        self.dedup = dedup;
    }

    fn run_dedup_bench(&self) {
        let space_id = self.space_id;
        let table_id = self.table_id;
        let sequence = u64::MAX;
        let projected_schema = self.sst_read_options.projected_schema.clone();
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

        builder
            .mut_ssts_of_level(0)
            .extend_from_slice(&self.file_handles);

        self.runtime.block_on(async {
            let begin_instant = Instant::now();

            let merge_iter = builder.build().await.unwrap();
            let mut dedup_iter = DedupIterator::new(request_id, merge_iter, iter_options);
            let mut total_rows = 0;
            let mut batch_num = 0;

            while let Some(batch) = dedup_iter.next_batch().await.unwrap() {
                let num_rows = batch.num_rows();
                total_rows += num_rows;
                batch_num += 1;
            }

            info!(
                "\nMergeSstBench total rows of sst: {}, total batch num: {}, cost: {:?}",
                total_rows,
                batch_num,
                begin_instant.elapsed(),
            );
        });
    }

    fn run_no_dedup_bench(&self) {
        let space_id = self.space_id;
        let table_id = self.table_id;
        let projected_schema = self.sst_read_options.projected_schema.clone();
        let sst_factory: SstFactoryRef = Arc::new(FactoryImpl::default());

        let request_id = RequestId::next_id();
        let store_picker: ObjectStorePickerRef = Arc::new(self.store.clone());
        let builder = chain::Builder::new(ChainConfig {
            request_id,
            deadline: None,
            space_id,
            table_id,
            projected_schema,
            predicate: Arc::new(Predicate::empty()),
            sst_factory: &sst_factory,
            sst_read_options: self.sst_read_options.clone(),
            store_picker: &store_picker,
        })
        .ssts(vec![self.file_handles.clone()]);

        self.runtime.block_on(async {
            let begin_instant = Instant::now();

            let mut chain_iter = builder.build().await.unwrap();
            let mut total_rows = 0;
            let mut batch_num = 0;

            while let Some(batch) = chain_iter.next_batch().await.unwrap() {
                let num_rows = batch.num_rows();
                total_rows += num_rows;
                batch_num += 1;
            }

            info!(
                "\nMergeSstBench total rows of sst: {}, total batch num: {}, cost: {:?}",
                total_rows,
                batch_num,
                begin_instant.elapsed(),
            );
        });
    }

    pub fn run_bench(&self) {
        if self.dedup {
            self.run_dedup_bench();
        } else {
            self.run_no_dedup_bench();
        }
    }
}

impl Drop for MergeSstBench {
    fn drop(&mut self) {
        self.file_handles.clear();
    }
}
