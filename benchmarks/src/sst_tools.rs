// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

//! Tools to generate SST.

use std::sync::Arc;

use analytic_engine::{
    row_iter::{
        self,
        dedup::DedupIterator,
        merge::{MergeBuilder, MergeConfig},
        IterOptions,
    },
    space::SpaceId,
    sst::{
        builder::RecordBatchStream,
        factory::{
            Factory, FactoryImpl, FactoryRef as SstFactoryRef, SstBuilderOptions, SstReaderOptions,
            SstType,
        },
        file::{self, FilePurgeQueue, SstMetaData},
        manager::FileId,
    },
    table::sst_util,
    table_options::Compression,
};
use common_types::{projected_schema::ProjectedSchema, request_id::RequestId};
use common_util::runtime::Runtime;
use futures::TryStreamExt;
use log::info;
use object_store::{LocalFileSystem, ObjectStoreRef, Path};
use serde_derive::Deserialize;
use table_engine::{predicate::Predicate, table::TableId};
use tokio::sync::mpsc;

use crate::{config::BenchPredicate, util};

#[derive(Debug)]
struct SstConfig {
    sst_meta: SstMetaData,
    store_path: String,
    sst_file_name: String,
    num_rows_per_row_group: usize,
    compression: Compression,
}

async fn create_sst_from_stream(config: SstConfig, record_batch_stream: RecordBatchStream) {
    let sst_factory = FactoryImpl;
    let sst_builder_options = SstBuilderOptions {
        sst_type: SstType::Parquet,
        num_rows_per_row_group: config.num_rows_per_row_group,
        compression: config.compression,
    };

    info!(
        "create sst from stream, config:{:?}, sst_builder_options:{:?}",
        config, sst_builder_options
    );

    let store = Arc::new(LocalFileSystem::new_with_prefix(config.store_path).unwrap()) as _;
    let sst_file_path = Path::from(config.sst_file_name);

    let mut builder = sst_factory
        .new_sst_builder(&sst_builder_options, &sst_file_path, &store)
        .unwrap();
    builder
        .build(RequestId::next_id(), &config.sst_meta, record_batch_stream)
        .await
        .unwrap();
}

#[derive(Debug, Deserialize)]
pub struct RebuildSstConfig {
    store_path: String,
    input_file_name: String,
    read_batch_row_num: usize,
    predicate: BenchPredicate,

    // Output sst config:
    output_file_name: String,
    num_rows_per_row_group: usize,
    compression: Compression,
}

pub async fn rebuild_sst(config: RebuildSstConfig, runtime: Arc<Runtime>) {
    info!("Start rebuild sst, config:{:?}", config);

    let store = Arc::new(LocalFileSystem::new_with_prefix(config.store_path.clone()).unwrap()) as _;
    let input_path = Path::from(config.input_file_name);

    let sst_meta = util::meta_from_sst(&store, &input_path, &None).await;

    let projected_schema = ProjectedSchema::no_projection(sst_meta.schema.clone());
    let sst_reader_options = SstReaderOptions {
        sst_type: SstType::Parquet,
        read_batch_row_num: config.read_batch_row_num,
        reverse: false,
        just_once: false,
        projected_schema,
        predicate: config.predicate.into_predicate(),
        meta_cache: None,
        runtime,
        background_read_parallelism: 1,
        num_rows_per_row_group: config.read_batch_row_num,
    };

    let record_batch_stream =
        sst_to_record_batch_stream(&sst_reader_options, &input_path, &store).await;

    let output_sst_config = SstConfig {
        sst_meta,
        store_path: config.store_path,
        sst_file_name: config.output_file_name,
        num_rows_per_row_group: config.num_rows_per_row_group,
        compression: config.compression,
    };

    create_sst_from_stream(output_sst_config, record_batch_stream).await;

    info!("Start rebuild sst done");
}

async fn sst_to_record_batch_stream(
    sst_reader_options: &SstReaderOptions,
    input_path: &Path,
    store: &ObjectStoreRef,
) -> RecordBatchStream {
    let sst_factory = FactoryImpl;
    let mut sst_reader = sst_factory
        .new_sst_reader(sst_reader_options, input_path, store)
        .unwrap();

    let sst_stream = sst_reader.read().await.unwrap();

    Box::new(sst_stream.map_err(|e| Box::new(e) as _))
}

#[derive(Debug, Deserialize)]
pub struct MergeSstConfig {
    store_path: String,
    space_id: SpaceId,
    table_id: TableId,
    sst_file_ids: Vec<FileId>,
    dedup: bool,
    read_batch_row_num: usize,
    predicate: BenchPredicate,

    // Output sst config:
    output_store_path: String,
    output_file_name: String,
    num_rows_per_row_group: usize,
    compression: Compression,
}

pub async fn merge_sst(config: MergeSstConfig, runtime: Arc<Runtime>) {
    if config.sst_file_ids.is_empty() {
        info!("No input files to merge");
        return;
    }

    info!("Merge sst begin, config:{:?}", config);

    let space_id = config.space_id;
    let table_id = config.table_id;
    let store = Arc::new(LocalFileSystem::new_with_prefix(config.store_path.clone()).unwrap()) as _;
    let (tx, _rx) = mpsc::unbounded_channel();
    let purge_queue = FilePurgeQueue::new(space_id, table_id, tx);

    let file_handles = util::file_handles_from_ssts(
        &store,
        space_id,
        table_id,
        &config.sst_file_ids,
        purge_queue,
        &None,
    )
    .await;
    let max_sequence = file_handles
        .iter()
        .map(|file| file.max_sequence())
        .max()
        .unwrap();

    let first_sst_path = sst_util::new_sst_file_path(space_id, table_id, config.sst_file_ids[0]);
    let schema = util::schema_from_sst(&store, &first_sst_path, &None).await;
    let iter_options = IterOptions {
        batch_size: config.read_batch_row_num,
        sst_background_read_parallelism: 1,
    };

    let request_id = RequestId::next_id();
    let iter = {
        let space_id = config.space_id;
        let table_id = config.table_id;
        let sequence = max_sequence + 1;
        let projected_schema = ProjectedSchema::no_projection(schema.clone());
        let sst_reader_options = SstReaderOptions {
            sst_type: SstType::Parquet,
            read_batch_row_num: config.read_batch_row_num,
            reverse: false,
            just_once: false,
            projected_schema: projected_schema.clone(),
            predicate: config.predicate.into_predicate(),
            meta_cache: None,
            runtime: runtime.clone(),
            background_read_parallelism: iter_options.sst_background_read_parallelism,
            num_rows_per_row_group: config.read_batch_row_num,
        };

        let sst_factory: SstFactoryRef = Arc::new(FactoryImpl::default());
        let mut builder = MergeBuilder::new(MergeConfig {
            request_id,
            space_id,
            table_id,
            sequence,
            projected_schema,
            predicate: Arc::new(Predicate::empty()),
            sst_factory: &sst_factory,
            sst_reader_options,
            store: &store,
            merge_iter_options: iter_options.clone(),
            need_dedup: true,
            reverse: false,
        });
        builder
            .mut_ssts_of_level(0)
            .extend_from_slice(&file_handles);

        builder.build().await.unwrap()
    };

    let record_batch_stream = if config.dedup {
        let iter = DedupIterator::new(request_id, iter, iter_options);
        row_iter::record_batch_with_key_iter_to_stream(iter, &runtime)
    } else {
        row_iter::record_batch_with_key_iter_to_stream(iter, &runtime)
    };

    let sst_meta = file::merge_sst_meta(&file_handles, schema);
    let output_sst_config = SstConfig {
        sst_meta,
        store_path: config.output_store_path,
        sst_file_name: config.output_file_name,
        num_rows_per_row_group: config.num_rows_per_row_group,
        compression: config.compression,
    };

    create_sst_from_stream(output_sst_config, record_batch_stream).await;

    info!("Merge sst done");
}
