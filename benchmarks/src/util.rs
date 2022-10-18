// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

//! Utilities.

use std::sync::Arc;

use analytic_engine::{
    memtable::{key::KeySequence, MemTableRef, PutContext},
    space::SpaceId,
    sst::{
        factory::{Factory, FactoryImpl, SstReaderOptions, SstType},
        file::{FileHandle, FileMeta, FilePurgeQueue, SstMetaData},
        manager::FileId,
        parquet::reader::{CachableParquetFileReaderFactory, ParquetSstReader},
    },
    table::sst_util,
};
use common_types::{
    bytes::MemBufMut,
    projected_schema::ProjectedSchema,
    schema::{IndexInWriterSchema, Schema},
};
use common_util::{
    define_result,
    runtime::{self, Runtime},
};
use datafusion::physical_plan::file_format::ParquetFileReaderFactory;
use futures::stream::StreamExt;
use object_store::{ObjectStoreRef, Path};
use parquet_ext::{DataCacheRef, MetaCacheRef};
use snafu::Snafu;
use table_engine::{predicate::Predicate, table::TableId};
use wal::log_batch::Payload;

#[derive(Debug, Snafu)]
pub enum Error {}

define_result!(Error);

pub fn new_runtime(thread_num: usize) -> Runtime {
    runtime::Builder::default()
        .thread_name("engine_bench")
        .worker_threads(thread_num)
        .enable_all()
        .build()
        .unwrap()
}

pub async fn meta_from_sst(
    store: &ObjectStoreRef,
    sst_path: &Path,
    meta_cache: &Option<MetaCacheRef>,
    data_cache: &Option<DataCacheRef>,
) -> SstMetaData {
    let reader_factory: Arc<dyn ParquetFileReaderFactory> = Arc::new(
        CachableParquetFileReaderFactory::new(store.clone(), data_cache.clone()),
    );

    ParquetSstReader::read_sst_meta(store, sst_path, None, meta_cache, &reader_factory)
        .await
        .unwrap()
}

pub async fn schema_from_sst(
    store: &ObjectStoreRef,
    sst_path: &Path,
    meta_cache: &Option<MetaCacheRef>,
    data_cache: &Option<DataCacheRef>,
) -> Schema {
    let sst_meta = meta_from_sst(store, sst_path, meta_cache, data_cache).await;

    sst_meta.schema
}

pub fn projected_schema_by_number(
    schema: &Schema,
    num_columns: usize,
    max_projections: usize,
) -> ProjectedSchema {
    if num_columns < max_projections {
        let projection = (0..num_columns + 1).into_iter().collect();

        ProjectedSchema::new(schema.clone(), Some(projection)).unwrap()
    } else {
        ProjectedSchema::no_projection(schema.clone())
    }
}

pub async fn load_sst_to_memtable(
    store: &ObjectStoreRef,
    sst_path: &Path,
    schema: &Schema,
    memtable: &MemTableRef,
    runtime: Arc<Runtime>,
) {
    let sst_reader_options = SstReaderOptions {
        sst_type: SstType::Parquet,
        read_batch_row_num: 500,
        reverse: false,
        projected_schema: ProjectedSchema::no_projection(schema.clone()),
        predicate: Arc::new(Predicate::empty()),
        meta_cache: None,
        data_cache: None,
        runtime,
    };
    let sst_factory = FactoryImpl;
    let mut sst_reader = sst_factory
        .new_sst_reader(&sst_reader_options, sst_path, store)
        .unwrap();

    let mut sst_stream = sst_reader.read().await.unwrap();
    let index_in_writer = IndexInWriterSchema::for_same_schema(schema.num_columns());
    let mut ctx = PutContext::new(index_in_writer);

    let mut sequence = crate::INIT_SEQUENCE;

    while let Some(batch) = sst_stream.next().await {
        let batch = batch.unwrap();

        for i in 0..batch.num_rows() {
            let row = batch.clone_row_at(i);

            let key_seq = KeySequence::new(sequence, i as u32);

            memtable.put(&mut ctx, key_seq, &row, schema).unwrap();

            sequence += 1;
        }
    }
}

pub async fn file_handles_from_ssts(
    store: &ObjectStoreRef,
    space_id: SpaceId,
    table_id: TableId,
    sst_file_ids: &[FileId],
    purge_queue: FilePurgeQueue,
    meta_cache: &Option<MetaCacheRef>,
    data_cache: &Option<DataCacheRef>,
) -> Vec<FileHandle> {
    let mut file_handles = Vec::with_capacity(sst_file_ids.len());

    for file_id in sst_file_ids.iter() {
        let path = sst_util::new_sst_file_path(space_id, table_id, *file_id);

        let sst_meta = meta_from_sst(store, &path, meta_cache, data_cache).await;
        let file_meta = FileMeta {
            id: *file_id,
            meta: sst_meta,
        };

        let handle = FileHandle::new(file_meta, purge_queue.clone());

        file_handles.push(handle);
    }

    file_handles
}

/// Header size in bytes
const HEADER_SIZE: usize = 1;

/// Wal entry header
#[derive(Clone, Copy)]
enum Header {
    Write = 1,
}

impl Header {
    pub fn to_u8(self) -> u8 {
        self as u8
    }
}

fn write_header(header: Header, buf: &mut dyn MemBufMut) -> Result<()> {
    buf.write_u8(header.to_u8())
        .expect("should succeed to write u8");
    Ok(())
}

#[derive(Debug)]
pub struct WritePayload<'a>(pub &'a [u8]);

impl<'a> Payload for WritePayload<'a> {
    type Error = Error;

    fn encode_size(&self) -> usize {
        let body_size = self.0.len();
        HEADER_SIZE + body_size as usize
    }

    fn encode_to<B: MemBufMut>(&self, buf: &mut B) -> Result<()> {
        write_header(Header::Write, buf).unwrap();
        buf.write_slice(self.0).unwrap();
        Ok(())
    }
}

impl<'a> From<&'a Vec<u8>> for WritePayload<'a> {
    fn from(data: &'a Vec<u8>) -> Self {
        Self(data)
    }
}
