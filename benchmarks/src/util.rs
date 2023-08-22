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

//! Utilities.

use std::sync::Arc;

use analytic_engine::{
    memtable::{key::KeySequence, MemTableRef, PutContext},
    space::SpaceId,
    sst::{
        factory::{
            Factory, FactoryImpl, ObjectStorePickerRef, ReadFrequency, ScanOptions, SstReadHint,
            SstReadOptions,
        },
        file::{FileHandle, FileMeta, FilePurgeQueue},
        manager::FileId,
        meta_data::cache::{self, MetaCacheRef},
        writer::MetaData,
    },
    table::sst_util,
    table_options::StorageFormat,
};
use bytes_ext::{BufMut, SafeBufMut};
use common_types::{
    projected_schema::ProjectedSchema,
    schema::{IndexInWriterSchema, Schema},
};
use macros::define_result;
use object_store::{ObjectStoreRef, Path};
use parquet::file::footer;
use runtime::Runtime;
use snafu::{ResultExt, Snafu};
use table_engine::{predicate::Predicate, table::TableId};
use wal::log_batch::Payload;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Failed to writer header, err:{}.", source))]
    WriteHeader { source: bytes_ext::Error },

    #[snafu(display("Failed to writer body, err:{}.", source))]
    WriteBody { source: bytes_ext::Error },
}

define_result!(Error);

pub fn new_runtime(thread_num: usize) -> Runtime {
    runtime::Builder::default()
        .thread_name("engine_bench")
        .worker_threads(thread_num)
        .enable_all()
        .build()
        .unwrap()
}

pub async fn parquet_metadata(
    store: &ObjectStoreRef,
    sst_path: &Path,
) -> parquet_ext::ParquetMetaData {
    let get_result = store.get(sst_path).await.unwrap();
    let chunk_reader = get_result.bytes().await.unwrap();
    footer::parse_metadata(&chunk_reader).unwrap()
}

pub async fn meta_from_sst(
    metadata: &parquet_ext::ParquetMetaData,
    store: &ObjectStoreRef,
    _meta_cache: &Option<MetaCacheRef>,
) -> MetaData {
    let md = cache::MetaData::try_new(metadata, false, store.clone())
        .await
        .unwrap();

    MetaData::from(md.custom().clone())
}

pub async fn schema_from_sst(
    store: &ObjectStoreRef,
    sst_path: &Path,
    meta_cache: &Option<MetaCacheRef>,
) -> Schema {
    let parquet_metadata = parquet_metadata(store, sst_path).await;
    let sst_meta = meta_from_sst(&parquet_metadata, store, meta_cache).await;
    sst_meta.schema
}

pub fn projected_schema_by_number(
    schema: &Schema,
    num_columns: usize,
    max_projections: usize,
) -> ProjectedSchema {
    if num_columns < max_projections {
        let projection = (0..num_columns + 1).collect();

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
    let scan_options = ScanOptions {
        background_read_parallelism: 1,
        max_record_batches_in_flight: 1024,
        num_streams_to_prefetch: 0,
    };
    let sst_read_options = SstReadOptions {
        frequency: ReadFrequency::Frequent,
        num_rows_per_row_group: 8192,
        projected_schema: ProjectedSchema::no_projection(schema.clone()),
        predicate: Arc::new(Predicate::empty()),
        meta_cache: None,
        scan_options,
        runtime,
    };
    let sst_factory = FactoryImpl;
    let store_picker: ObjectStorePickerRef = Arc::new(store.clone());
    let mut sst_reader = sst_factory
        .create_reader(
            sst_path,
            &sst_read_options,
            SstReadHint::default(),
            &store_picker,
            None,
        )
        .await
        .unwrap();

    let mut sst_stream = sst_reader.read().await.unwrap();
    let index_in_writer = IndexInWriterSchema::for_same_schema(schema.num_columns());
    let mut ctx = PutContext::new(index_in_writer);

    let mut sequence = crate::INIT_SEQUENCE;

    while let Some(batch) = sst_stream.fetch_next().await {
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
) -> Vec<FileHandle> {
    let mut file_handles = Vec::with_capacity(sst_file_ids.len());

    for file_id in sst_file_ids.iter() {
        let path = sst_util::new_sst_file_path(space_id, table_id, *file_id);
        let parquet_metadata = parquet_metadata(store, &path).await;
        let sst_meta = meta_from_sst(&parquet_metadata, store, meta_cache).await;

        let file_meta = FileMeta {
            id: *file_id,
            size: 0,
            row_num: 0,
            time_range: sst_meta.time_range,
            max_seq: sst_meta.max_sequence,
            storage_format: StorageFormat::Columnar,
            meta_path: None,
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

fn write_header<B: BufMut>(header: Header, buf: &mut B) -> Result<()> {
    buf.try_put_u8(header.to_u8()).context(WriteHeader)
}

#[derive(Debug)]
pub struct WritePayload<'a>(pub &'a [u8]);

impl<'a> Payload for WritePayload<'a> {
    type Error = Error;

    fn encode_size(&self) -> usize {
        let body_size = self.0.len();
        HEADER_SIZE + body_size
    }

    fn encode_to<B: BufMut>(&self, buf: &mut B) -> Result<()> {
        write_header(Header::Write, buf)?;
        buf.try_put(self.0).context(WriteBody)
    }
}

impl<'a> From<&'a Vec<u8>> for WritePayload<'a> {
    fn from(data: &'a Vec<u8>) -> Self {
        Self(data)
    }
}
