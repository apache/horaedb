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

//! Utilities.

use std::{collections::HashMap, future::Future, sync::Arc};

use analytic_engine::{
    memtable::{key::KeySequence, MemTableRef, PutContext},
    setup::{EngineBuilder, TableEngineContext},
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
    Config, RecoverMode,
};
use bytes_ext::{BufMut, SafeBufMut};
use common_types::{
    projected_schema::{ProjectedSchema, RowProjectorBuilder},
    record_batch::RecordBatch,
    row::RowGroup,
    schema::{IndexInWriterSchema, Schema},
    table::{ShardId, DEFAULT_SHARD_ID},
    time::Timestamp,
};
use futures::stream::StreamExt;
use macros::define_result;
use object_store::{
    config::{LocalOptions, ObjectStoreOptions, StorageOptions},
    ObjectStoreRef, Path,
};
use parquet::file::footer;
use runtime::{PriorityRuntime, Runtime};
use size_ext::ReadableSize;
use snafu::{ResultExt, Snafu};
use table_engine::{
    engine::{CreateTableRequest, EngineRuntimes, OpenShardRequest, TableDef, TableEngineRef},
    predicate::Predicate,
    table::{ReadRequest, SchemaId, TableId, TableRef, WriteRequest},
};
use tempfile::TempDir;
use time_ext::ReadableDuration;
use wal::{
    config::{Config as WalConfig, StorageConfig},
    log_batch::Payload,
    manager::{OpenedWals, WalRuntimes, WalsOpener},
    rocksdb_impl::{config::RocksDBStorageConfig, manager::RocksDBWalsOpener},
};

use crate::{table, table::FixedSchemaTable};

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
    let projected_schema = ProjectedSchema::no_projection(schema.clone());

    let fetched_schema = projected_schema.to_record_schema();
    let table_schema = projected_schema.table_schema().clone();
    let row_projector_builder = RowProjectorBuilder::new(fetched_schema, table_schema, None);
    let sst_read_options = SstReadOptions {
        maybe_table_level_metrics: None,
        frequency: ReadFrequency::Frequent,
        num_rows_per_row_group: 8192,
        predicate: Arc::new(Predicate::empty()),
        meta_cache: None,
        scan_options,
        runtime,
        row_projector_builder,
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
            size: store.head(&path).await.unwrap().size as u64,
            row_num: parquet_metadata.file_metadata().num_rows() as u64,
            time_range: sst_meta.time_range,
            max_seq: sst_meta.max_sequence,
            storage_format: StorageFormat::Columnar,
            associated_files: Vec::new(),
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

const DAY_MS: i64 = 24 * 60 * 60 * 1000;
/// 3 days ago.
pub fn start_ms() -> i64 {
    Timestamp::now().as_i64() - 3 * DAY_MS
}
#[derive(Clone, Copy, Debug)]
pub enum OpenTablesMethod {
    WithOpenTable,
    WithOpenShard,
}

pub struct TestEnv {
    _dir: TempDir,
    pub config: Config,
    pub runtimes: Arc<EngineRuntimes>,
}

pub struct Builder {
    num_workers: usize,
}

impl Builder {
    pub fn build(self) -> TestEnv {
        let dir = tempfile::tempdir().unwrap();

        let config = Config {
            storage: StorageOptions {
                mem_cache_capacity: ReadableSize::mb(0),
                mem_cache_partition_bits: 0,
                disk_cache_dir: "".to_string(),
                disk_cache_capacity: ReadableSize::mb(0),
                disk_cache_page_size: ReadableSize::mb(0),
                disk_cache_partition_bits: 0,
                object_store: ObjectStoreOptions::Local(LocalOptions {
                    data_dir: dir.path().to_str().unwrap().to_string(),
                    max_retries: 3,
                    timeout: Default::default(),
                }),
            },
            wal: WalConfig {
                storage: StorageConfig::RocksDB(Box::new(RocksDBStorageConfig {
                    data_dir: dir.path().to_str().unwrap().to_string(),
                    ..Default::default()
                })),
                disable_data: false,
            },
            ..Default::default()
        };

        let runtime = Arc::new(
            runtime::Builder::default()
                .worker_threads(self.num_workers)
                .enable_all()
                .build()
                .unwrap(),
        );

        TestEnv {
            _dir: dir,
            config,
            runtimes: Arc::new(EngineRuntimes {
                read_runtime: PriorityRuntime::new(runtime.clone(), runtime.clone()),
                write_runtime: runtime.clone(),
                meta_runtime: runtime.clone(),
                compact_runtime: runtime.clone(),
                default_runtime: runtime.clone(),
                io_runtime: runtime,
            }),
        }
    }
}

impl Default for Builder {
    fn default() -> Self {
        Self { num_workers: 2 }
    }
}

pub trait EngineBuildContext: Clone + Default {
    type WalsOpener: WalsOpener;

    fn wals_opener(&self) -> Self::WalsOpener;
    fn config(&self) -> Config;
    fn open_method(&self) -> OpenTablesMethod;
}

pub struct RocksDBEngineBuildContext {
    config: Config,
    open_method: OpenTablesMethod,
}

impl RocksDBEngineBuildContext {
    pub fn new(mode: RecoverMode, open_method: OpenTablesMethod) -> Self {
        let mut context = Self::default();
        context.config.recover_mode = mode;
        context.open_method = open_method;

        context
    }
}

impl Default for RocksDBEngineBuildContext {
    fn default() -> Self {
        let dir = tempfile::tempdir().unwrap();

        let config = Config {
            storage: StorageOptions {
                mem_cache_capacity: ReadableSize::mb(0),
                mem_cache_partition_bits: 0,
                disk_cache_dir: "".to_string(),
                disk_cache_capacity: ReadableSize::mb(0),
                disk_cache_page_size: ReadableSize::mb(0),
                disk_cache_partition_bits: 0,
                object_store: ObjectStoreOptions::Local(LocalOptions {
                    data_dir: dir.path().to_str().unwrap().to_string(),
                    max_retries: 3,
                    timeout: Default::default(),
                }),
            },
            wal: WalConfig {
                storage: StorageConfig::RocksDB(Box::new(RocksDBStorageConfig {
                    data_dir: dir.path().to_str().unwrap().to_string(),
                    ..Default::default()
                })),
                disable_data: false,
            },
            ..Default::default()
        };

        Self {
            config,
            open_method: OpenTablesMethod::WithOpenTable,
        }
    }
}

impl Clone for RocksDBEngineBuildContext {
    fn clone(&self) -> Self {
        let mut config = self.config.clone();

        let dir = tempfile::tempdir().unwrap();
        let storage = StorageOptions {
            mem_cache_capacity: ReadableSize::mb(0),
            mem_cache_partition_bits: 0,
            disk_cache_dir: "".to_string(),
            disk_cache_capacity: ReadableSize::mb(0),
            disk_cache_page_size: ReadableSize::mb(0),
            disk_cache_partition_bits: 0,
            object_store: ObjectStoreOptions::Local(LocalOptions {
                data_dir: dir.path().to_str().unwrap().to_string(),
                max_retries: 3,
                timeout: Default::default(),
            }),
        };

        config.storage = storage;
        config.wal = WalConfig {
            storage: StorageConfig::RocksDB(Box::new(RocksDBStorageConfig {
                data_dir: dir.path().to_str().unwrap().to_string(),
                ..Default::default()
            })),
            disable_data: false,
        };
        Self {
            config,
            open_method: self.open_method,
        }
    }
}

impl EngineBuildContext for RocksDBEngineBuildContext {
    type WalsOpener = RocksDBWalsOpener;

    fn wals_opener(&self) -> Self::WalsOpener {
        RocksDBWalsOpener
    }

    fn config(&self) -> Config {
        self.config.clone()
    }

    fn open_method(&self) -> OpenTablesMethod {
        self.open_method
    }
}

pub struct TestContext<T> {
    config: Config,
    wals_opener: T,
    runtimes: Arc<EngineRuntimes>,
    engine: Option<TableEngineRef>,
    opened_wals: Option<OpenedWals>,
    schema_id: SchemaId,
    last_table_seq: u32,

    name_to_tables: HashMap<String, TableRef>,
}

impl TestEnv {
    pub fn builder() -> Builder {
        Builder::default()
    }

    pub fn new_context<T: EngineBuildContext>(
        &self,
        build_context: &T,
    ) -> TestContext<T::WalsOpener> {
        let config = build_context.config();
        let wals_opener = build_context.wals_opener();

        TestContext {
            config,
            wals_opener,
            runtimes: self.runtimes.clone(),
            engine: None,
            opened_wals: None,
            schema_id: SchemaId::from_u32(100),
            last_table_seq: 1,
            name_to_tables: HashMap::new(),
        }
    }

    pub fn block_on<F: Future>(&self, future: F) -> F::Output {
        self.runtimes.default_runtime.block_on(future)
    }
}

impl<T: WalsOpener> TestContext<T> {
    pub async fn open(&mut self) {
        let opened_wals = if let Some(opened_wals) = self.opened_wals.take() {
            opened_wals
        } else {
            self.wals_opener
                .open_wals(
                    &self.config.wal,
                    WalRuntimes {
                        read_runtime: self.runtimes.read_runtime.high().clone(),
                        write_runtime: self.runtimes.write_runtime.clone(),
                        default_runtime: self.runtimes.default_runtime.clone(),
                    },
                )
                .await
                .unwrap()
        };

        let engine_builder = EngineBuilder {
            config: &self.config,
            engine_runtimes: self.runtimes.clone(),
            opened_wals: opened_wals.clone(),
            meta_client: None,
        };
        self.opened_wals = Some(opened_wals);

        let TableEngineContext { table_engine, .. } = engine_builder.build().await.unwrap();
        self.engine = Some(table_engine);
    }

    pub async fn create_fixed_schema_table(&mut self, table_name: &str) -> FixedSchemaTable {
        let fixed_schema_table = FixedSchemaTable::builder()
            .schema_id(self.schema_id)
            .table_name(table_name.to_string())
            .table_id(self.next_table_id())
            .ttl("7d".parse::<ReadableDuration>().unwrap())
            .build_fixed();

        self.create_table(fixed_schema_table.create_request().clone())
            .await;

        fixed_schema_table
    }

    fn next_table_id(&mut self) -> TableId {
        self.last_table_seq += 1;
        table::new_table_id(2, self.last_table_seq)
    }

    async fn create_table(&mut self, create_request: CreateTableRequest) {
        let table_name = create_request.params.table_name.clone();
        let table = self.engine().create_table(create_request).await.unwrap();

        self.name_to_tables.insert(table_name.to_string(), table);
    }

    #[inline]
    pub fn engine(&self) -> &TableEngineRef {
        self.engine.as_ref().unwrap()
    }

    pub async fn write_to_table(&self, table_name: &str, row_group: RowGroup) {
        let table = self.table(table_name);

        table.write(WriteRequest { row_group }).await.unwrap();
    }

    pub fn table(&self, table_name: &str) -> TableRef {
        self.name_to_tables.get(table_name).cloned().unwrap()
    }

    pub async fn read_table(
        &self,
        table_name: &str,
        read_request: ReadRequest,
    ) -> Vec<RecordBatch> {
        let table = self.table(table_name);

        let mut stream = table.read(read_request).await.unwrap();
        let mut record_batches = Vec::new();
        while let Some(batch) = stream.next().await {
            let batch = batch.unwrap();

            record_batches.push(batch);
        }

        record_batches
    }

    pub async fn partitioned_read_table(
        &self,
        table_name: &str,
        read_request: ReadRequest,
    ) -> Vec<RecordBatch> {
        let table = self.table(table_name);

        let streams = table.partitioned_read(read_request).await.unwrap();
        let mut record_batches = Vec::new();

        for mut stream in streams.streams {
            while let Some(batch) = stream.next().await {
                let batch = batch.unwrap();

                record_batches.push(batch);
            }
        }

        record_batches
    }

    pub async fn reopen_with_tables(&mut self, tables: &[&str]) {
        let table_infos: Vec<_> = tables
            .iter()
            .map(|name| {
                let table_id = self.name_to_tables.get(*name).unwrap().id();
                (table_id, *name)
            })
            .collect();
        {
            // Close all tables.
            self.name_to_tables.clear();

            // Close engine.
            let engine = self.engine.take().unwrap();
            engine.close().await.unwrap();
        }

        self.open().await;

        self.open_tables_of_shard(table_infos, DEFAULT_SHARD_ID)
            .await;
    }

    async fn open_tables_of_shard(&mut self, table_infos: Vec<(TableId, &str)>, shard_id: ShardId) {
        let table_defs = table_infos
            .into_iter()
            .map(|table| TableDef {
                catalog_name: "horaedb".to_string(),
                schema_name: "public".to_string(),
                schema_id: self.schema_id,
                id: table.0,
                name: table.1.to_string(),
            })
            .collect();

        let open_shard_request = OpenShardRequest {
            shard_id,
            table_defs,
            engine: table_engine::ANALYTIC_ENGINE_TYPE.to_string(),
        };

        let tables = self
            .engine()
            .open_shard(open_shard_request)
            .await
            .unwrap()
            .into_values()
            .map(|result| result.unwrap().unwrap());

        for table in tables {
            self.name_to_tables.insert(table.name().to_string(), table);
        }
    }

    pub fn name_to_tables(&self) -> &HashMap<String, TableRef> {
        &self.name_to_tables
    }
}
