use std::{
    ops::Range,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
    time::Instant,
};

use arrow::datatypes::SchemaRef as ArrowSchemaRef;
use async_trait::async_trait;
use bytes::Bytes;
use common_types::{
    projected_schema::{ProjectedSchema, RowProjector},
    record_batch::{ArrowRecordBatchProjector, RecordBatchWithKey},
};
use common_util::runtime::Runtime;
use datafusion::{
    datasource::{file_format, listing::PartitionedFile, object_store::ObjectStoreUrl},
    execution::context::TaskContext,
    physical_plan::{
        execute_stream,
        file_format::{
            FileMeta, FileScanConfig, ParquetExec, ParquetFileMetrics, ParquetFileReaderFactory,
        },
        metrics::ExecutionPlanMetricsSet,
        stream::RecordBatchStreamAdapter,
        SendableRecordBatchStream, Statistics,
    },
    prelude::{Expr, SessionContext},
};
use futures::{
    future::{self, BoxFuture},
    FutureExt, Stream, StreamExt, TryFutureExt, TryStreamExt,
};
use log::{debug, error};
use object_store::{ObjectMeta, ObjectStoreRef, Path};
use parquet::arrow::async_reader::AsyncFileReader;
use parquet_ext::{DataCacheRef, MetaCacheRef};
use snafu::{ensure, OptionExt, ResultExt};
use table_engine::predicate::PredicateRef;
use tokio::{
    runtime::Handle,
    sync::mpsc::{self, Receiver, Sender},
};

use super::encoding;
use crate::{
    sst::{
        factory::SstReaderOptions,
        file::SstMetaData,
        reader::{self, Result, SstReader},
    },
    table_options::StorageFormatOptions,
};

pub struct ParquetSstReader<'a> {
    /// The path where the data is persisted.
    path: &'a Path,
    /// The storage where the data is persist.
    storage: &'a ObjectStoreRef,
    runtime: Arc<Runtime>,
    projected_schema: ProjectedSchema,
    reader_factory: Arc<dyn ParquetFileReaderFactory>,
    /// init this field in `init_if_necessary`
    meta_data: Option<SstMetaData>,
    object_meta: Option<ObjectMeta>,

    channel_cap: usize,

    schema: ArrowSchemaRef,
    meta_cache: Option<MetaCacheRef>,
    data_cache: Option<DataCacheRef>,
    predicate: PredicateRef,
}

const DEFAULT_CHANNEL_CAP: usize = 1000;

impl<'a> ParquetSstReader<'a> {
    pub fn new(path: &'a Path, storage: &'a ObjectStoreRef, options: &SstReaderOptions) -> Self {
        let schema_to_read = options.projected_schema.to_projected_arrow_schema();
        let reader_factory = Arc::new(CachableParquetFileReaderFactory {
            storage: storage.clone(),
            meta_cache: options.meta_cache.clone(),
            data_cache: options.data_cache.clone(),
        });
        Self {
            path,
            storage,
            reader_factory,
            meta_data: None,
            object_meta: None,
            runtime: options.runtime.clone(),
            projected_schema: options.projected_schema.clone(),
            channel_cap: DEFAULT_CHANNEL_CAP,
            schema: schema_to_read,
            meta_cache: options.meta_cache.clone(),
            data_cache: options.data_cache.clone(),
            predicate: options.predicate.clone(),
        }
    }

    fn send_record_batch_with_key(&mut self, tx: Sender<Result<RecordBatchWithKey>>) -> Result<()> {
        assert!(self.meta_data.is_some());

        let meta_data = self.meta_data.as_ref().unwrap();
        let object_meta = self.object_meta.as_ref().unwrap().clone();

        let storage_format_opts = meta_data.storage_format_opts.clone();
        let schema = meta_data.schema.clone();
        let arrow_schema = schema.to_arrow_schema_ref();
        let storage = self.storage.clone();
        let scan_config = FileScanConfig {
            object_store_url: ObjectStoreUrl::parse("ceresdb://ceresdb/")
                .expect("valid object store URL"),
            file_schema: arrow_schema.clone(),
            file_groups: vec![vec![PartitionedFile {
                object_meta: object_meta.clone(),
                partition_values: vec![],
                range: None,
                extensions: None,
            }]],
            statistics: Statistics::default(),
            projection: None,
            limit: None,
            table_partition_cols: vec![],
        };
        let filter_expr = self.predicate.filter_expr(schema.timestamp_name());
        debug!(
            "send record_batch, object_meta:{:?}, filter:{:?}, scan_config:{:?}",
            object_meta, filter_expr, scan_config
        );

        let reader_factory = self.reader_factory.clone();
        let row_projector = self
            .projected_schema
            .try_project_with_key(&schema)
            .map_err(|e| Box::new(e) as _)
            .context(reader::error::Projection)?;

        let rt = self.runtime.clone();
        // let handle = Handle::current();

        self.runtime.spawn_blocking(move || {
            let mut send_failed = false;
            let send = |v| -> Result<()> {
                tx.blocking_send(v)
                    .map_err(|e| {
                        send_failed = true;
                        Box::new(e) as _
                    })
                    .context(reader::error::Other)?;
                Ok(())
            };

            let reader = ProjectAndFilterReader {
                storage,
                object_meta,
                arrow_schema,
                scan_config,
                filter_expr,
                reader_factory,
                row_projector,
                storage_format_opts,
            };

            let start_fetch = Instant::now();
            debug!("begin fetch:{:?}", start_fetch);
            match rt.block_on(reader.fetch_and_send_record_batch(send)) {
                Ok(row_num) => {
                    debug!(
                        "finish reading record batch({} rows) from the sst:{}, time cost:{:?}",
                        row_num,
                        "",
                        start_fetch.elapsed(),
                    );
                }
                Err(e) => {
                    if send_failed {
                        error!("fail to send the fetched record batch result, err:{}", e);
                    } else {
                        error!("failed to read record batch from the sst:{}, err:{}", "", e);
                        let _ = tx.blocking_send(Err(e));
                    }
                }
            }
        });
        // let source_schema = &self.meta_data.as_ref().unwrap().schema;
        // let row_projector = self
        //     .projected_schema
        //     .try_project_with_key(source_schema)
        //     .unwrap();
        // let arrow_record_batch_projector =
        // ArrowRecordBatchProjector::from(row_projector); let record_batches =
        // self.read_record_batches().await?; record_batches.map(|record_batch|
        // {     let record_batch = record_batch
        //         .map_err(|e| Box::new(e) as _)
        //         .context(reader::error::Other {})?;

        //     arrow_record_batch_projector
        //         .project_to_record_batch_with_key(record_batch)
        //         .map_err(|e| Box::new(e) as _)
        //         .context(reader::error::Other {})
        // });

        // let mut rows = 0;
        // while let Some(record_batch) = record_batches.next().await {
        //     let record_batch = record_batch
        //         .map_err(|e| Box::new(e) as _)
        //         .context(reader::error::Other {})?;

        //     rows += record_batch.num_rows();
        //     let record_batch_with_key = arrow_record_batch_projector
        //         .project_to_record_batch_with_key(record_batch)
        //         .map_err(|e| Box::new(e) as _)
        //         .context(reader::error::Other {});

        //     tx.blocking_send(record_batch_with_key)
        //         .map_err(|e| Box::new(e) as _)
        //         .context(reader::error::Other {})?;
        // }

        Ok(())
    }

    async fn init_if_necessary(&mut self) -> Result<()> {
        if self.meta_data.is_some() {
            return Ok(());
        }

        let object_meta = self
            .storage
            .head(self.path)
            .await
            .map_err(|e| Box::new(e) as _)
            .context(reader::error::Other {})?;
        self.object_meta = Some(object_meta.clone());

        let mut reader = self
            .reader_factory
            .create_reader(0, object_meta.into(), None, &ExecutionPlanMetricsSet::new())
            .map_err(|e| Box::new(e) as _)
            .context(reader::error::Other {})?;

        reader
            .get_metadata()
            .map_ok(|metadata| -> Result<()> {
                if let Some(cache) = &self.meta_cache {
                    cache.put(self.path.to_string(), metadata.clone());
                }
                let kv_metas = metadata
                    .file_metadata()
                    .key_value_metadata()
                    .context(reader::error::SstMetaNotFound)?;
                ensure!(!kv_metas.is_empty(), reader::error::EmptySstMeta);

                let sst_meta = encoding::decode_sst_meta_data(&kv_metas[0])
                    .map_err(|e| Box::new(e) as _)
                    .context(reader::error::DecodeSstMeta)?;

                debug!("read sst_meta, path:{}, meta:{:?}", &self.path, sst_meta);
                self.meta_data = Some(sst_meta);
                Ok(())
            })
            .await
            .map_err(|e| Box::new(e) as _)
            .context(reader::error::Other {})?
    }
}

struct ProjectAndFilterReader {
    storage: ObjectStoreRef,
    arrow_schema: ArrowSchemaRef,
    object_meta: ObjectMeta,
    scan_config: FileScanConfig,
    filter_expr: Expr,
    reader_factory: Arc<dyn ParquetFileReaderFactory>,
    row_projector: RowProjector,
    storage_format_opts: StorageFormatOptions,
}

impl ProjectAndFilterReader {
    async fn fetch_and_send_record_batch(
        self,
        mut send: impl FnMut(Result<RecordBatchWithKey>) -> Result<()>,
    ) -> Result<usize> {
        let mut row_num = 0;
        let mut record_batches = self.fetch_record_batch().await?;
        let arrow_record_batch_projector = ArrowRecordBatchProjector::from(self.row_projector);
        while let Some(record_batch) = record_batches.next().await {
            let record_batch = record_batch
                .map_err(|e| Box::new(e) as _)
                .context(reader::error::Other {})?;

            row_num += record_batch.num_rows();
            let record_batch_with_key = arrow_record_batch_projector
                .project_to_record_batch_with_key(record_batch)
                .map_err(|e| Box::new(e) as _)
                .context(reader::error::Other {});

            send(record_batch_with_key)?;
        }

        Ok(row_num)
    }

    async fn fetch_record_batch(&self) -> Result<SendableRecordBatchStream> {
        let exec = ParquetExec::new(
            self.scan_config.clone(),
            Some(self.filter_expr.clone()),
            None,
        )
        .with_parquet_file_reader_factory(self.reader_factory.clone());

        // set up "fake" DataFusion session
        let session_ctx = SessionContext::new();
        let task_ctx = Arc::new(TaskContext::from(&session_ctx));
        task_ctx
            .runtime_env()
            .register_object_store("ceresdb", "ceresdb", self.storage.clone());

        execute_stream(Arc::new(exec), task_ctx)
            .await
            .context(reader::error::DataFusionError {})
        // Ok(Box::pin(RecordBatchStreamAdapter::new(
        //     self.arrow_schema.clone(),
        //     futures::stream::once(execute_stream(Arc::new(exec),
        // task_ctx)).try_flatten(), )))
    }
}
/*
impl Stream for ProjectAndFilterReader {
    type Item = Result<RecordBatchWithKey>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let reader = self.get_mut();
        match reader.fetch_record_batch().poll() {
            Poll::Pending => Poll::Pending,
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Ready(Some(record_batch)) => {
                let arrow_record_batch_projector =
                    ArrowRecordBatchProjector::from(self.row_projector);
                let record_batch = record_batch
                    .map_err(|e| Box::new(e) as _)
                    .context(reader::error::Other {})?;

                let x = arrow_record_batch_projector
                    .project_to_record_batch_with_key(record_batch)
                    .map_err(|e| Box::new(e) as _)
                    .context(reader::error::Other {});
                Poll::from(Some(x))
            }
        }
    }
}
*/

#[derive(Debug)]
struct CachableParquetFileReaderFactory {
    storage: ObjectStoreRef,
    data_cache: Option<DataCacheRef>,
    meta_cache: Option<MetaCacheRef>,
}

impl ParquetFileReaderFactory for CachableParquetFileReaderFactory {
    fn create_reader(
        &self,
        partition_index: usize,
        file_meta: FileMeta,
        metadata_size_hint: Option<usize>,
        metrics: &ExecutionPlanMetricsSet,
    ) -> datafusion::error::Result<Box<dyn AsyncFileReader + Send>> {
        let parquet_file_metrics =
            ParquetFileMetrics::new(partition_index, file_meta.location().as_ref(), metrics);

        Ok(Box::new(CachableParquetFileReader {
            storage: self.storage.clone(),
            data_cache: self.data_cache.clone(),
            meta_cache: self.meta_cache.clone(),
            meta: file_meta.object_meta,
            metadata_size_hint,
            metrics: parquet_file_metrics,
        }))
    }
}

struct CachableParquetFileReader {
    storage: ObjectStoreRef,
    data_cache: Option<DataCacheRef>,
    meta_cache: Option<MetaCacheRef>,
    meta: ObjectMeta,
    metrics: ParquetFileMetrics,
    metadata_size_hint: Option<usize>,
}

impl CachableParquetFileReader {
    fn format_page_data_key(name: &str, start: usize, end: usize) -> String {
        format!("{}_{}_{}", name, start, end)
    }
}

impl AsyncFileReader for CachableParquetFileReader {
    fn get_bytes(&mut self, range: Range<usize>) -> BoxFuture<'_, parquet::errors::Result<Bytes>> {
        self.metrics.bytes_scanned.add(range.end - range.start);
        if let Some(cache) = &self.data_cache {
            let key =
                Self::format_page_data_key(self.meta.location.as_ref(), range.start, range.end);
            if let Some(cached_bytes) = cache.get(&key) {
                return Box::pin(future::ok(Bytes::from(cached_bytes.to_vec())));
            };
        }

        let cache = self.data_cache.clone();
        let key = Self::format_page_data_key(self.meta.location.as_ref(), range.start, range.end);
        self.storage
            .get_range(&self.meta.location, range)
            .map_ok(move |bytes| {
                if let Some(cache) = cache {
                    cache.put(key, Arc::new(bytes.to_vec()));
                }
                bytes
            })
            .map_err(|e| {
                parquet::errors::ParquetError::General(format!(
                    "CachableParquetFileReader::get_bytes error: {}",
                    e
                ))
            })
            .boxed()
    }

    fn get_metadata(
        &mut self,
    ) -> BoxFuture<'_, parquet::errors::Result<Arc<parquet::file::metadata::ParquetMetaData>>> {
        Box::pin(async move {
            let metadata = file_format::parquet::fetch_parquet_metadata(
                self.storage.as_ref(),
                &self.meta,
                self.metadata_size_hint,
            )
            .await
            .map_err(|e| {
                parquet::errors::ParquetError::General(format!(
                    "CachableParquetFileReader::get_metadata error: {}",
                    e
                ))
            })?;
            Ok(Arc::new(metadata))
        })
    }
}

struct RecordBatchReceiver {
    rx: Receiver<Result<RecordBatchWithKey>>,
}

impl Stream for RecordBatchReceiver {
    type Item = Result<RecordBatchWithKey>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.as_mut().rx.poll_recv(cx)
    }
}

#[async_trait]
impl<'a> SstReader for ParquetSstReader<'a> {
    async fn meta_data(&mut self) -> Result<&SstMetaData> {
        self.init_if_necessary().await?;
        Ok(self.meta_data.as_ref().unwrap())
    }

    async fn read(
        &mut self,
    ) -> Result<Box<dyn Stream<Item = Result<RecordBatchWithKey>> + Send + Unpin>> {
        self.init_if_necessary().await?;
        let (tx, rx) = mpsc::channel::<Result<RecordBatchWithKey>>(self.channel_cap);
        self.send_record_batch_with_key(tx)?;

        Ok(Box::new(RecordBatchReceiver { rx }))
    }
}
