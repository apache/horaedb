// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

//! Sst reader implementation based on parquet.

use std::{
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
    time::Instant,
};

use arrow::{error::Result as ArrowResult, record_batch::RecordBatch};
use async_trait::async_trait;
use bytes::Bytes;
use common_types::{
    projected_schema::{ProjectedSchema, RowProjector},
    record_batch::{ArrowRecordBatchProjector, RecordBatchWithKey},
    schema::Schema,
};
use common_util::runtime::Runtime;
use futures::Stream;
use log::{debug, error, trace};
use object_store::{ObjectStoreRef, Path};
use parquet::{
    arrow::{arrow_reader::ParquetRecordBatchReaderBuilder, ProjectionMask},
    file::metadata::{ParquetMetaData, RowGroupMetaData},
};
use parquet_ext::reverse_reader::Builder as ReverseRecordBatchReaderBuilder;
use snafu::{ensure, OptionExt, ResultExt};
use table_engine::predicate::PredicateRef;
use tokio::sync::mpsc::{self, Receiver, Sender};

use super::row_group_filter::RowGroupFilter;
use crate::sst::{
    factory::SstReaderOptions,
    file::SstMetaData,
    meta_cache::MetaCacheRef,
    parquet::encoding::{self, ParquetDecoder},
    reader::{error::*, SstReader},
};

const DEFAULT_CHANNEL_CAP: usize = 1000;

pub async fn make_sst_chunk_reader(storage: &ObjectStoreRef, path: &Path) -> Result<Bytes> {
    let get_result = storage
        .get(path)
        .await
        .map_err(|e| Box::new(e) as _)
        .with_context(|| ReadPersist {
            path: path.to_string(),
        })?;
    // TODO: The `ChunkReader` (trait from parquet crate) doesn't support async
    // read. So under this situation it would be better to pass a local file to
    // it, avoiding consumes lots of memory. Once parquet support stream data source
    // we can feed the `GetResult` to it directly.
    get_result
        .bytes()
        .await
        .map_err(|e| Box::new(e) as _)
        .context(ReadPersist {
            path: path.to_string(),
        })
}

pub fn make_sst_reader_builder(
    chunk_reader: Bytes,
) -> Result<ParquetRecordBatchReaderBuilder<Bytes>> {
    ParquetRecordBatchReaderBuilder::try_new(chunk_reader)
        .map_err(|e| Box::new(e) as _)
        .context(DecodeRecordBatch)
}

pub fn read_sst_meta(metadata: &ParquetMetaData) -> Result<SstMetaData> {
    // parse sst meta data
    let kv_metas = metadata
        .file_metadata()
        .key_value_metadata()
        .context(SstMetaNotFound)?;

    ensure!(!kv_metas.is_empty(), EmptySstMeta);

    encoding::decode_sst_meta_data(&kv_metas[0])
        .map_err(|e| Box::new(e) as _)
        .context(DecodeSstMeta)
}

/// The implementation of sst based on parquet and object storage.
pub struct ParquetSstReader<'a> {
    /// The path where the data is persisted.
    path: &'a Path,
    /// The storage where the data is persist.
    storage: &'a ObjectStoreRef,
    projected_schema: ProjectedSchema,
    predicate: PredicateRef,
    meta_data: Option<SstMetaData>,
    chunk_reader: Option<Bytes>,
    reader_builder: Option<ParquetRecordBatchReaderBuilder<Bytes>>,
    /// The batch of rows in one `record_batch`.
    batch_size: usize,
    /// Read the rows in reverse order.
    reverse: bool,
    channel_cap: usize,

    #[allow(unused)]
    meta_cache: Option<MetaCacheRef>,

    runtime: Arc<Runtime>,
}

impl<'a> ParquetSstReader<'a> {
    pub fn new(path: &'a Path, storage: &'a ObjectStoreRef, options: &SstReaderOptions) -> Self {
        Self {
            path,
            storage,
            projected_schema: options.projected_schema.clone(),
            predicate: options.predicate.clone(),
            meta_data: None,
            chunk_reader: None,
            reader_builder: None,
            batch_size: options.read_batch_row_num,
            reverse: options.reverse,
            channel_cap: DEFAULT_CHANNEL_CAP,
            meta_cache: options.meta_cache.clone(),
            runtime: options.runtime.clone(),
        }
    }
}

impl<'a> ParquetSstReader<'a> {
    async fn init_if_necessary(&mut self) -> Result<()> {
        if self.meta_data.is_some() {
            return Ok(());
        }

        let chunk_reader = make_sst_chunk_reader(self.storage, self.path).await?;
        let reader_builder = make_sst_reader_builder(chunk_reader.clone())?;
        let meta_data = read_sst_meta(reader_builder.metadata())?;

        self.reader_builder = Some(reader_builder);
        self.meta_data = Some(meta_data);
        self.chunk_reader = Some(chunk_reader);

        Ok(())
    }

    fn read_record_batches(&mut self, tx: Sender<Result<RecordBatchWithKey>>) -> Result<()> {
        let path = self.path.to_string();
        ensure!(self.reader_builder.is_some(), ReadAgain { path });
        ensure!(self.chunk_reader.is_some(), ReadAgain { path });
        ensure!(self.meta_data.is_some(), ReadAgain { path });

        let reader_builder = self.reader_builder.take();
        let chunk_reader = self.chunk_reader.take().unwrap();
        let batch_size = self.batch_size;
        let schema = {
            let meta_data = self.meta_data.as_ref().unwrap();
            meta_data.schema.clone()
        };
        let projected_schema = self.projected_schema.clone();
        let row_projector = projected_schema
            .try_project_with_key(&schema)
            .map_err(|e| Box::new(e) as _)
            .context(Projection)?;
        let predicate = self.predicate.clone();
        let reverse = self.reverse;

        let meta_data = self.meta_data.take().unwrap();
        let _ = self.runtime.spawn_blocking(move || {
            debug!(
                "begin reading record batch from the sst:{}, predicate:{:?}, projection:{:?}",
                path, predicate, projected_schema,
            );

            let mut send_failed = false;
            let send = |v| -> Result<()> {
                tx.blocking_send(v)
                    .map_err(|e| {
                        send_failed = true;
                        Box::new(e) as _
                    })
                    .context(Other)?;
                Ok(())
            };

            let reader = ProjectAndFilterReader {
                file_path: path.clone(),
                chunk_reader,
                reader_builder,
                schema,
                projected_schema,
                row_projector,
                predicate,
                batch_size,
                reverse,
                meta_data,
            };

            let start_fetch = Instant::now();
            match reader.fetch_and_send_record_batch(send) {
                Ok(row_num) => {
                    debug!(
                        "finish reading record batch({} rows) from the sst:{}, time cost:{:?}",
                        row_num,
                        path,
                        start_fetch.elapsed(),
                    );
                }
                Err(e) => {
                    if send_failed {
                        error!("fail to send the fetched record batch result, err:{}", e);
                    } else {
                        error!(
                            "failed to read record batch from the sst:{}, err:{}",
                            path, e
                        );
                        let _ = tx.blocking_send(Err(e));
                    }
                }
            }
        });

        Ok(())
    }

    #[cfg(test)]
    pub(crate) async fn row_groups(&mut self) -> &[parquet::file::metadata::RowGroupMetaData] {
        self.init_if_necessary().await.unwrap();
        self.reader_builder
            .as_ref()
            .unwrap()
            .metadata()
            .row_groups()
    }
}

/// A reader for projection and filter on the parquet file.
struct ProjectAndFilterReader {
    file_path: String,
    chunk_reader: Bytes,
    reader_builder: Option<ParquetRecordBatchReaderBuilder<Bytes>>,
    schema: Schema,
    projected_schema: ProjectedSchema,
    row_projector: RowProjector,
    predicate: PredicateRef,
    batch_size: usize,
    reverse: bool,
    meta_data: SstMetaData,
}

impl ProjectAndFilterReader {
    fn filter_row_groups(&self, row_groups: &[RowGroupMetaData]) -> Result<Vec<usize>> {
        let filter = RowGroupFilter::try_new(
            self.schema.as_arrow_schema_ref(),
            row_groups,
            self.meta_data.bloom_filter.filters(),
            self.predicate.exprs(),
        )?;

        Ok(filter.filter())
    }

    /// Generate the reader which has processed projection and filter.
    /// This `file_reader` is consumed after calling this method.
    fn project_and_filter_reader(
        &mut self,
    ) -> Result<Box<dyn Iterator<Item = ArrowResult<RecordBatch>>>> {
        assert!(self.reader_builder.is_some());

        let reader_builder = self.reader_builder.take().unwrap();
        let filtered_row_groups = self.filter_row_groups(reader_builder.metadata().row_groups())?;

        debug!(
            "fetch_record_batch row_groups total:{}, after filter:{}",
            reader_builder.metadata().num_row_groups(),
            filtered_row_groups.len()
        );

        if self.reverse {
            let mut builder = ReverseRecordBatchReaderBuilder::new(
                reader_builder.metadata(),
                self.chunk_reader.clone(),
                self.batch_size,
            )
            .filtered_row_groups(Some(filtered_row_groups));
            if !self.projected_schema.is_all_projection() {
                builder = builder.projection(Some(self.row_projector.existed_source_projection()));
            }

            let reverse_reader = builder
                .build()
                .map_err(|e| Box::new(e) as _)
                .context(DecodeRecordBatch)?;

            Ok(Box::new(reverse_reader))
        } else {
            let builder = reader_builder
                .with_batch_size(self.batch_size)
                .with_row_groups(filtered_row_groups);

            let builder = if self.projected_schema.is_all_projection() {
                builder
            } else {
                let proj_mask = ProjectionMask::leaves(
                    builder.metadata().file_metadata().schema_descr(),
                    self.row_projector
                        .existed_source_projection()
                        .iter()
                        .copied(),
                );
                builder.with_projection(proj_mask)
            };
            let reader = builder
                .build()
                .map_err(|e| Box::new(e) as _)
                .context(DecodeRecordBatch)?;

            Ok(Box::new(reader))
        }
    }

    /// Fetch the record batch from the `reader` and send them.
    /// Returns the fetched row number.
    fn fetch_and_send_record_batch(
        mut self,
        mut send: impl FnMut(Result<RecordBatchWithKey>) -> Result<()>,
    ) -> Result<usize> {
        let reader = self.project_and_filter_reader()?;

        let arrow_record_batch_projector = ArrowRecordBatchProjector::from(self.row_projector);
        let parquet_decoder = ParquetDecoder::new(self.meta_data.storage_format_opts);
        let mut row_num = 0;
        for record_batch in reader {
            trace!(
                "Fetch one record batch from sst:{}, num_rows:{:?}",
                self.file_path,
                record_batch.as_ref().map(|v| v.num_rows())
            );

            match record_batch
                .map_err(|e| Box::new(e) as _)
                .context(DecodeRecordBatch)
            {
                Ok(record_batch) => {
                    let record_batch = parquet_decoder
                        .decode_record_batch(record_batch)
                        .map_err(|e| Box::new(e) as _)
                        .context(DecodeRecordBatch)?;
                    row_num += record_batch.num_rows();
                    let record_batch_with_key = arrow_record_batch_projector
                        .project_to_record_batch_with_key(record_batch)
                        .map_err(|e| Box::new(e) as _)
                        .context(DecodeRecordBatch);
                    send(record_batch_with_key)?;
                }
                Err(e) => {
                    send(Err(e))?;
                    break;
                }
            };
        }

        Ok(row_num)
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

    // TODO(yingwen): Project the schema in parquet
    async fn read(
        &mut self,
    ) -> Result<Vec<Box<dyn Stream<Item = Result<RecordBatchWithKey>> + Send + Unpin>>> {
        debug!(
            "read sst:{}, projected_schema:{:?}, predicate:{:?}",
            self.path.to_string(),
            self.projected_schema,
            self.predicate
        );

        self.init_if_necessary().await?;
        let (tx, rx) = mpsc::channel::<Result<RecordBatchWithKey>>(self.channel_cap);
        self.read_record_batches(tx)?;

        Ok(vec![Box::new(RecordBatchReceiver { rx }) as _])
    }
}
