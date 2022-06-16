// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

//! Sst reader implementation based on parquet.

use std::{
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
    time::Instant,
};

use arrow_deps::{
    arrow::{error::Result as ArrowResult, record_batch::RecordBatch},
    parquet::{
        arrow::{ArrowReader, ParquetFileArrowReader, ProjectionMask},
        file::{
            metadata::RowGroupMetaData, reader::FileReader, serialized_reader::SliceableCursor,
        },
    },
};
use async_trait::async_trait;
use common_types::{
    projected_schema::{ProjectedSchema, RowProjector},
    record_batch::{ArrowRecordBatchProjector, RecordBatchWithKey},
    schema::Schema,
};
use common_util::runtime::Runtime;
use futures::Stream;
use log::{debug, error, trace};
use object_store::{ObjectStore, Path};
use parquet::{
    reverse_reader::Builder as ReverseRecordBatchReaderBuilder, CachableSerializedFileReader,
    DataCacheRef, MetaCacheRef,
};
use snafu::{ensure, OptionExt, ResultExt};
use table_engine::predicate::PredicateRef;
use tokio::sync::mpsc::{self, Receiver, Sender};

use crate::sst::{
    factory::SstReaderOptions,
    file::SstMetaData,
    parquet::encoding,
    reader::{error::*, SstReader},
};

const DEFAULT_CHANNEL_CAP: usize = 1000;

pub async fn read_sst_meta<S: ObjectStore>(
    storage: &S,
    path: &Path,
    meta_cache: &Option<MetaCacheRef>,
    data_cache: &Option<DataCacheRef>,
) -> Result<(CachableSerializedFileReader<SliceableCursor>, SstMetaData)> {
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
    let bytes = SliceableCursor::new(Arc::new(
        get_result
            .bytes()
            .await
            .map_err(|e| Box::new(e) as _)
            .context(ReadPersist {
                path: path.to_string(),
            })?
            .to_vec(),
    ));

    // generate the file reader
    let file_reader = CachableSerializedFileReader::new(
        path.to_string(),
        bytes,
        meta_cache.clone(),
        data_cache.clone(),
    )
    .map_err(|e| Box::new(e) as _)
    .with_context(|| ReadPersist {
        path: path.to_string(),
    })?;

    // parse sst meta data
    let sst_meta = {
        let kv_metas = file_reader
            .metadata()
            .file_metadata()
            .key_value_metadata()
            .context(SstMetaNotFound)?;

        ensure!(!kv_metas.is_empty(), EmptySstMeta);

        encoding::decode_sst_meta_data(&kv_metas[0])
            .map_err(|e| Box::new(e) as _)
            .context(DecodeSstMeta)?
    };

    Ok((file_reader, sst_meta))
}

/// The implementation of sst based on parquet and object storage.
pub struct ParquetSstReader<'a, S: ObjectStore> {
    /// The path where the data is persisted.
    path: &'a Path,
    /// The storage where the data is persist.
    storage: &'a S,
    projected_schema: ProjectedSchema,
    predicate: PredicateRef,
    meta_data: Option<SstMetaData>,
    file_reader: Option<CachableSerializedFileReader<SliceableCursor>>,
    /// The batch of rows in one `record_batch`.
    batch_size: usize,
    /// Read the rows in reverse order.
    reverse: bool,
    channel_cap: usize,

    meta_cache: Option<MetaCacheRef>,
    data_cache: Option<DataCacheRef>,

    runtime: Arc<Runtime>,
}

impl<'a, S: ObjectStore> ParquetSstReader<'a, S> {
    pub fn new(path: &'a Path, storage: &'a S, options: &SstReaderOptions) -> Self {
        Self {
            path,
            storage,
            projected_schema: options.projected_schema.clone(),
            predicate: options.predicate.clone(),
            meta_data: None,
            file_reader: None,
            batch_size: options.read_batch_row_num,
            reverse: options.reverse,
            channel_cap: DEFAULT_CHANNEL_CAP,
            meta_cache: options.meta_cache.clone(),
            data_cache: options.data_cache.clone(),
            runtime: options.runtime.clone(),
        }
    }
}

impl<'a, S: ObjectStore> ParquetSstReader<'a, S> {
    async fn init_if_necessary(&mut self) -> Result<()> {
        if self.meta_data.is_some() {
            return Ok(());
        }

        let (file_reader, sst_meta) =
            read_sst_meta(self.storage, self.path, &self.meta_cache, &self.data_cache).await?;

        self.file_reader = Some(file_reader);
        self.meta_data = Some(sst_meta);

        Ok(())
    }

    fn read_record_batches(&mut self, tx: Sender<Result<RecordBatchWithKey>>) -> Result<()> {
        let path = self.path.to_string();
        ensure!(self.file_reader.is_some(), ReadAgain { path });

        let file_reader = self.file_reader.take().unwrap();
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
                file_reader: Some(file_reader),
                schema,
                projected_schema,
                row_projector,
                predicate,
                batch_size,
                reverse,
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
    pub(crate) async fn row_groups(&mut self) -> &[RowGroupMetaData] {
        self.init_if_necessary().await.unwrap();
        self.file_reader.as_ref().unwrap().metadata().row_groups()
    }
}

/// A reader for projection and filter on the parquet file.
struct ProjectAndFilterReader {
    file_path: String,
    file_reader: Option<CachableSerializedFileReader<SliceableCursor>>,
    schema: Schema,
    projected_schema: ProjectedSchema,
    row_projector: RowProjector,
    predicate: PredicateRef,
    batch_size: usize,
    reverse: bool,
}

impl ProjectAndFilterReader {
    fn build_row_group_predicate(&self) -> Box<dyn Fn(&RowGroupMetaData, usize) -> bool + 'static> {
        assert!(self.file_reader.is_some());

        let row_groups = self.file_reader.as_ref().unwrap().metadata().row_groups();
        let filter_results = self.predicate.filter_row_groups(&self.schema, row_groups);

        trace!("Finish build row group predicate, predicate:{:?}, schema:{:?}, filter_results:{:?}, row_groups meta data:{:?}", self.predicate, self.schema, filter_results, row_groups);

        Box::new(move |_, idx: usize| filter_results[idx])
    }

    /// Generate the reader which has processed projection and filter.
    /// This `file_reader` is consumed after calling this method.
    fn project_and_filter_reader(
        &mut self,
    ) -> Result<Box<dyn Iterator<Item = ArrowResult<RecordBatch>>>> {
        assert!(self.file_reader.is_some());

        let row_group_predicate = self.build_row_group_predicate();
        let mut file_reader = self.file_reader.take().unwrap();
        file_reader.filter_row_groups(&row_group_predicate);

        if self.reverse {
            let mut builder =
                ReverseRecordBatchReaderBuilder::new(Arc::new(file_reader), self.batch_size);
            if !self.projected_schema.is_all_projection() {
                builder = builder.projection(Some(self.row_projector.existed_source_projection()));
            }

            let reverse_reader = builder
                .build()
                .map_err(|e| Box::new(e) as _)
                .context(DecodeRecordBatch)?;

            Ok(Box::new(reverse_reader))
        } else {
            let mut arrow_reader = ParquetFileArrowReader::new(Arc::new(file_reader));

            let reader = if self.projected_schema.is_all_projection() {
                arrow_reader.get_record_reader(self.batch_size)
            } else {
                let proj_mask = ProjectionMask::leaves(
                    arrow_reader.get_metadata().file_metadata().schema_descr(),
                    self.row_projector
                        .existed_source_projection()
                        .iter()
                        .copied(),
                );
                arrow_reader.get_record_reader_by_columns(proj_mask, self.batch_size)
            };
            let reader = reader
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
impl<'a, S: ObjectStore> SstReader for ParquetSstReader<'a, S> {
    async fn meta_data(&mut self) -> Result<&SstMetaData> {
        self.init_if_necessary().await?;
        Ok(self.meta_data.as_ref().unwrap())
    }

    // TODO(yingwen): Project the schema in parquet
    async fn read(
        &mut self,
    ) -> Result<Box<dyn Stream<Item = Result<RecordBatchWithKey>> + Send + Unpin>> {
        debug!(
            "read sst:{}, projected_schema:{:?}, predicate:{:?}",
            self.path.to_string(),
            self.projected_schema,
            self.predicate
        );

        self.init_if_necessary().await?;
        let (tx, rx) = mpsc::channel::<Result<RecordBatchWithKey>>(self.channel_cap);
        self.read_record_batches(tx)?;

        Ok(Box::new(RecordBatchReceiver { rx }))
    }
}
