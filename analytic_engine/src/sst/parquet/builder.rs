// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

//! Sst builder implementation based on parquet.

use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};

use async_trait::async_trait;
use common_types::request_id::RequestId;
use datafusion::parquet::basic::Compression;
use futures::StreamExt;
use log::debug;
use object_store::{ObjectStoreRef, Path};
use snafu::ResultExt;

use crate::sst::{
    builder::{RecordBatchStream, SstBuilder, *},
    factory::SstBuilderOptions,
    file::SstMetaData,
    parquet::encoding::ParquetEncoder,
};

/// The implementation of sst based on parquet and object storage.
#[derive(Debug)]
pub struct ParquetSstBuilder<'a> {
    /// The path where the data is persisted.
    path: &'a Path,
    /// The storage where the data is persist.
    storage: &'a ObjectStoreRef,
    /// Max row group size.
    num_rows_per_row_group: usize,
    compression: Compression,
}

impl<'a> ParquetSstBuilder<'a> {
    pub fn new(path: &'a Path, storage: &'a ObjectStoreRef, options: &SstBuilderOptions) -> Self {
        Self {
            path,
            storage,
            num_rows_per_row_group: options.num_rows_per_row_group,
            compression: options.compression.into(),
        }
    }
}

/// RecordBytesReader provides AsyncRead implementation for the encoded records
/// by parquet.
struct RecordBytesReader {
    request_id: RequestId,
    record_stream: RecordBatchStream,
    num_rows_per_row_group: usize,
    compression: Compression,
    meta_data: SstMetaData,
    total_row_num: Arc<AtomicUsize>,

    fetched_row_num: usize,
}

impl RecordBytesReader {
    async fn read_all(mut self) -> Result<Vec<u8>> {
        let mut arrow_record_batch_vec = Vec::new();

        let mut parquet_encoder = ParquetEncoder::try_new(
            self.num_rows_per_row_group,
            self.compression,
            self.meta_data,
        )
        .map_err(|e| Box::new(e) as _)
        .context(EncodeRecordBatch)?;

        // process record batch stream
        while let Some(record_batch) = self.record_stream.next().await {
            let record_batch = record_batch.context(PollRecordBatch)?;

            debug_assert!(
                !record_batch.is_empty(),
                "found empty record batch, request id:{}",
                self.request_id
            );

            self.fetched_row_num += record_batch.num_rows();
            arrow_record_batch_vec.push(record_batch.into_record_batch().into_arrow_record_batch());

            if self.fetched_row_num >= self.num_rows_per_row_group {
                let buf_len = arrow_record_batch_vec.len();
                self.fetched_row_num = 0;
                let row_num = parquet_encoder
                    .encode_record_batch(arrow_record_batch_vec)
                    .map_err(|e| Box::new(e) as _)
                    .context(EncodeRecordBatch)?;
                arrow_record_batch_vec = Vec::with_capacity(buf_len);
                self.total_row_num.fetch_add(row_num, Ordering::Relaxed);
            }
        }

        // final check if there is any record batch left
        if self.fetched_row_num != 0 {
            let row_num = parquet_encoder
                .encode_record_batch(arrow_record_batch_vec)
                .map_err(|e| Box::new(e) as _)
                .context(EncodeRecordBatch)?;
            self.total_row_num.fetch_add(row_num, Ordering::Relaxed);
        }

        let bytes = parquet_encoder
            .close()
            .map_err(|e| Box::new(e) as _)
            .context(EncodeRecordBatch)?;
        Ok(bytes)
    }
}

#[async_trait]
impl<'a> SstBuilder for ParquetSstBuilder<'a> {
    async fn build(
        &mut self,
        request_id: RequestId,
        meta: &SstMetaData,
        record_stream: RecordBatchStream,
    ) -> Result<SstInfo> {
        debug!(
            "Build parquet file, request_id:{}, meta:{:?}, num_rows_per_row_group:{}",
            request_id, meta, self.num_rows_per_row_group
        );

        let total_row_num = Arc::new(AtomicUsize::new(0));
        let reader = RecordBytesReader {
            request_id,
            record_stream,
            num_rows_per_row_group: self.num_rows_per_row_group,
            compression: self.compression,
            total_row_num: total_row_num.clone(),
            // TODO(xikai): should we avoid this clone?
            meta_data: meta.to_owned(),
            fetched_row_num: 0,
        };
        let bytes = reader.read_all().await?;
        self.storage
            .put(self.path, bytes.into())
            .await
            .context(Storage)?;

        let file_head = self.storage.head(self.path).await.context(Storage)?;

        Ok(SstInfo {
            file_size: file_head.size,
            row_num: total_row_num.load(Ordering::Relaxed),
        })
    }
}

#[cfg(test)]
mod tests {

    use std::task::Poll;

    use common_types::{
        bytes::Bytes,
        projected_schema::ProjectedSchema,
        tests::{build_row, build_schema},
        time::{TimeRange, Timestamp},
    };
    use common_util::runtime::{self, Runtime};
    use futures::stream;
    use object_store::LocalFileSystem;
    use table_engine::predicate::Predicate;
    use tempfile::tempdir;

    use super::*;
    use crate::{
        row_iter::tests::build_record_batch_with_key,
        sst::{
            factory::{Factory, FactoryImpl, SstBuilderOptions, SstReaderOptions, SstType},
            parquet::{reader::ParquetSstReader, AsyncParquetReader},
            reader::{tests::check_stream, SstReader},
        },
        table_options::{self, StorageFormatOptions},
    };

    // TODO(xikai): add test for reverse reader

    #[test]
    fn test_parquet_build_and_read() {
        let runtime = Arc::new(runtime::Builder::default().build().unwrap());
        parquet_write_and_then_read_back(runtime.clone(), 3, vec![3, 3, 3, 3, 3]);
        parquet_write_and_then_read_back(runtime.clone(), 4, vec![4, 4, 4, 3]);
        parquet_write_and_then_read_back(runtime, 5, vec![5, 5, 5]);
    }

    fn parquet_write_and_then_read_back(
        runtime: Arc<Runtime>,
        num_rows_per_row_group: usize,
        expected_num_rows: Vec<i64>,
    ) {
        parquet_write_and_then_read_back_inner(
            runtime.clone(),
            num_rows_per_row_group,
            expected_num_rows.clone(),
            false,
        );
        parquet_write_and_then_read_back_inner(
            runtime,
            num_rows_per_row_group,
            expected_num_rows,
            true,
        );
    }

    fn parquet_write_and_then_read_back_inner(
        runtime: Arc<Runtime>,
        num_rows_per_row_group: usize,
        expected_num_rows: Vec<i64>,
        async_reader: bool,
    ) {
        runtime.block_on(async {
            let sst_factory = FactoryImpl;
            let sst_builder_options = SstBuilderOptions {
                sst_type: SstType::Parquet,
                num_rows_per_row_group,
                compression: table_options::Compression::Uncompressed,
            };

            let dir = tempdir().unwrap();
            let root = dir.path();
            let store = Arc::new(LocalFileSystem::new_with_prefix(root).unwrap()) as _;
            let sst_file_path = Path::from("data.par");

            let schema = build_schema();
            let projected_schema = ProjectedSchema::no_projection(schema.clone());
            let sst_meta = SstMetaData {
                min_key: Bytes::from_static(b"100"),
                max_key: Bytes::from_static(b"200"),
                time_range: TimeRange::new_unchecked(Timestamp::new(1), Timestamp::new(2)),
                max_sequence: 200,
                schema: schema.clone(),
                size: 10,
                row_num: 2,
                storage_format_opts: StorageFormatOptions::default(),
            };

            let mut counter = 10;
            let record_batch_stream = Box::new(stream::poll_fn(move |ctx| -> Poll<Option<_>> {
                counter -= 1;
                if counter == 0 {
                    return Poll::Ready(None);
                } else if counter % 2 == 0 {
                    ctx.waker().wake_by_ref();
                    return Poll::Pending;
                }

                // reach here when counter is 9 7 5 3 1
                let ts = 100 + counter;
                let rows = vec![
                    build_row(b"a", ts, 10.0, "v4"),
                    build_row(b"b", ts, 10.0, "v4"),
                    build_row(b"c", ts, 10.0, "v4"),
                ];
                let batch = build_record_batch_with_key(schema.clone(), rows);
                Poll::Ready(Some(Ok(batch)))
            }));

            let mut builder = sst_factory
                .new_sst_builder(&sst_builder_options, &sst_file_path, &store)
                .unwrap();
            let sst_info = builder
                .build(RequestId::next_id(), &sst_meta, record_batch_stream)
                .await
                .unwrap();

            assert_eq!(15, sst_info.row_num);

            // read sst back to test
            let sst_reader_options = SstReaderOptions {
                sst_type: SstType::Parquet,
                read_batch_row_num: 5,
                reverse: false,
                projected_schema,
                predicate: Arc::new(Predicate::empty()),
                meta_cache: None,
                data_cache: None,
                runtime: runtime.clone(),
            };

            let mut reader: Box<dyn SstReader + Send> = if async_reader {
                let mut reader =
                    AsyncParquetReader::new(&sst_file_path, &store, &sst_reader_options);
                let sst_meta_readback = {
                    // FIXME: size of SstMetaData is not what this file's size, so overwrite it
                    // https://github.com/CeresDB/ceresdb/issues/321
                    let mut meta = reader.meta_data().await.unwrap().clone();
                    meta.size = sst_meta.size;
                    meta
                };
                assert_eq!(&sst_meta_readback, &sst_meta);
                Box::new(reader)
            } else {
                let mut reader = ParquetSstReader::new(&sst_file_path, &store, &sst_reader_options);
                assert_eq!(reader.meta_data().await.unwrap(), &sst_meta);
                Box::new(reader)
            };
            assert_eq!(
                expected_num_rows,
                reader
                    .row_groups()
                    .await
                    .iter()
                    .map(|g| g.num_rows())
                    .collect::<Vec<_>>()
            );

            let mut stream = reader.read().await.unwrap();
            let mut expect_rows = vec![];
            for counter in &[9, 7, 5, 3, 1] {
                expect_rows.push(build_row(b"a", 100 + counter, 10.0, "v4"));
                expect_rows.push(build_row(b"b", 100 + counter, 10.0, "v4"));
                expect_rows.push(build_row(b"c", 100 + counter, 10.0, "v4"));
            }
            check_stream(&mut stream, expect_rows).await;
        });
    }
}
