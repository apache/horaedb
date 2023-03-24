// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

//! Sst writer implementation based on parquet.

use async_trait::async_trait;
use common_types::{record_batch::RecordBatchWithKey, request_id::RequestId};
use common_util::error::BoxError;
use datafusion::parquet::basic::Compression;
use futures::StreamExt;
use log::{debug, error};
use object_store::{ObjectStoreRef, Path};
use snafu::ResultExt;
use tokio::io::AsyncWrite;

use crate::{
    sst::{
        factory::{ObjectStorePickerRef, SstWriteOptions},
        parquet::{
            encoding::ParquetEncoder,
            meta_data::{ParquetFilter, ParquetMetaData, RowGroupFilterBuilder},
        },
        writer::{
            self, BuildParquetFilter, EncodeRecordBatch, MetaData, PollRecordBatch,
            RecordBatchStream, Result, SstInfo, SstWriter, Storage,
        },
    },
    table_options::StorageFormat,
};

/// The implementation of sst based on parquet and object storage.
#[derive(Debug)]
pub struct ParquetSstWriter<'a> {
    /// The path where the data is persisted.
    path: &'a Path,
    hybrid_encoding: bool,
    /// The storage where the data is persist.
    store: &'a ObjectStoreRef,
    /// Max row group size.
    num_rows_per_row_group: usize,
    max_buffer_size: usize,
    compression: Compression,
}

impl<'a> ParquetSstWriter<'a> {
    pub fn new(
        path: &'a Path,
        hybrid_encoding: bool,
        store_picker: &'a ObjectStorePickerRef,
        options: &SstWriteOptions,
    ) -> Self {
        let store = store_picker.default_store();
        Self {
            path,
            hybrid_encoding,
            store,
            num_rows_per_row_group: options.num_rows_per_row_group,
            compression: options.compression.into(),
            max_buffer_size: options.max_buffer_size,
        }
    }
}

/// The writer will reorganize the record batches into row groups, and then
/// encode them to parquet file.
struct RecordBatchGroupWriter {
    request_id: RequestId,
    hybrid_encoding: bool,
    input: RecordBatchStream,
    input_exhausted: bool,
    meta_data: MetaData,
    num_rows_per_row_group: usize,
    max_buffer_size: usize,
    compression: Compression,
    /// The filter for the parquet file, and it will be updated during
    /// generating the parquet file.
    parquet_filter: ParquetFilter,
}

impl RecordBatchGroupWriter {
    /// Fetch an integral row group from the `self.input`.
    ///
    /// Except the last one, every row group is ensured to contains exactly
    /// `self.num_rows_per_row_group`. As for the last one, it will cover all
    /// the left rows.
    async fn fetch_next_row_group(
        &mut self,
        prev_record_batch: &mut Option<RecordBatchWithKey>,
    ) -> Result<Vec<RecordBatchWithKey>> {
        let mut curr_row_group = vec![];
        // Used to record the number of remaining rows to fill `curr_row_group`.
        let mut remaining = self.num_rows_per_row_group;

        // Keep filling `curr_row_group` until `remaining` is zero.
        while remaining > 0 {
            // Use the `prev_record_batch` to fill `curr_row_group` if possible.
            if let Some(v) = prev_record_batch {
                let total_rows = v.num_rows();
                if total_rows <= remaining {
                    // The whole record batch is part of the `curr_row_group`, and let's feed it
                    // into `curr_row_group`.
                    curr_row_group.push(prev_record_batch.take().unwrap());
                    remaining -= total_rows;
                } else {
                    // Only first `remaining` rows of the record batch belongs to `curr_row_group`,
                    // the rest should be put to `prev_record_batch` for next row group.
                    curr_row_group.push(v.slice(0, remaining));
                    *v = v.slice(remaining, total_rows - remaining);
                    remaining = 0;
                }

                continue;
            }

            if self.input_exhausted {
                break;
            }

            // Previous record batch has been exhausted, and let's fetch next record batch.
            match self.input.next().await {
                Some(v) => {
                    let v = v.context(PollRecordBatch)?;
                    debug_assert!(
                        !v.is_empty(),
                        "found empty record batch, request id:{}",
                        self.request_id
                    );

                    // Updated the exhausted `prev_record_batch`, and let next loop to continue to
                    // fill `curr_row_group`.
                    prev_record_batch.replace(v);
                }
                None => {
                    self.input_exhausted = true;
                    break;
                }
            };
        }

        Ok(curr_row_group)
    }

    /// Build the parquet filter for the given `row_group`, and then update it
    /// into `self.parquet_filter`.
    fn update_parquet_filter(&mut self, row_group_batch: &[RecordBatchWithKey]) -> Result<()> {
        // TODO: support filter in hybrid storage format [#435](https://github.com/CeresDB/ceresdb/issues/435)
        if self.hybrid_encoding {
            return Ok(());
        }

        let row_group_filter = {
            let mut builder =
                RowGroupFilterBuilder::with_num_columns(row_group_batch[0].num_columns());

            for partial_batch in row_group_batch {
                for (col_idx, column) in partial_batch.columns().iter().enumerate() {
                    for row in 0..column.num_rows() {
                        let datum = column.datum(row);
                        let bytes = datum.to_bytes();
                        builder.add_key(col_idx, &bytes);
                    }
                }
            }

            builder.build().box_err().context(BuildParquetFilter)?
        };

        self.parquet_filter.push_row_group_filter(row_group_filter);
        Ok(())
    }

    async fn write_all<W: AsyncWrite + Send + Unpin + 'static>(mut self, sink: W) -> Result<usize> {
        let mut prev_record_batch: Option<RecordBatchWithKey> = None;
        let mut arrow_row_group = Vec::new();
        let mut total_num_rows = 0;

        let mut parquet_encoder = ParquetEncoder::try_new(
            sink,
            &self.meta_data.schema,
            self.hybrid_encoding,
            self.num_rows_per_row_group,
            self.max_buffer_size,
            self.compression,
        )
        .box_err()
        .context(EncodeRecordBatch)?;

        loop {
            let row_group = self.fetch_next_row_group(&mut prev_record_batch).await?;
            if row_group.is_empty() {
                break;
            }

            self.update_parquet_filter(&row_group)?;

            let num_batches = row_group.len();
            for record_batch in row_group {
                arrow_row_group.push(record_batch.into_record_batch().into_arrow_record_batch());
            }
            let num_rows = parquet_encoder
                .encode_record_batches(arrow_row_group)
                .await
                .box_err()
                .context(EncodeRecordBatch)?;

            // TODO: it will be better to use `arrow_row_group.clear()` to reuse the
            // allocated memory.
            arrow_row_group = Vec::with_capacity(num_batches);
            total_num_rows += num_rows;
        }

        let parquet_meta_data = {
            let mut parquet_meta_data = ParquetMetaData::from(self.meta_data);
            parquet_meta_data.parquet_filter = Some(self.parquet_filter);
            parquet_meta_data
        };
        parquet_encoder
            .set_meta_data(parquet_meta_data)
            .box_err()
            .context(EncodeRecordBatch)?;

        parquet_encoder
            .close()
            .await
            .box_err()
            .context(EncodeRecordBatch)?;

        Ok(total_num_rows)
    }
}

struct ObjectStoreMultiUploadAborter<'a> {
    location: &'a Path,
    session_id: String,
    object_store: &'a ObjectStoreRef,
}

impl<'a> ObjectStoreMultiUploadAborter<'a> {
    async fn initialize_upload(
        object_store: &'a ObjectStoreRef,
        location: &'a Path,
    ) -> Result<(
        ObjectStoreMultiUploadAborter<'a>,
        Box<dyn AsyncWrite + Unpin + Send>,
    )> {
        let (session_id, upload_writer) = object_store
            .put_multipart(location)
            .await
            .context(Storage)?;
        let aborter = Self {
            location,
            session_id,
            object_store,
        };
        Ok((aborter, upload_writer))
    }

    async fn abort(self) -> Result<()> {
        self.object_store
            .abort_multipart(self.location, &self.session_id)
            .await
            .context(Storage)
    }
}

#[async_trait]
impl<'a> SstWriter for ParquetSstWriter<'a> {
    async fn write(
        &mut self,
        request_id: RequestId,
        meta: &MetaData,
        input: RecordBatchStream,
    ) -> writer::Result<SstInfo> {
        debug!(
            "Build parquet file, request_id:{}, meta:{:?}, num_rows_per_row_group:{}",
            request_id, meta, self.num_rows_per_row_group
        );

        let group_writer = RecordBatchGroupWriter {
            hybrid_encoding: self.hybrid_encoding,
            request_id,
            input,
            input_exhausted: false,
            num_rows_per_row_group: self.num_rows_per_row_group,
            max_buffer_size: self.max_buffer_size,
            compression: self.compression,
            meta_data: meta.clone(),
            parquet_filter: ParquetFilter::default(),
        };

        let (aborter, sink) =
            ObjectStoreMultiUploadAborter::initialize_upload(self.store, self.path).await?;
        let total_num_rows = match group_writer.write_all(sink).await {
            Ok(v) => v,
            Err(e) => {
                if let Err(e) = aborter.abort().await {
                    // The uploading file will be leaked if failed to abort. A repair command will
                    // be provided to clean up the leaked files.
                    error!(
                        "Failed to abort multi-upload for sst:{}, err:{}",
                        self.path, e
                    );
                }
                return Err(e);
            }
        };

        let file_head = self.store.head(self.path).await.context(Storage)?;
        let storage_format = if self.hybrid_encoding {
            StorageFormat::Hybrid
        } else {
            StorageFormat::Columnar
        };
        Ok(SstInfo {
            file_size: file_head.size,
            row_num: total_num_rows,
            storage_format,
        })
    }
}

#[cfg(test)]
mod tests {

    use std::{sync::Arc, task::Poll};

    use common_types::{
        bytes::Bytes,
        projected_schema::ProjectedSchema,
        tests::{build_row, build_schema},
        time::{TimeRange, Timestamp},
    };
    use common_util::{
        runtime::{self, Runtime},
        tests::init_log_for_test,
    };
    use futures::stream;
    use object_store::LocalFileSystem;
    use table_engine::predicate::Predicate;
    use tempfile::tempdir;

    use super::*;
    use crate::{
        row_iter::tests::build_record_batch_with_key,
        sst::{
            factory::{
                Factory, FactoryImpl, ReadFrequency, ScanOptions, SstReadOptions, SstWriteOptions,
            },
            parquet::AsyncParquetReader,
            reader::{tests::check_stream, SstReader},
        },
        table_options::{self, StorageFormatHint},
    };

    // TODO(xikai): add test for reverse reader

    #[test]
    fn test_parquet_build_and_read() {
        init_log_for_test();

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
        runtime.block_on(async {
            let sst_factory = FactoryImpl;
            let sst_write_options = SstWriteOptions {
                storage_format_hint: StorageFormatHint::Auto,
                num_rows_per_row_group,
                compression: table_options::Compression::Uncompressed,
                max_buffer_size: 0,
            };

            let dir = tempdir().unwrap();
            let root = dir.path();
            let store: ObjectStoreRef = Arc::new(LocalFileSystem::new_with_prefix(root).unwrap());
            let store_picker: ObjectStorePickerRef = Arc::new(store);
            let sst_file_path = Path::from("data.par");

            let schema = build_schema();
            let projected_schema = ProjectedSchema::no_projection(schema.clone());
            let sst_meta = MetaData {
                min_key: Bytes::from_static(b"100"),
                max_key: Bytes::from_static(b"200"),
                time_range: TimeRange::new_unchecked(Timestamp::new(1), Timestamp::new(2)),
                max_sequence: 200,
                schema: schema.clone(),
            };

            let mut counter = 5;
            let record_batch_stream = Box::new(stream::poll_fn(move |_| -> Poll<Option<_>> {
                if counter == 0 {
                    return Poll::Ready(None);
                }
                counter -= 1;

                // reach here when counter is 9 7 5 3 1
                let ts = 100 + counter;
                let rows = vec![
                    build_row(b"a", ts, 10.0, "v4", 1000, 1_000_000),
                    build_row(b"b", ts, 10.0, "v4", 1000, 1_000_000),
                    build_row(b"c", ts, 10.0, "v4", 1000, 1_000_000),
                ];
                let batch = build_record_batch_with_key(schema.clone(), rows);
                Poll::Ready(Some(Ok(batch)))
            }));

            let mut writer = sst_factory
                .create_writer(&sst_write_options, &sst_file_path, &store_picker)
                .await
                .unwrap();
            let sst_info = writer
                .write(RequestId::next_id(), &sst_meta, record_batch_stream)
                .await
                .unwrap();

            assert_eq!(15, sst_info.row_num);

            let scan_options = ScanOptions::default();
            // read sst back to test
            let sst_read_options = SstReadOptions {
                reverse: false,
                frequency: ReadFrequency::Frequent,
                num_rows_per_row_group: 5,
                projected_schema,
                predicate: Arc::new(Predicate::empty()),
                meta_cache: None,
                scan_options,
                runtime: runtime.clone(),
            };

            let mut reader: Box<dyn SstReader + Send> = {
                let mut reader = AsyncParquetReader::new(
                    &sst_file_path,
                    &sst_read_options,
                    None,
                    &store_picker,
                    None,
                );
                let mut sst_meta_readback = reader
                    .meta_data()
                    .await
                    .unwrap()
                    .as_parquet()
                    .unwrap()
                    .as_ref()
                    .clone();
                // sst filter is built insider sst writer, so overwrite to default for
                // comparison.
                sst_meta_readback.parquet_filter = Default::default();
                assert_eq!(&sst_meta_readback, &ParquetMetaData::from(sst_meta));
                assert_eq!(
                    expected_num_rows,
                    reader
                        .row_groups()
                        .await
                        .iter()
                        .map(|g| g.num_rows())
                        .collect::<Vec<_>>()
                );

                Box::new(reader)
            };

            let mut stream = reader.read().await.unwrap();
            let mut expect_rows = vec![];
            for counter in &[4, 3, 2, 1, 0] {
                expect_rows.push(build_row(b"a", 100 + counter, 10.0, "v4", 1000, 1_000_000));
                expect_rows.push(build_row(b"b", 100 + counter, 10.0, "v4", 1000, 1_000_000));
                expect_rows.push(build_row(b"c", 100 + counter, 10.0, "v4", 1000, 1_000_000));
            }
            check_stream(&mut stream, expect_rows).await;
        });
    }

    #[tokio::test]
    async fn test_fetch_row_group() {
        // rows per group: 10
        let testcases = vec![
            // input, expected
            (10, vec![], vec![]),
            (10, vec![10, 10], vec![10, 10]),
            (10, vec![10, 10, 1], vec![10, 10, 1]),
            (10, vec![10, 10, 21], vec![10, 10, 10, 10, 1]),
            (10, vec![5, 6, 10], vec![10, 10, 1]),
            (10, vec![5, 4, 4, 30], vec![10, 10, 10, 10, 3]),
            (10, vec![20, 7, 23, 20], vec![10, 10, 10, 10, 10, 10, 10]),
            (10, vec![21], vec![10, 10, 1]),
            (10, vec![2, 2, 2, 2, 2], vec![10]),
            (4, vec![3, 3, 3, 3, 3], vec![4, 4, 4, 3]),
            (5, vec![3, 3, 3, 3, 3], vec![5, 5, 5]),
        ];

        for (num_rows_per_group, input, expected) in testcases {
            check_num_rows_of_row_group(num_rows_per_group, input, expected).await;
        }
    }

    async fn check_num_rows_of_row_group(
        num_rows_per_row_group: usize,
        input_num_rows: Vec<usize>,
        expected_num_rows: Vec<usize>,
    ) {
        init_log_for_test();
        let schema = build_schema();
        let mut poll_cnt = 0;
        let schema_clone = schema.clone();
        let record_batch_stream = Box::new(stream::poll_fn(move |_ctx| -> Poll<Option<_>> {
            if poll_cnt == input_num_rows.len() {
                return Poll::Ready(None);
            }

            let rows = (0..input_num_rows[poll_cnt])
                .map(|_| build_row(b"a", 100, 10.0, "v4", 1000, 1_000_000))
                .collect::<Vec<_>>();

            let batch = build_record_batch_with_key(schema_clone.clone(), rows);
            poll_cnt += 1;

            Poll::Ready(Some(Ok(batch)))
        }));

        let mut group_writer = RecordBatchGroupWriter {
            request_id: RequestId::next_id(),
            hybrid_encoding: false,
            input: record_batch_stream,
            input_exhausted: false,
            num_rows_per_row_group,
            compression: Compression::UNCOMPRESSED,
            meta_data: MetaData {
                min_key: Default::default(),
                max_key: Default::default(),
                time_range: Default::default(),
                max_sequence: 1,
                schema,
            },
            max_buffer_size: 0,
            parquet_filter: ParquetFilter::default(),
        };

        let mut prev_record_batch = None;
        for expect_num_row in expected_num_rows {
            let batch = group_writer
                .fetch_next_row_group(&mut prev_record_batch)
                .await
                .unwrap();

            let actual_num_row: usize = batch.iter().map(|b| b.num_rows()).sum();
            assert_eq!(expect_num_row, actual_num_row);
        }
    }
}
