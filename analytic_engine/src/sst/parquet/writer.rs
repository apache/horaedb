// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

//! Sst writer implementation based on parquet.

use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};

use async_trait::async_trait;
use common_types::{record_batch::RecordBatchWithKey, request_id::RequestId};
use common_util::error::BoxError;
use datafusion::parquet::basic::Compression;
use futures::StreamExt;
use log::debug;
use object_store::{ObjectStoreRef, Path};
use snafu::ResultExt;

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
        }
    }
}

/// RecordBytesReader provides AsyncRead implementation for the encoded records
/// by parquet.
struct RecordBytesReader {
    request_id: RequestId,
    hybrid_encoding: bool,
    meta_data: MetaData,
    record_stream: RecordBatchStream,
    num_rows_per_row_group: usize,
    compression: Compression,
    total_row_num: Arc<AtomicUsize>,
    // Record batch partitioned by exactly given `num_rows_per_row_group`
    // There may be more than one `RecordBatchWithKey` inside each partition
    partitioned_record_batch: Vec<Vec<RecordBatchWithKey>>,
}

impl RecordBytesReader {
    // Partition record batch stream into batch vector with exactly given
    // `num_rows_per_row_group`
    async fn partition_record_batch(&mut self) -> Result<()> {
        let mut prev_record_batch: Option<RecordBatchWithKey> = None;

        loop {
            let row_group = self.fetch_next_row_group(&mut prev_record_batch).await?;
            if row_group.is_empty() {
                break;
            }
            self.partitioned_record_batch.push(row_group);
        }

        Ok(())
    }

    /// Fetch an integral row group from the `self.record_stream`.
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

            // Previous record batch has been exhausted, and let's fetch next record batch.
            match self.record_stream.next().await {
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
                None => break,
            };
        }

        Ok(curr_row_group)
    }

    fn build_parquet_filter(&self) -> Result<ParquetFilter> {
        // TODO: support filter in hybrid storage format [#435](https://github.com/CeresDB/ceresdb/issues/435)
        if self.hybrid_encoding {
            return Ok(ParquetFilter::default());
        }
        let filters = self
            .partitioned_record_batch
            .iter()
            .map(|row_group_batch| {
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

                builder.build().box_err().context(BuildParquetFilter)
            })
            .collect::<Result<Vec<_>>>()?;

        Ok(ParquetFilter::new(filters))
    }

    async fn read_all(mut self) -> Result<Vec<u8>> {
        self.partition_record_batch().await?;

        let parquet_meta_data = {
            let sst_filter = self.build_parquet_filter()?;
            let mut parquet_meta_data = ParquetMetaData::from(self.meta_data);
            parquet_meta_data.parquet_filter = Some(sst_filter);
            parquet_meta_data
        };

        let mut parquet_encoder = ParquetEncoder::try_new(
            self.hybrid_encoding,
            self.num_rows_per_row_group,
            self.compression,
            parquet_meta_data,
        )
        .box_err()
        .context(EncodeRecordBatch)?;

        // process record batch stream
        let mut arrow_record_batch_vec = Vec::new();
        for record_batches in self.partitioned_record_batch {
            for batch in record_batches {
                arrow_record_batch_vec.push(batch.into_record_batch().into_arrow_record_batch());
            }

            let buf_len = arrow_record_batch_vec.len();
            let row_num = parquet_encoder
                .encode_record_batch(arrow_record_batch_vec)
                .box_err()
                .context(EncodeRecordBatch)?;
            self.total_row_num.fetch_add(row_num, Ordering::Relaxed);
            arrow_record_batch_vec = Vec::with_capacity(buf_len);
        }

        let bytes = parquet_encoder
            .close()
            .box_err()
            .context(EncodeRecordBatch)?;
        Ok(bytes)
    }
}

#[async_trait]
impl<'a> SstWriter for ParquetSstWriter<'a> {
    async fn write(
        &mut self,
        request_id: RequestId,
        meta: &MetaData,
        record_stream: RecordBatchStream,
    ) -> writer::Result<SstInfo> {
        debug!(
            "Build parquet file, request_id:{}, meta:{:?}, num_rows_per_row_group:{}",
            request_id, meta, self.num_rows_per_row_group
        );

        let total_row_num = Arc::new(AtomicUsize::new(0));
        let reader = RecordBytesReader {
            hybrid_encoding: self.hybrid_encoding,
            request_id,
            record_stream,
            num_rows_per_row_group: self.num_rows_per_row_group,
            compression: self.compression,
            total_row_num: total_row_num.clone(),
            meta_data: meta.clone(),
            partitioned_record_batch: Default::default(),
        };
        let bytes = reader.read_all().await?;
        self.store
            .put(self.path, bytes.into())
            .await
            .context(Storage)?;

        let file_head = self.store.head(self.path).await.context(Storage)?;

        let storage_format = if self.hybrid_encoding {
            StorageFormat::Hybrid
        } else {
            StorageFormat::Columnar
        };
        Ok(SstInfo {
            file_size: file_head.size,
            row_num: total_row_num.load(Ordering::Relaxed),
            storage_format,
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
            factory::{Factory, FactoryImpl, ReadFrequency, SstReadOptions, SstWriteOptions},
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

            // read sst back to test
            let sst_read_options = SstReadOptions {
                read_batch_row_num: 5,
                reverse: false,
                frequency: ReadFrequency::Frequent,
                projected_schema,
                predicate: Arc::new(Predicate::empty()),
                meta_cache: None,
                runtime: runtime.clone(),
                num_rows_per_row_group: 5,
                background_read_parallelism: 1,
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
    async fn test_partition_record_batch() {
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
            test_partition_record_batch_inner(num_rows_per_group, input, expected).await;
        }
    }

    async fn test_partition_record_batch_inner(
        num_rows_per_row_group: usize,
        input_row_nums: Vec<usize>,
        expected_row_nums: Vec<usize>,
    ) {
        init_log_for_test();
        let schema = build_schema();
        let mut poll_cnt = 0;
        let schema_clone = schema.clone();
        let record_batch_stream = Box::new(stream::poll_fn(move |_ctx| -> Poll<Option<_>> {
            if poll_cnt == input_row_nums.len() {
                return Poll::Ready(None);
            }

            let rows = (0..input_row_nums[poll_cnt])
                .map(|_| build_row(b"a", 100, 10.0, "v4", 1000, 1_000_000))
                .collect::<Vec<_>>();

            let batch = build_record_batch_with_key(schema_clone.clone(), rows);
            poll_cnt += 1;

            Poll::Ready(Some(Ok(batch)))
        }));

        let mut reader = RecordBytesReader {
            request_id: RequestId::next_id(),
            hybrid_encoding: false,
            record_stream: record_batch_stream,
            num_rows_per_row_group,
            compression: Compression::UNCOMPRESSED,
            meta_data: MetaData {
                min_key: Default::default(),
                max_key: Default::default(),
                time_range: Default::default(),
                max_sequence: 1,
                schema,
            },
            total_row_num: Arc::new(AtomicUsize::new(0)),
            partitioned_record_batch: Vec::new(),
        };

        reader.partition_record_batch().await.unwrap();

        for (i, expected_row_num) in expected_row_nums.into_iter().enumerate() {
            let actual: usize = reader.partitioned_record_batch[i]
                .iter()
                .map(|b| b.num_rows())
                .sum();
            assert_eq!(expected_row_num, actual);
        }
    }
}
