// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

//! Sst builder implementation based on parquet.

use std::{
    io::SeekFrom,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Mutex,
    },
};

use arrow_deps::{
    arrow::record_batch::RecordBatch as ArrowRecordBatch,
    datafusion::parquet::basic::Compression,
    parquet::{arrow::ArrowWriter, file::properties::WriterProperties},
};
use async_trait::async_trait;
use common_types::request_id::RequestId;
use futures::StreamExt;
use log::debug;
use object_store::{ObjectStore, Path};
use snafu::ResultExt;

use crate::sst::{
    builder::{RecordBatchStream, SstBuilder, *},
    factory::SstBuilderOptions,
    file::SstMetaData,
    parquet::encoding,
};

/// The implementation of sst based on parquet and object storage.
#[derive(Debug)]
pub struct ParquetSstBuilder<'a, S: ObjectStore> {
    /// The path where the data is persisted.
    path: &'a Path,
    /// The storage where the data is persist.
    storage: &'a S,
    /// Max row group size.
    num_rows_per_row_group: usize,
    compression: Compression,
}

impl<'a, S: ObjectStore> ParquetSstBuilder<'a, S> {
    pub fn new(path: &'a Path, storage: &'a S, options: &SstBuilderOptions) -> Self {
        Self {
            path,
            storage,
            num_rows_per_row_group: options.num_rows_per_row_group,
            compression: options.compression.into(),
        }
    }
}

/// A memory writer implementing the [ParquetWriter].
///
/// The writer accepts the encoded bytes by parquet format and provides the byte
/// stream to the reader.
#[derive(Clone, Debug)]
struct EncodingBuffer {
    // In order to reuse the buffer, the buffer must be wrapped in the Arc and the Mutex because
    // the writer is consumed when building a ArrowWriter.
    inner: Arc<Mutex<EncodingBufferInner>>,
}

impl Default for EncodingBuffer {
    fn default() -> Self {
        Self {
            inner: Arc::new(Mutex::new(EncodingBufferInner {
                bytes_written: 0,
                read_offset: 0,
                buf: Vec::new(),
            })),
        }
    }
}

impl std::io::Write for EncodingBuffer {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let mut inner = self.inner.lock().unwrap();
        inner.write(buf)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        let mut inner = self.inner.lock().unwrap();
        inner.flush()
    }
}

impl std::io::Seek for EncodingBuffer {
    fn seek(&mut self, pos: SeekFrom) -> std::io::Result<u64> {
        let mut inner = self.inner.lock().unwrap();
        inner.seek(pos)
    }
}

impl EncodingBuffer {
    fn into_bytes(self) -> Vec<u8> {
        Arc::try_unwrap(self.inner)
            .unwrap()
            .into_inner()
            .unwrap()
            .buf
    }
}

/// The underlying buffer implementing [ParquetWriter].
///
/// Provides the write function for [ArrowWriter] and read function for
/// [AsyncRead].
#[derive(Clone, Debug)]
struct EncodingBufferInner {
    bytes_written: usize,
    read_offset: usize,
    buf: Vec<u8>,
}

impl std::io::Write for EncodingBufferInner {
    /// Write the `buf` to the `self.buf`.
    ///
    /// The readable bytes should be exhausted before writing new bytes.
    /// `self.bytes_written` and `self.read_offset` is updated after writing.
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        if self.read_offset != 0 {
            assert_eq!(self.buf.len(), self.read_offset);
            self.buf.clear();
            self.buf.reserve(buf.len());
            // reset the read offset
            self.read_offset = 0;
        }

        let bytes_written = self.buf.write(buf)?;
        // accumulate the written bytes
        self.bytes_written += bytes_written;

        Ok(bytes_written)
    }

    /// Actually nothing to flush.
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl std::io::Seek for EncodingBufferInner {
    /// Given the assumption that the seek usage of the [ParquetWriter] in the
    /// parquet project is just `seek(SeekFrom::Current(0))`, the
    /// implementation panics if seek to a different target.
    fn seek(&mut self, pos: SeekFrom) -> std::io::Result<u64> {
        if let SeekFrom::Current(offset) = pos {
            assert_eq!(offset, 0);
            return Ok(self.bytes_written as u64);
        }

        unreachable!("Only can handle the case where seek to current(0)")
    }
}

/// RecordBytesReader provides AsyncRead implementation for the encoded records
/// by parquet.
struct RecordBytesReader {
    request_id: RequestId,
    record_stream: Option<RecordBatchStream>,
    encoding_buffer: EncodingBuffer,
    num_rows_per_row_group: usize,
    compression: Compression,
    meta_data: SstMetaData,
    total_row_num: Arc<AtomicUsize>,

    fetched_row_num: usize,
}

/// Build the write properties containing the sst meta data.
fn build_write_properties(
    num_rows_per_row_group: usize,
    compression: Compression,
    meta_data: &SstMetaData,
) -> Result<WriterProperties> {
    let meta_data_kv = encoding::encode_sst_meta_data(meta_data.clone())
        .map_err(|e| Box::new(e) as _)
        .context(EncodeMetaData)?;

    Ok(WriterProperties::builder()
        .set_key_value_metadata(Some(vec![meta_data_kv]))
        .set_max_row_group_size(num_rows_per_row_group)
        .set_compression(compression)
        .build())
}

impl RecordBytesReader {
    async fn read_all(mut self) -> Result<Vec<u8>> {
        let mut arrow_writer = None;

        let batches = self.record_stream.take().unwrap().collect::<Vec<_>>().await;
        let mut arrow_record_batch_vec = Vec::new();
        let num_batches = batches.len();
        for (index, record_batch) in batches.into_iter().enumerate() {
            let record_batch = record_batch.context(PollRecordBatch)?;

            debug_assert!(
                !record_batch.is_empty(),
                "found empty record batch, request id:{}",
                self.request_id
            );

            self.fetched_row_num += record_batch.num_rows();
            arrow_record_batch_vec.push(record_batch.into_record_batch().into_arrow_record_batch());

            if self.fetched_row_num >= self.num_rows_per_row_group || index + 1 == num_batches {
                self.fetched_row_num = 0;
                let row_num = self
                    .encode_record_batch(&mut arrow_writer, arrow_record_batch_vec)
                    .map_err(|e| Box::new(e) as _)
                    .context(EncodeRecordBatch)?;
                arrow_record_batch_vec = Vec::new();
                self.total_row_num.fetch_add(row_num, Ordering::Relaxed);
            }
        }
        if let Some(arrow_writer) = arrow_writer {
            arrow_writer
                .close()
                .map_err(|e| Box::new(e) as _)
                .context(EncodeMetaData)?;
        }

        let buf = self.encoding_buffer.into_bytes();
        Ok(buf)
    }

    /// Encode the record batch with [ArrowWriter] and the encoded contents is
    /// written to the [EncodingBuffer].
    fn encode_record_batch(
        &mut self,
        arrow_writer: &mut Option<ArrowWriter<EncodingBuffer>>,
        arrow_record_batch_vec: Vec<ArrowRecordBatch>,
    ) -> Result<usize> {
        if arrow_record_batch_vec.is_empty() {
            return Ok(0);
        }

        let arrow_schema = arrow_record_batch_vec[0].schema();

        // create arrow writer if not exist
        if arrow_writer.is_none() {
            let write_props = build_write_properties(
                self.num_rows_per_row_group,
                self.compression,
                &self.meta_data,
            )?;
            let writer = ArrowWriter::try_new(
                self.encoding_buffer.clone(),
                arrow_schema.clone(),
                Some(write_props),
            )
            .map_err(|e| Box::new(e) as _)
            .context(EncodeRecordBatch)?;
            *arrow_writer = Some(writer);
        }

        let record_batch = ArrowRecordBatch::concat(&arrow_schema, &arrow_record_batch_vec)
            .map_err(|e| Box::new(e) as _)
            .context(EncodeRecordBatch)?;

        arrow_writer
            .as_mut()
            .unwrap()
            .write(&record_batch)
            .map_err(|e| Box::new(e) as _)
            .context(EncodeRecordBatch)?;

        Ok(record_batch.num_rows())
    }
}

#[async_trait]
impl<'a, S: ObjectStore> SstBuilder for ParquetSstBuilder<'a, S> {
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
            record_stream: Some(record_stream),
            encoding_buffer: EncodingBuffer::default(),
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
            parquet::reader::ParquetSstReader,
            reader::{tests::check_stream, SstReader},
        },
        table_options,
    };

    // TODO(xikai): add test for reverse reader

    #[test]
    fn test_parquet_build_and_read() {
        let runtime = Arc::new(runtime::Builder::default().build().unwrap());
        parquet_write_and_then_read_back(runtime.clone(), 3, vec![3, 3, 3, 3, 3]);
        // TODO: num_rows should be [4, 4, 4, 3]?
        parquet_write_and_then_read_back(runtime.clone(), 4, vec![4, 2, 4, 2, 3]);
        // TODO: num_rows should be [5, 5, 5]?
        parquet_write_and_then_read_back(runtime, 5, vec![5, 1, 5, 1, 3]);
    }

    fn parquet_write_and_then_read_back(
        runtime: Arc<Runtime>,
        num_rows_per_row_group: usize,
        expected_num_rows: Vec<i64>,
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
            let store = LocalFileSystem::new_with_prefix(root).unwrap();
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
                predicate: Arc::new(Predicate::new(TimeRange::min_to_max())),
                meta_cache: None,
                data_cache: None,
                runtime: runtime.clone(),
            };

            let mut reader = ParquetSstReader::new(&sst_file_path, &store, &sst_reader_options);
            assert_eq!(reader.meta_data().await.unwrap(), &sst_meta);
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
