// Copyright 2022-2023 CeresDB Project Authors. Licensed under Apache-2.0.

use std::{collections::VecDeque, sync::Arc};

use arrow::{
    datatypes::SchemaRef,
    error::Result as ArrowResult,
    record_batch::{RecordBatch, RecordBatchReader},
};
use arrow_ext::operation;
use bytes::Bytes;
use parquet::{
    arrow::{
        arrow_reader::{ParquetRecordBatchReader, ParquetRecordBatchReaderBuilder},
        ProjectionMask,
    },
    errors::Result,
    file::metadata::ParquetMetaData,
};

/// The reverse reader for [FileReader].
///
/// The details of implementation is:
/// - Split the original [FileReader] into [RowGroup]s.
/// - Reverse all the [RowGroup]s into `reversed_readers` so the order of
///   [RowGroup] is already reversed.
/// - Reverse all the [RecordBatch]es of the [RowGroup] into the
///   `current_reversed_batches`.
/// - Pop one [RecordBatch] from the `current_reversed_batches`and reverse its
///   data and send it to caller.
pub struct ReversedFileReader {
    schema: SchemaRef,
    /// The readers are arranged in reversed order and built from the
    /// [RowGroup].
    reversed_readers: Vec<ParquetRecordBatchReader>,
    /// Buffer all the record batches of one reader and every record batch is
    /// reversed.
    current_reversed_batches: VecDeque<ArrowResult<RecordBatch>>,
    next_reader_idx: usize,
}

impl ReversedFileReader {
    fn fetch_next_batches_if_necessary(&mut self) {
        if !self.current_reversed_batches.is_empty() {
            // current reader is not exhausted and no need to fetch data.
            return;
        }

        if self.next_reader_idx >= self.reversed_readers.len() {
            // all the readers have been exhausted.
            return;
        }

        let reader = &mut self.reversed_readers[self.next_reader_idx];
        for batch in reader {
            // reverse the order of the data of every record batch.
            let reversed_batch = match batch {
                Ok(v) => operation::reverse_record_batch(&v),
                Err(e) => Err(e),
            };
            // reverse the order of the record batches.
            self.current_reversed_batches.push_front(reversed_batch);
        }

        self.next_reader_idx += 1;
    }
}

impl Iterator for ReversedFileReader {
    type Item = ArrowResult<RecordBatch>;

    fn next(&mut self) -> Option<Self::Item> {
        self.fetch_next_batches_if_necessary();
        self.current_reversed_batches.pop_front()
    }
}

impl RecordBatchReader for ReversedFileReader {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

/// Builder for [ReverseRecordBatchReader] from the `file_reader`.
#[must_use]
pub struct Builder<'a> {
    metadata: &'a ParquetMetaData,
    chunk_reader: Bytes,
    batch_size: usize,
    projection: Option<Vec<usize>>,
    row_groups: Option<Vec<usize>>,
}

impl<'a> Builder<'a> {
    pub fn new(metadata: &'a ParquetMetaData, chunk_reader: Bytes, batch_size: usize) -> Self {
        Self {
            metadata,
            chunk_reader,
            batch_size,
            projection: None,
            row_groups: None,
        }
    }

    pub fn projection(mut self, projection: Option<Vec<usize>>) -> Self {
        self.projection = projection;

        self
    }

    pub fn target_row_groups(mut self, row_groups: Option<Vec<usize>>) -> Self {
        self.row_groups = row_groups;

        self
    }

    pub fn build(self) -> Result<ReversedFileReader> {
        let row_groups = match self.row_groups {
            Some(v) => v,
            None => (0..self.metadata.num_row_groups()).collect(),
        };

        let mut reversed_readers = Vec::with_capacity(row_groups.len());
        for row_group_idx in row_groups.into_iter().rev() {
            let builder = ParquetRecordBatchReaderBuilder::try_new(self.chunk_reader.clone())?
                .with_batch_size(self.batch_size)
                .with_row_groups(vec![row_group_idx]);
            let builder = if let Some(proj) = &self.projection {
                let proj_mask = ProjectionMask::leaves(
                    builder.metadata().file_metadata().schema_descr(),
                    proj.iter().copied(),
                );
                builder.with_projection(proj_mask)
            } else {
                builder
            };
            reversed_readers.push(builder.build()?);
        }

        let schema = {
            let file_metadata = self.metadata.file_metadata();
            Arc::new(parquet::arrow::parquet_to_arrow_schema(
                file_metadata.schema_descr(),
                file_metadata.key_value_metadata(),
            )?)
        };

        Ok(ReversedFileReader {
            schema,
            reversed_readers,
            current_reversed_batches: VecDeque::new(),
            next_reader_idx: 0,
        })
    }
}

#[cfg(test)]
mod tests {
    use std::io::Read;

    use parquet::{
        file::{reader::FileReader, serialized_reader::SerializedFileReader},
        record::reader::RowIter,
    };

    use super::*;

    const TEST_FILE: &str = "binary.parquet";
    const TEST_BATCH_SIZE: usize = 1000;

    fn check_reversed_row_iter(original: RowIter, reversed: ReversedFileReader) {
        let mut original_reversed_rows: Vec<_> = original.into_iter().map(|v| v.unwrap()).collect();
        original_reversed_rows.reverse();

        let reversed_record_batches: Vec<_> = reversed
            .into_iter()
            .map(|v| v.expect("Fail to fetch record batch"))
            .collect();

        crate::tests::check_rows_and_record_batches(
            &original_reversed_rows,
            &reversed_record_batches,
        );
    }

    #[test]
    fn test_reverse_file_reader() {
        let mut test_file = crate::tests::get_test_file(TEST_FILE);
        let mut buf = Vec::new();
        let _ = test_file.read_to_end(&mut buf).unwrap();
        let bytes = Bytes::from(buf);
        let file_reader =
            SerializedFileReader::new(bytes.clone()).expect("Should succeed to make file reader");
        let reversed_reader = Builder::new(file_reader.metadata(), bytes, TEST_BATCH_SIZE)
            .build()
            .expect("Should succeed to build reversed file reader");
        check_reversed_row_iter(file_reader.get_row_iter(None).unwrap(), reversed_reader);
    }
}
