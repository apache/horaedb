// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

use std::{collections::VecDeque, sync::Arc};

use arrow_deps::{
    arrow::{
        datatypes::SchemaRef,
        error::Result as ArrowResult,
        record_batch::{RecordBatch, RecordBatchReader},
    },
    parquet::{
        arrow::{
            self, arrow_reader::ParquetRecordBatchReader, ArrowReader, ParquetFileArrowReader,
        },
        errors::Result,
        file::{
            metadata::{FileMetaData, ParquetMetaData},
            reader::{FileReader, RowGroupReader},
        },
        record::reader::RowIter,
        schema::types::Type as SchemaType,
    },
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
                Ok(v) => arrow_deps::util::reverse_record_batch(&v),
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

/// Reader for one [RowGroup] of the [FileReader].
struct SingleRowGroupFileReader {
    file_reader: Arc<dyn FileReader>,
    /// The index of row group in `file_reader` to read.
    row_group_idx: usize,
    /// The meta data for the reader of the one row group.
    meta_data: ParquetMetaData,
}

impl SingleRowGroupFileReader {
    fn new(file_reader: Arc<dyn FileReader>, row_group_idx: usize) -> Self {
        let meta_data = {
            let orig_meta_data = file_reader.metadata();
            let orig_file_meta_data = orig_meta_data.file_metadata();
            let row_group_meta_data = orig_meta_data.row_group(row_group_idx);
            let file_meta_data = FileMetaData::new(
                orig_file_meta_data.version(),
                // provide the row group's row number because of the reader only contains one row
                // group.
                row_group_meta_data.num_rows(),
                orig_file_meta_data.created_by().clone(),
                orig_file_meta_data.key_value_metadata().clone(),
                orig_file_meta_data.schema_descr_ptr(),
                orig_file_meta_data.column_orders().cloned(),
            );
            ParquetMetaData::new(file_meta_data, vec![row_group_meta_data.clone()])
        };

        Self {
            file_reader,
            row_group_idx,
            meta_data,
        }
    }
}

impl FileReader for SingleRowGroupFileReader {
    fn metadata(&self) -> &ParquetMetaData {
        &self.meta_data
    }

    fn num_row_groups(&self) -> usize {
        1
    }

    fn get_row_group(&self, i: usize) -> Result<Box<dyn RowGroupReader + '_>> {
        self.file_reader.get_row_group(self.row_group_idx + i)
    }

    fn get_row_iter(&self, projection: Option<SchemaType>) -> Result<RowIter> {
        RowIter::from_file(projection, self)
    }
}

/// Builder for [ReverseRecordBatchReader] from the `file_reader`.
#[must_use]
pub struct Builder {
    file_reader: Arc<dyn FileReader>,
    batch_size: usize,
    projection: Option<Vec<usize>>,
}

impl Builder {
    pub fn new(file_reader: Arc<dyn FileReader>, batch_size: usize) -> Self {
        Self {
            file_reader,
            batch_size,
            projection: None,
        }
    }

    pub fn projection(mut self, projection: Option<Vec<usize>>) -> Self {
        self.projection = projection;

        self
    }

    pub fn build(self) -> Result<ReversedFileReader> {
        let mut reversed_readers = Vec::with_capacity(self.file_reader.num_row_groups());
        for row_group_idx in (0..self.file_reader.num_row_groups()).rev() {
            let row_group_file_reader =
                SingleRowGroupFileReader::new(self.file_reader.clone(), row_group_idx);
            let mut arrow_reader = ParquetFileArrowReader::new(Arc::new(row_group_file_reader));
            let batch_reader = if let Some(proj) = &self.projection {
                arrow_reader.get_record_reader_by_columns(proj.iter().cloned(), self.batch_size)?
            } else {
                arrow_reader.get_record_reader(self.batch_size)?
            };
            reversed_readers.push(batch_reader);
        }

        let schema = {
            let file_metadata = self.file_reader.metadata().file_metadata();
            Arc::new(arrow::parquet_to_arrow_schema(
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
    use arrow_deps::parquet::file::reader::SerializedFileReader;

    use super::*;

    const TEST_FILE: &str = "binary.parquet";
    const TEST_BATCH_SIZE: usize = 1000;

    fn check_reversed_row_iter(original: RowIter, reversed: ReversedFileReader) {
        let mut original_reversed_rows: Vec<_> = original.into_iter().collect();
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
        let test_file = crate::tests::get_test_file(TEST_FILE);
        let file_reader: Arc<dyn FileReader> = Arc::new(
            SerializedFileReader::new(test_file).expect("Should succeed to init file reader"),
        );
        let reversed_reader = Builder::new(file_reader.clone(), TEST_BATCH_SIZE)
            .build()
            .expect("Should succeed to build reversed file reader");
        check_reversed_row_iter(file_reader.get_row_iter(None).unwrap(), reversed_reader);
    }
}
