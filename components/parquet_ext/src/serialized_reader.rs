// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

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

//! fork from https://github.com/apache/arrow-rs/blob/5.2.0/parquet/src/file/serialized_reader.rs

//! Contains implementations of the reader traits FileReader, RowGroupReader and
//! PageReader Also contains implementations of the ChunkReader for files (with
//! buffering) and byte arrays (RAM)

use std::{fs::File, option::Option::Some, sync::Arc};

use bytes::{Buf, Bytes};
use parquet::{
    column::page::PageReader,
    errors::Result,
    file::{footer, metadata::*, reader::*},
    record::{reader::RowIter, Row},
    schema::types::Type as SchemaType,
};

use crate::{DataCacheRef, MetaCacheRef};

fn format_page_data_key(name: &str, start: u64, length: usize) -> String {
    format!("{}_{}_{}", name, start, length)
}

/// Conversion into a [`RowIter`](crate::record::reader::RowIter)
/// using the full file schema over all row groups.
impl IntoIterator for CacheableSerializedFileReader<File> {
    type IntoIter = RowIter<'static>;
    type Item = Row;

    fn into_iter(self) -> Self::IntoIter {
        RowIter::from_file_into(Box::new(self))
    }
}

// ----------------------------------------------------------------------
// Implementations of file & row group readers

/// A serialized with cache implementation for Parquet [`FileReader`].
/// Two kinds of items are cacheable:
///  - [`ParquetMetaData`]: only used for creating the reader.
///  - Column chunk bytes: used for reading data by
///    [`SerializedRowGroupReader`].
///
/// Note: the implementation is based on the https://github.com/apache/arrow-rs/blob/5.2.0/parquet/src/file/serialized_reader.rs.
pub struct CacheableSerializedFileReader<R: ChunkReader> {
    name: String,
    chunk_reader: Arc<R>,
    metadata: Arc<ParquetMetaData>,
    data_cache: Option<DataCacheRef>,
}

impl<R: 'static + ChunkReader> CacheableSerializedFileReader<R> {
    /// Creates file reader from a Parquet file.
    /// Returns error if Parquet file does not exist or is corrupt.
    pub fn new(
        name: String,
        chunk_reader: R,
        meta_cache: Option<MetaCacheRef>,
        data_cache: Option<DataCacheRef>,
    ) -> Result<Self> {
        // MODIFICATION START: consider cache for meta data.
        let metadata = if let Some(meta_cache) = meta_cache {
            if let Some(v) = meta_cache.get(&name) {
                v
            } else {
                let meta_data = Arc::new(footer::parse_metadata(&chunk_reader)?);
                meta_cache.put(name.clone(), meta_data.clone());
                meta_data
            }
        } else {
            Arc::new(footer::parse_metadata(&chunk_reader)?)
        };
        // MODIFICATION END.

        Ok(Self {
            name,
            chunk_reader: Arc::new(chunk_reader),
            metadata,
            data_cache,
        })
    }

    /// Filters row group metadata to only those row groups,
    /// for which the predicate function returns true
    pub fn filter_row_groups(&mut self, predicate: &dyn Fn(&RowGroupMetaData, usize) -> bool) {
        let mut filtered_row_groups = Vec::<RowGroupMetaData>::new();
        for (i, row_group_metadata) in self.metadata.row_groups().iter().enumerate() {
            if predicate(row_group_metadata, i) {
                filtered_row_groups.push(row_group_metadata.clone());
            }
        }
        self.metadata = Arc::new(ParquetMetaData::new(
            self.metadata.file_metadata().clone(),
            filtered_row_groups,
        ));
    }
}

impl<R: 'static + ChunkReader> FileReader for CacheableSerializedFileReader<R> {
    fn metadata(&self) -> &ParquetMetaData {
        &self.metadata
    }

    fn num_row_groups(&self) -> usize {
        self.metadata.num_row_groups()
    }

    fn get_row_group(&self, i: usize) -> Result<Box<dyn RowGroupReader + '_>> {
        let row_group_metadata = self.metadata.row_group(i);
        // Row groups should be processed sequentially.
        let f = Arc::clone(&self.chunk_reader);
        Ok(Box::new(SerializedRowGroupReader::new(
            f,
            row_group_metadata,
            self.name.clone(),
            self.data_cache.clone(),
        )))
    }

    fn get_row_iter(&self, projection: Option<SchemaType>) -> Result<RowIter> {
        RowIter::from_file(projection, self)
    }
}

/// A serialized with cache implementation for Parquet [`RowGroupReader`].
///
/// The cache is used for column data chunk when building [`PageReader`].
///
/// NOTE: the implementation is based on the https://github.com/apache/arrow-rs/blob/5.2.0/parquet/src/file/serialized_reader.rs
pub struct SerializedRowGroupReader<'a, R: ChunkReader> {
    chunk_reader: Arc<R>,
    metadata: &'a RowGroupMetaData,
    name: String,
    data_cache: Option<DataCacheRef>,
}

impl<'a, R: ChunkReader> SerializedRowGroupReader<'a, R> {
    /// Creates new row group reader from a file and row group metadata.
    fn new(
        chunk_reader: Arc<R>,
        metadata: &'a RowGroupMetaData,
        name: String,
        data_cache: Option<DataCacheRef>,
    ) -> Self {
        Self {
            chunk_reader,
            metadata,
            name,
            data_cache,
        }
    }
}

struct CacheableChunkReader<R: ChunkReader> {
    reader: Arc<R>,
    data_cache: Option<DataCacheRef>,
    name: String,
}

impl<R: ChunkReader> Length for CacheableChunkReader<R> {
    fn len(&self) -> u64 {
        self.reader.len() as u64
    }
}

impl<R: ChunkReader> ChunkReader for CacheableChunkReader<R> {
    type T = bytes::buf::Reader<Bytes>;

    fn get_read(&self, start: u64, length: usize) -> Result<Self::T> {
        Ok(self.get_bytes(start, length)?.reader())
    }

    fn get_bytes(&self, start: u64, length: usize) -> Result<Bytes> {
        let bytes = if let Some(data_cache) = &self.data_cache {
            let key = format_page_data_key(&self.name, start, length);
            if let Some(v) = data_cache.get(&key) {
                // TODO: avoid data copy
                Bytes::from(v.to_vec())
            } else {
                let data = self.reader.get_bytes(start, length)?;
                // TODO: avoid data copy
                data_cache.put(key, Arc::new(data.to_vec()));
                data
            }
        } else {
            self.reader.get_bytes(start, length)?
        };
        Ok(bytes)
    }
}

impl<'a, R: 'static + ChunkReader> RowGroupReader for SerializedRowGroupReader<'a, R> {
    fn metadata(&self) -> &RowGroupMetaData {
        self.metadata
    }

    fn num_columns(&self) -> usize {
        self.metadata.num_columns()
    }

    // TODO: fix PARQUET-816
    fn get_column_page_reader(&self, i: usize) -> Result<Box<dyn PageReader>> {
        let col = self.metadata.column(i);

        let page_locations = self
            .metadata
            .page_offset_index()
            .as_ref()
            .map(|x| x[i].clone());

        let cacheable_chunk_reader = CacheableChunkReader {
            reader: self.chunk_reader.clone(),
            data_cache: self.data_cache.clone(),
            name: self.name.clone(),
        };

        Ok(Box::new(SerializedPageReader::new(
            Arc::new(cacheable_chunk_reader),
            col,
            self.metadata.num_rows() as usize,
            page_locations,
        )?))
    }

    fn get_row_iter(&self, projection: Option<SchemaType>) -> Result<RowIter> {
        RowIter::from_row_group(projection, self)
    }
}

#[cfg(test)]
mod tests {
    use std::{io::Read, sync::Arc};

    use parquet::{
        basic::{ColumnOrder, Encoding},
        column::page::Page,
    };

    use super::*;
    use crate::cache::{LruDataCache, LruMetaCache};

    #[test]
    fn test_cursor_and_file_has_the_same_behaviour() {
        let mut buf: Vec<u8> = Vec::new();
        crate::tests::get_test_file("alltypes_plain.parquet")
            .read_to_end(&mut buf)
            .unwrap();
        let cursor = Bytes::from(buf);
        let read_from_cursor =
            CacheableSerializedFileReader::new("read_from_cursor".to_string(), cursor, None, None)
                .unwrap();

        let test_file = crate::tests::get_test_file("alltypes_plain.parquet");
        let read_from_file =
            CacheableSerializedFileReader::new("read_from_file".to_string(), test_file, None, None)
                .unwrap();

        let file_iter = read_from_file.get_row_iter(None).unwrap();
        let cursor_iter = read_from_cursor.get_row_iter(None).unwrap();

        assert!(file_iter.eq(cursor_iter));
    }

    #[test]
    fn test_reuse_file_chunk() {
        // This test covers the case of maintaining the correct start position in a file
        // stream for each column reader after initializing and moving to the next one
        // (without necessarily reading the entire column).
        let test_file = crate::tests::get_test_file("alltypes_plain.parquet");
        let reader =
            CacheableSerializedFileReader::new("test".to_string(), test_file, None, None).unwrap();
        let row_group = reader.get_row_group(0).unwrap();

        let mut page_readers = Vec::new();
        for i in 0..row_group.num_columns() {
            page_readers.push(row_group.get_column_page_reader(i).unwrap());
        }

        // Now buffer each col reader, we do not expect any failures like:
        // General("underlying Thrift error: end of file")
        for mut page_reader in page_readers {
            assert!(page_reader.get_next_page().is_ok());
        }
    }

    fn new_filer_reader_with_cache() -> CacheableSerializedFileReader<File> {
        let data_cache: Option<DataCacheRef> = Some(Arc::new(LruDataCache::new(1000)));
        let meta_cache: Option<MetaCacheRef> = Some(Arc::new(LruMetaCache::new(1000)));
        let test_file = crate::tests::get_test_file("alltypes_plain.parquet");
        let reader_result = CacheableSerializedFileReader::new(
            "test".to_string(),
            test_file,
            meta_cache.clone(),
            data_cache.clone(),
        );
        assert!(reader_result.is_ok());
        reader_result.unwrap()
    }

    fn test_with_file_reader(reader: &CacheableSerializedFileReader<File>) {
        // Test contents in Parquet metadata
        let metadata = reader.metadata();
        assert_eq!(metadata.num_row_groups(), 1);

        // Test contents in file metadata
        let file_metadata = metadata.file_metadata();
        assert!(file_metadata.created_by().is_some());
        assert_eq!(
            file_metadata.created_by().as_ref().unwrap(),
            &"impala version 1.3.0-INTERNAL (build 8a48ddb1eff84592b3fc06bc6f51ec120e1fffc9)"
        );
        assert!(file_metadata.key_value_metadata().is_none());
        assert_eq!(file_metadata.num_rows(), 8);
        assert_eq!(file_metadata.version(), 1);
        assert_eq!(file_metadata.column_orders(), None);

        // Test contents in row group metadata
        let row_group_metadata = metadata.row_group(0);
        assert_eq!(row_group_metadata.num_columns(), 11);
        assert_eq!(row_group_metadata.num_rows(), 8);
        assert_eq!(row_group_metadata.total_byte_size(), 671);
        // Check each column order
        for i in 0..row_group_metadata.num_columns() {
            assert_eq!(file_metadata.column_order(i), ColumnOrder::UNDEFINED);
        }

        // Test row group reader
        let row_group_reader_result = reader.get_row_group(0);
        assert!(row_group_reader_result.is_ok());
        let row_group_reader: Box<dyn RowGroupReader> = row_group_reader_result.unwrap();
        assert_eq!(
            row_group_reader.num_columns(),
            row_group_metadata.num_columns()
        );
        assert_eq!(
            row_group_reader.metadata().total_byte_size(),
            row_group_metadata.total_byte_size()
        );

        // Test page readers
        // TODO: test for every column
        let page_reader_0_result = row_group_reader.get_column_page_reader(0);
        assert!(page_reader_0_result.is_ok());
        let mut page_reader_0: Box<dyn PageReader> = page_reader_0_result.unwrap();
        let mut page_count = 0;
        while let Ok(Some(page)) = page_reader_0.get_next_page() {
            let is_expected_page = match page {
                Page::DictionaryPage {
                    buf,
                    num_values,
                    encoding,
                    is_sorted,
                } => {
                    assert_eq!(buf.len(), 32);
                    assert_eq!(num_values, 8);
                    assert_eq!(encoding, Encoding::PLAIN_DICTIONARY);
                    assert!(!is_sorted);
                    true
                }
                Page::DataPage {
                    buf,
                    num_values,
                    encoding,
                    def_level_encoding,
                    rep_level_encoding,
                    statistics,
                } => {
                    assert_eq!(buf.len(), 11);
                    assert_eq!(num_values, 8);
                    assert_eq!(encoding, Encoding::PLAIN_DICTIONARY);
                    assert_eq!(def_level_encoding, Encoding::RLE);
                    assert_eq!(rep_level_encoding, Encoding::BIT_PACKED);
                    assert!(statistics.is_none());
                    true
                }
                _ => false,
            };
            assert!(is_expected_page);
            page_count += 1;
        }
        assert_eq!(page_count, 2);
    }

    #[test]
    fn test_file_reader() {
        let test_file = crate::tests::get_test_file("alltypes_plain.parquet");
        let reader = CacheableSerializedFileReader::new("test".to_string(), test_file, None, None)
            .expect("Should succeed to build test reader");
        test_with_file_reader(&reader);
    }

    #[test]
    fn test_file_reader_with_cache() {
        let reader = new_filer_reader_with_cache();
        let test_num = 10usize;
        for _ in 0..test_num {
            test_with_file_reader(&reader);
        }
    }

    #[test]
    fn test_file_reader_datapage_v2() {
        let test_file = crate::tests::get_test_file("datapage_v2.snappy.parquet");
        let reader_result =
            CacheableSerializedFileReader::new("test".to_string(), test_file, None, None);
        assert!(reader_result.is_ok());
        let reader = reader_result.unwrap();

        // Test contents in Parquet metadata
        let metadata = reader.metadata();
        assert_eq!(metadata.num_row_groups(), 1);

        // Test contents in file metadata
        let file_metadata = metadata.file_metadata();
        assert!(file_metadata.created_by().is_some());
        assert_eq!(
            file_metadata.created_by().as_ref().unwrap(),
            &"parquet-mr version 1.8.1 (build 4aba4dae7bb0d4edbcf7923ae1339f28fd3f7fcf)"
        );
        assert!(file_metadata.key_value_metadata().is_some());
        assert_eq!(
            file_metadata.key_value_metadata().to_owned().unwrap().len(),
            1
        );

        assert_eq!(file_metadata.num_rows(), 5);
        assert_eq!(file_metadata.version(), 1);
        assert_eq!(file_metadata.column_orders(), None);

        let row_group_metadata = metadata.row_group(0);

        // Check each column order
        for i in 0..row_group_metadata.num_columns() {
            assert_eq!(file_metadata.column_order(i), ColumnOrder::UNDEFINED);
        }

        // Test row group reader
        let row_group_reader_result = reader.get_row_group(0);
        assert!(row_group_reader_result.is_ok());
        let row_group_reader: Box<dyn RowGroupReader> = row_group_reader_result.unwrap();
        assert_eq!(
            row_group_reader.num_columns(),
            row_group_metadata.num_columns()
        );
        assert_eq!(
            row_group_reader.metadata().total_byte_size(),
            row_group_metadata.total_byte_size()
        );

        // Test page readers
        // TODO: test for every column
        let page_reader_0_result = row_group_reader.get_column_page_reader(0);
        assert!(page_reader_0_result.is_ok());
        let mut page_reader_0: Box<dyn PageReader> = page_reader_0_result.unwrap();
        let mut page_count = 0;
        while let Ok(Some(page)) = page_reader_0.get_next_page() {
            let is_expected_page = match page {
                Page::DictionaryPage {
                    buf,
                    num_values,
                    encoding,
                    is_sorted,
                } => {
                    assert_eq!(buf.len(), 7);
                    assert_eq!(num_values, 1);
                    assert_eq!(encoding, Encoding::PLAIN);
                    assert!(!is_sorted);
                    true
                }
                Page::DataPageV2 {
                    buf,
                    num_values,
                    encoding,
                    num_nulls,
                    num_rows,
                    def_levels_byte_len,
                    rep_levels_byte_len,
                    is_compressed,
                    statistics,
                } => {
                    assert_eq!(buf.len(), 4);
                    assert_eq!(num_values, 5);
                    assert_eq!(encoding, Encoding::RLE_DICTIONARY);
                    assert_eq!(num_nulls, 1);
                    assert_eq!(num_rows, 5);
                    assert_eq!(def_levels_byte_len, 2);
                    assert_eq!(rep_levels_byte_len, 0);
                    assert!(is_compressed);
                    assert!(statistics.is_some());
                    true
                }
                _ => false,
            };
            assert!(is_expected_page);
            page_count += 1;
        }
        assert_eq!(page_count, 2);
    }

    #[test]
    fn test_page_iterator() {
        let file = crate::tests::get_test_file("alltypes_plain.parquet");
        let file_reader = Arc::new(
            CacheableSerializedFileReader::new("test".to_string(), file, None, None).unwrap(),
        );

        let mut page_iterator = FilePageIterator::new(0, file_reader.clone()).unwrap();

        // read first page
        let page = page_iterator.next();
        assert!(page.is_some());
        assert!(page.unwrap().is_ok());

        // reach end of file
        let page = page_iterator.next();
        assert!(page.is_none());

        let row_group_indices = Box::new(0..1);
        let mut page_iterator =
            FilePageIterator::with_row_groups(0, row_group_indices, file_reader).unwrap();

        // read first page
        let page = page_iterator.next();
        assert!(page.is_some());
        assert!(page.unwrap().is_ok());

        // reach end of file
        let page = page_iterator.next();
        assert!(page.is_none());
    }

    #[test]
    fn test_file_reader_key_value_metadata() {
        let file = crate::tests::get_test_file("binary.parquet");
        let file_reader = Arc::new(
            CacheableSerializedFileReader::new("test".to_string(), file, None, None).unwrap(),
        );

        let metadata = file_reader
            .metadata
            .file_metadata()
            .key_value_metadata()
            .unwrap();

        assert_eq!(metadata.len(), 3);

        assert_eq!(metadata.get(0).unwrap().key, "parquet.proto.descriptor");

        assert_eq!(metadata.get(1).unwrap().key, "writer.model.name");
        assert_eq!(metadata.get(1).unwrap().value, Some("protobuf".to_owned()));

        assert_eq!(metadata.get(2).unwrap().key, "parquet.proto.class");
        assert_eq!(
            metadata.get(2).unwrap().value,
            Some("foo.baz.Foobaz$Event".to_owned())
        );
    }

    #[test]
    fn test_file_reader_filter_row_groups() -> Result<()> {
        let test_file = crate::tests::get_test_file("alltypes_plain.parquet");
        let mut reader =
            CacheableSerializedFileReader::new("test".to_string(), test_file, None, None)?;

        // test initial number of row groups
        let metadata = reader.metadata();
        assert_eq!(metadata.num_row_groups(), 1);

        // test filtering out all row groups
        reader.filter_row_groups(&|_, _| false);
        let metadata = reader.metadata();
        assert_eq!(metadata.num_row_groups(), 0);

        Ok(())
    }
}
