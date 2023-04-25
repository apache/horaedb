// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

//! Memory table kv, mainly for test.

use std::{
    collections::{BTreeMap, HashMap},
    ops::Bound,
    sync::{Arc, Mutex},
};

use common_util::define_result;
use snafu::{Backtrace, OptionExt, Snafu};

use crate::{
    KeyBoundary, ScanContext, ScanIter, ScanRequest, TableError, TableKv, WriteBatch, WriteContext,
};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Table not found, table:{}.\nBacktrace:\n{}", table_name, backtrace))]
    TableNotFound {
        table_name: String,
        backtrace: Backtrace,
    },

    #[snafu(display("Primary key duplicate.\nBacktrace:\n{}", backtrace))]
    PrimaryKeyDuplicate { backtrace: Backtrace },

    #[snafu(display(
        "Table delete data error, table:{}.\nBacktrace:\n{}",
        table_name,
        backtrace
    ))]
    DeleteData {
        table_name: String,
        backtrace: Backtrace,
    },
}

define_result!(Error);

impl TableError for Error {
    fn is_primary_key_duplicate(&self) -> bool {
        matches!(self, Self::PrimaryKeyDuplicate { .. })
    }
}

enum WriteOp {
    /// Insert (key, value).
    Insert(Vec<u8>, Vec<u8>),
    /// Insert or update (key, value).
    InsertOrUpdate(Vec<u8>, Vec<u8>),
    /// Delete key.
    Delete(Vec<u8>),
}

#[derive(Default)]
pub struct MemoryWriteBatch(Vec<WriteOp>);

impl WriteBatch for MemoryWriteBatch {
    fn with_capacity(capacity: usize) -> Self {
        Self(Vec::with_capacity(capacity))
    }

    fn insert(&mut self, key: &[u8], value: &[u8]) {
        let op = WriteOp::Insert(key.to_vec(), value.to_vec());

        self.0.push(op);
    }

    fn insert_or_update(&mut self, key: &[u8], value: &[u8]) {
        let op = WriteOp::InsertOrUpdate(key.to_vec(), value.to_vec());

        self.0.push(op);
    }

    fn delete(&mut self, key: &[u8]) {
        let op = WriteOp::Delete(key.to_vec());

        self.0.push(op);
    }
}

type KeyValue = (Vec<u8>, Vec<u8>);

#[derive(Debug)]
pub struct MemoryScanIter {
    /// All key values from the iterator.
    key_values: Vec<KeyValue>,
    /// Current key/value offset.
    offset: usize,
}

impl MemoryScanIter {
    fn new(key_values: Vec<KeyValue>) -> Self {
        Self {
            key_values,
            offset: 0,
        }
    }
}

impl ScanIter for MemoryScanIter {
    type Error = Error;

    fn valid(&self) -> bool {
        self.offset < self.key_values.len()
    }

    fn next(&mut self) -> Result<bool> {
        assert!(self.valid());
        self.offset += 1;
        Ok(self.valid())
    }

    fn key(&self) -> &[u8] {
        assert!(self.valid());
        &self.key_values[self.offset].0
    }

    fn value(&self) -> &[u8] {
        assert!(self.valid());
        &self.key_values[self.offset].1
    }
}

type BytesMap = BTreeMap<Vec<u8>, Vec<u8>>;

#[derive(Default, Debug)]
struct Table {
    data: Mutex<BytesMap>,
}

impl Table {
    fn truncate(&self) {
        self.data.lock().unwrap().clear();
    }

    fn insert(&self, key: Vec<u8>, value: Vec<u8>) -> Result<()> {
        let mut data = self.data.lock().unwrap();
        if data.contains_key(&key) {
            return PrimaryKeyDuplicate.fail();
        }

        data.insert(key, value);

        Ok(())
    }

    fn insert_or_update(&self, key: Vec<u8>, value: Vec<u8>) -> Result<()> {
        let mut data = self.data.lock().unwrap();
        data.insert(key, value);

        Ok(())
    }

    fn delete(&self, key: &[u8]) -> Result<()> {
        let mut data = self.data.lock().unwrap();
        data.remove(key);

        Ok(())
    }

    fn scan(&self, request: ScanRequest) -> Vec<KeyValue> {
        let range = match to_range(request.start, request.end) {
            Some(v) => v,
            None => return Vec::new(),
        };

        let data = self.data.lock().unwrap();
        if request.reverse {
            data.range(range)
                .rev()
                .map(|(k, v)| (k.to_vec(), v.to_vec()))
                .collect()
        } else {
            data.range(range)
                .map(|(k, v)| (k.to_vec(), v.to_vec()))
                .collect()
        }
    }

    fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        let data = self.data.lock().unwrap();
        data.get(key).cloned()
    }
}

type TableRef = Arc<Table>;
type TableMap = HashMap<String, TableRef>;

#[derive(Clone, Default, Debug)]
pub struct MemoryImpl {
    tables: Arc<Mutex<TableMap>>,
}

impl MemoryImpl {
    fn find_table(&self, table_name: &str) -> Option<TableRef> {
        self.tables.lock().unwrap().get(table_name).cloned()
    }
}

impl TableKv for MemoryImpl {
    type Error = Error;
    type ScanIter = MemoryScanIter;
    type WriteBatch = MemoryWriteBatch;

    fn table_exists(&self, table_name: &str) -> Result<bool> {
        let exists = self.tables.lock().unwrap().contains_key(table_name);
        Ok(exists)
    }

    fn create_table(&self, table_name: &str) -> Result<()> {
        let mut tables = self.tables.lock().unwrap();
        if tables.contains_key(table_name) {
            return Ok(());
        }

        let table = Arc::new(Table::default());
        tables.insert(table_name.to_string(), table);

        Ok(())
    }

    fn drop_table(&self, table_name: &str) -> Result<()> {
        let mut tables = self.tables.lock().unwrap();
        tables.remove(table_name);

        Ok(())
    }

    fn truncate_table(&self, table_name: &str) -> Result<()> {
        let table_opt = self.find_table(table_name);
        if let Some(table) = table_opt {
            table.truncate();
        }

        Ok(())
    }

    fn write(
        &self,
        _ctx: WriteContext,
        table_name: &str,
        write_batch: MemoryWriteBatch,
    ) -> Result<()> {
        let table = self
            .find_table(table_name)
            .context(TableNotFound { table_name })?;

        for op in write_batch.0 {
            match op {
                WriteOp::Insert(k, v) => {
                    table.insert(k, v)?;
                }
                WriteOp::InsertOrUpdate(k, v) => {
                    table.insert_or_update(k, v)?;
                }
                WriteOp::Delete(k) => {
                    table.delete(&k)?;
                }
            }
        }

        Ok(())
    }

    fn scan(
        &self,
        _ctx: ScanContext,
        table_name: &str,
        request: ScanRequest,
    ) -> Result<MemoryScanIter> {
        let table = self
            .find_table(table_name)
            .context(TableNotFound { table_name })?;

        let key_values = table.scan(request);

        Ok(MemoryScanIter::new(key_values))
    }

    fn get(&self, table_name: &str, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let table = self
            .find_table(table_name)
            .context(TableNotFound { table_name })?;

        Ok(table.get(key))
    }

    fn delete(&self, table_name: &str, key: &[u8]) -> std::result::Result<i64, Self::Error> {
        let table = self
            .find_table(table_name)
            .context(TableNotFound { table_name })?;
        table.delete(key)?;
        Ok(0)
    }
}

type Range = (Bound<Vec<u8>>, Bound<Vec<u8>>);

fn to_range(start: KeyBoundary, end: KeyBoundary) -> Option<Range> {
    let start_bound = match start {
        KeyBoundary::Included(k) => Bound::Included(k.to_bytes()),
        KeyBoundary::Excluded(k) => Bound::Excluded(k.to_bytes()),
        KeyBoundary::MinIncluded => Bound::Unbounded,
        KeyBoundary::MaxIncluded => return None,
    };

    let end_bound = match end {
        KeyBoundary::Included(k) => Bound::Included(k.to_bytes()),
        KeyBoundary::Excluded(k) => Bound::Excluded(k.to_bytes()),
        KeyBoundary::MinIncluded => return None,
        KeyBoundary::MaxIncluded => Bound::Unbounded,
    };

    Some((start_bound, end_bound))
}
