// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

//! Remote key-value api based on table.

use std::{fmt, time::Duration};

pub mod config;
pub mod memory;
pub mod obkv;
#[cfg(test)]
mod tests;

/// Context during write.
#[derive(Default)]
pub struct WriteContext {}

/// Write operations.
pub trait WriteBatch: Default {
    /// Create a new batch with given `capacity`.
    fn with_capacity(capacity: usize) -> Self;

    /// Insert given key-value pair, write should returns error if
    /// given key already exists.
    fn insert(&mut self, key: &[u8], value: &[u8]);

    /// Insert given key-value pair or update it if value of given
    /// key already exists.
    fn insert_or_update(&mut self, key: &[u8], value: &[u8]);

    /// Delete value with given key.
    fn delete(&mut self, key: &[u8]);
}

/// Key to seek.
#[derive(Debug, Clone)]
pub struct SeekKey(Vec<u8>);

impl From<&[u8]> for SeekKey {
    fn from(key: &[u8]) -> Self {
        Self(key.to_vec())
    }
}

impl SeekKey {
    pub fn to_bytes(&self) -> Vec<u8> {
        self.0.clone()
    }

    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }
}

/// Boundary of key to seek.
#[derive(Debug, Clone)]
pub enum KeyBoundary {
    /// Included key boudary.
    Included(SeekKey),
    /// Excluded key boudary.
    Excluded(SeekKey),
    /// Include min key, only used as start key boundary.
    MinIncluded,
    /// Include max key, only used as end key boundary.
    MaxIncluded,
}

impl KeyBoundary {
    pub fn included(key: &[u8]) -> Self {
        Self::Included(key.into())
    }

    pub fn excluded(key: &[u8]) -> Self {
        Self::Excluded(key.into())
    }

    pub fn min_included() -> Self {
        Self::MinIncluded
    }

    pub fn max_included() -> Self {
        Self::MaxIncluded
    }
}

/// Context during scan.
#[derive(Debug, Clone)]
pub struct ScanContext {
    /// Timeout for a single scan operation of the scan iteator. Note that the
    /// scan iterator continuouslly send scan request to remote server to
    /// fetch data of next key range, and this timeout is applied to every
    /// send request, instead of the whole iteration. So user can hold this
    /// iterator more longer than the `timeout`.
    pub timeout: Duration,
    /// Batch size of a single scan operation.
    pub batch_size: i32,
}

impl ScanContext {
    /// Default scan batch size.
    pub const DEFAULT_BATCH_SIZE: i32 = 100;
}

impl Default for ScanContext {
    fn default() -> Self {
        Self {
            timeout: Duration::from_secs(5),
            batch_size: Self::DEFAULT_BATCH_SIZE,
        }
    }
}

/// Scan request.
#[derive(Debug, Clone)]
pub struct ScanRequest {
    /// Start bound.
    pub start: KeyBoundary,
    /// End bound.
    pub end: KeyBoundary,
    /// Scan in reverse order if `reverse` is set to true.
    pub reverse: bool,
}

/// Iterator to the scan result.
pub trait ScanIter: fmt::Debug {
    type Error: std::error::Error + Send + Sync + 'static;

    /// Returns true if the iterator is valid.
    fn valid(&self) -> bool;

    /// Advance the iterator.
    ///
    /// Panic if the iterator is invalid.
    fn next(&mut self) -> Result<bool, Self::Error>;

    /// Get current key.
    ///
    /// Panic if iter is invalid.
    fn key(&self) -> &[u8];

    /// Get current value.
    ///
    /// Panic if iter is invalid.
    fn value(&self) -> &[u8];
}

/// Error of TableKv.
pub trait TableError: std::error::Error {
    /// Is it primary key duplicate error.
    fn is_primary_key_duplicate(&self) -> bool;
}

/// Kv service provided by a relational database.
pub trait TableKv: Clone + Send + Sync + fmt::Debug + 'static {
    type Error: TableError + Send + Sync + 'static;
    type WriteBatch: WriteBatch + Send;
    type ScanIter: ScanIter + Send;

    /// Returns true if table with `table_name` already exists.
    fn table_exists(&self, table_name: &str) -> Result<bool, Self::Error>;

    /// Create table with given `table_name` if it is not exist.
    fn create_table(&self, table_name: &str) -> Result<(), Self::Error>;

    /// Drop table with given `table_name`.
    fn drop_table(&self, table_name: &str) -> Result<(), Self::Error>;

    /// Truncate table with given `table_name`.
    fn truncate_table(&self, table_name: &str) -> Result<(), Self::Error>;

    /// Write data in `write_batch` to table with `table_name`.
    fn write(
        &self,
        ctx: WriteContext,
        table_name: &str,
        write_batch: Self::WriteBatch,
    ) -> Result<(), Self::Error>;

    /// Scan data in given `table_name`, returns a [ScanIter].
    fn scan(
        &self,
        ctx: ScanContext,
        table_name: &str,
        request: ScanRequest,
    ) -> Result<Self::ScanIter, Self::Error>;

    /// Get value by key from table with `table_name`.
    fn get(&self, table_name: &str, key: &[u8]) -> Result<Option<Vec<u8>>, Self::Error>;

    /// Delete data by key from table with `table_name`.
    fn delete(&self, table_name: &str, key: &[u8]) -> Result<i64, Self::Error>;
}
