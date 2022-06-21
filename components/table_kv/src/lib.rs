//! Remote key-value api based on table.

use std::time::Duration;

mod config;
pub mod obkv;

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
pub enum SeekKey {
    /// Min key.
    Min,
    /// Max key.
    Max,
    /// Key with given byte values.
    Bytes(Vec<u8>),
}

impl From<&[u8]> for SeekKey {
    fn from(key: &[u8]) -> Self {
        Self::Bytes(key.to_vec())
    }
}

/// Boundary of key to seek.
#[derive(Debug, Clone)]
pub enum KeyBoundary {
    /// Included key boudary.
    Included(SeekKey),
    /// Excluded key boudary.
    Excluded(SeekKey),
}

impl KeyBoundary {
    pub fn included(key: &[u8]) -> Self {
        Self::Included(SeekKey::from(key))
    }

    pub fn excluded(key: &[u8]) -> Self {
        Self::Excluded(SeekKey::from(key))
    }

    pub fn min_included() -> Self {
        Self::Included(SeekKey::Min)
    }

    pub fn max_included() -> Self {
        Self::Included(SeekKey::Max)
    }
}

/// Context during scan.
pub struct ScanContext {
    pub timeout: Duration,
    pub batch_size: i32,
}

impl Default for ScanContext {
    fn default() -> Self {
        Self {
            timeout: Duration::from_secs(10),
            batch_size: 100,
        }
    }
}

/// Scan request.
#[derive(Clone)]
pub struct ScanRequest {
    /// Start bound.
    pub start: KeyBoundary,
    /// End bound.
    pub end: KeyBoundary,
    /// Scan in reverse order if `reverse` is set to true.
    pub reverse: bool,
}

/// Iterator to the scan result.
pub trait ScanIter {
    type Error;

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

/// Kv service provided by a relational database.
pub trait TableKv {
    type Error;
    type WriteBatch: WriteBatch;
    type ScanIter: ScanIter;

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
}
