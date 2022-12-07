// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

use std::{fmt::Display, ops::Range};

use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::BoxStream;
use tokio::io::AsyncWrite;
use upstream::{
    path::{Path, DELIMITER},
    GetResult, ListResult, MultipartId, ObjectMeta, ObjectStore, Result,
};

use crate::ObjectStoreRef;

/// Wrap a real store and hijack all operations by adding the specific prefix to
/// the target location.
#[derive(Debug)]
pub struct StoreWithPrefix {
    store: ObjectStoreRef,
    prefix: Path,
}

impl Display for StoreWithPrefix {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Store with prefix, underlying store:{}, prefix path:{:?}",
            self.store, self.prefix,
        )
    }
}

impl StoreWithPrefix {
    pub fn new(prefix: String, store: ObjectStoreRef) -> Result<Self> {
        let prefix = Path::parse(prefix)?;
        Ok(Self { store, prefix })
    }

    fn loc_with_prefix(&self, loc: &Path) -> Path {
        let splitted_prefix = self.prefix.as_ref().split(DELIMITER).into_iter();
        let splitted_loc = loc.as_ref().split(DELIMITER).into_iter();
        Path::from_iter(splitted_prefix.chain(splitted_loc))
    }
}

#[async_trait]
impl ObjectStore for StoreWithPrefix {
    async fn put(&self, location: &Path, bytes: Bytes) -> Result<()> {
        let new_loc = self.loc_with_prefix(location);
        self.store.put(&new_loc, bytes).await
    }

    async fn put_multipart(
        &self,
        location: &Path,
    ) -> Result<(MultipartId, Box<dyn AsyncWrite + Unpin + Send>)> {
        let new_loc = self.loc_with_prefix(location);
        self.store.put_multipart(&new_loc).await
    }

    async fn abort_multipart(&self, location: &Path, multipart_id: &MultipartId) -> Result<()> {
        let new_loc = self.loc_with_prefix(location);
        self.store.abort_multipart(&new_loc, multipart_id).await
    }

    async fn get(&self, location: &Path) -> Result<GetResult> {
        let new_loc = self.loc_with_prefix(location);
        self.store.get(&new_loc).await
    }

    async fn get_range(&self, location: &Path, range: Range<usize>) -> Result<Bytes> {
        let new_loc = self.loc_with_prefix(location);
        self.store.get_range(&new_loc, range).await
    }

    async fn get_ranges(&self, location: &Path, ranges: &[Range<usize>]) -> Result<Vec<Bytes>> {
        let new_loc = self.loc_with_prefix(location);
        self.store.get_ranges(&new_loc, ranges).await
    }

    /// Return the metadata for the specified location
    async fn head(&self, location: &Path) -> Result<ObjectMeta> {
        let new_loc = self.loc_with_prefix(location);
        self.store.head(&new_loc).await
    }

    /// Delete the object at the specified location.
    async fn delete(&self, location: &Path) -> Result<()> {
        let new_loc = self.loc_with_prefix(location);
        self.store.delete(&new_loc).await
    }

    async fn list(&self, prefix: Option<&Path>) -> Result<BoxStream<'_, Result<ObjectMeta>>> {
        if let Some(loc) = prefix {
            let new_loc = self.loc_with_prefix(loc);
            self.store.list(Some(&new_loc)).await
        } else {
            self.store.list(Some(&self.prefix)).await
        }
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> Result<ListResult> {
        if let Some(loc) = prefix {
            let new_loc = self.loc_with_prefix(loc);
            self.store.list_with_delimiter(Some(&new_loc)).await
        } else {
            self.store.list_with_delimiter(Some(&self.prefix)).await
        }
    }

    async fn copy(&self, from: &Path, to: &Path) -> Result<()> {
        let new_from = self.loc_with_prefix(from);
        let new_to = self.loc_with_prefix(to);
        self.store.copy(&new_from, &new_to).await
    }

    async fn copy_if_not_exists(&self, from: &Path, to: &Path) -> Result<()> {
        let new_from = self.loc_with_prefix(from);
        let new_to = self.loc_with_prefix(to);
        self.store.copy(&new_from, &new_to).await
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use tempfile::tempdir;
    use upstream::local::LocalFileSystem;

    use super::*;

    #[test]
    fn test_prefix() {
        let cases = vec![
            ("", "100/101.sst", "100/101.sst"),
            ("0", "100/101.sst", "0/100/101.sst"),
            ("0/1", "100/101.sst", "0/1/100/101.sst"),
            ("/0/1", "100/101.sst", "0/1/100/101.sst"),
            ("/0/1/", "100/101.sst", "0/1/100/101.sst"),
        ];

        let local_path = tempdir().unwrap();
        let local_store = Arc::new(LocalFileSystem::new_with_prefix(local_path.path()).unwrap());
        for (prefix, filename, expect_loc) in cases {
            let prefix_store =
                StoreWithPrefix::new(prefix.to_string(), local_store.clone()).unwrap();
            let real_loc = prefix_store.loc_with_prefix(&Path::from(filename));
            assert_eq!(expect_loc, real_loc.as_ref(), "prefix:{}", prefix);
        }
    }
}
