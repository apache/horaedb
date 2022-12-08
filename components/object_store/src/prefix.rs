// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

use std::{fmt::Display, ops::Range};

use async_trait::async_trait;
use bytes::Bytes;
use futures::{stream::BoxStream, StreamExt};
use tokio::io::AsyncWrite;
use upstream::{
    path::{self, Path, DELIMITER},
    Error, GetResult, ListResult, MultipartId, ObjectMeta, ObjectStore, Result,
};

use crate::ObjectStoreRef;

#[derive(Debug)]
struct ErrorWithMsg {
    msg: String,
}

impl std::error::Error for ErrorWithMsg {}

impl Display for ErrorWithMsg {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "StoreWithPrefix error, msg:{}", self.msg,)
    }
}

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

    fn add_prefix_to_loc(&self, loc: &Path) -> Path {
        if self.prefix.as_ref().is_empty() {
            return loc.clone();
        }

        let splitted_prefix = self.prefix.as_ref().split(DELIMITER);
        let splitted_loc = loc.as_ref().split(DELIMITER);
        Path::from_iter(splitted_prefix.chain(splitted_loc))
    }

    fn remove_prefix_from_loc(&self, loc: &Path) -> Result<Path> {
        if self.prefix.as_ref().is_empty() {
            return Ok(loc.clone());
        }

        let raw_prefix = self.prefix.as_ref();
        let raw_loc = loc.as_ref();
        match raw_loc.strip_prefix(raw_prefix) {
            Some(v) => Path::parse(v).map_err(|e| Error::InvalidPath { source: e }),
            None => Err(Error::InvalidPath {
                source: path::Error::PrefixMismatch {
                    path: raw_loc.to_string(),
                    prefix: raw_prefix.to_string(),
                },
            }),
        }
    }
}

#[async_trait]
impl ObjectStore for StoreWithPrefix {
    async fn put(&self, location: &Path, bytes: Bytes) -> Result<()> {
        let new_loc = self.add_prefix_to_loc(location);
        self.store.put(&new_loc, bytes).await
    }

    async fn put_multipart(
        &self,
        location: &Path,
    ) -> Result<(MultipartId, Box<dyn AsyncWrite + Unpin + Send>)> {
        let new_loc = self.add_prefix_to_loc(location);
        self.store.put_multipart(&new_loc).await
    }

    async fn abort_multipart(&self, location: &Path, multipart_id: &MultipartId) -> Result<()> {
        let new_loc = self.add_prefix_to_loc(location);
        self.store.abort_multipart(&new_loc, multipart_id).await
    }

    async fn get(&self, location: &Path) -> Result<GetResult> {
        let new_loc = self.add_prefix_to_loc(location);
        let res = self.store.get(&new_loc).await?;
        if let GetResult::File(_, _) = &res {
            let err = ErrorWithMsg {
                msg: "StoreWithPrefix doesn't support object store based on local file system"
                    .to_string(),
            };
            return Err(Error::NotSupported {
                source: Box::new(err),
            });
        }

        Ok(res)
    }

    async fn get_range(&self, location: &Path, range: Range<usize>) -> Result<Bytes> {
        let new_loc = self.add_prefix_to_loc(location);
        self.store.get_range(&new_loc, range).await
    }

    async fn get_ranges(&self, location: &Path, ranges: &[Range<usize>]) -> Result<Vec<Bytes>> {
        let new_loc = self.add_prefix_to_loc(location);
        self.store.get_ranges(&new_loc, ranges).await
    }

    /// Return the metadata for the specified location
    async fn head(&self, location: &Path) -> Result<ObjectMeta> {
        let new_loc = self.add_prefix_to_loc(location);
        let mut meta = self.store.head(&new_loc).await?;
        meta.location = self.remove_prefix_from_loc(&meta.location)?;
        Ok(meta)
    }

    /// Delete the object at the specified location.
    async fn delete(&self, location: &Path) -> Result<()> {
        let new_loc = self.add_prefix_to_loc(location);
        self.store.delete(&new_loc).await
    }

    async fn list(&self, prefix: Option<&Path>) -> Result<BoxStream<'_, Result<ObjectMeta>>> {
        let objects = if let Some(loc) = prefix {
            let new_loc = self.add_prefix_to_loc(loc);
            self.store.list(Some(&new_loc)).await?
        } else {
            self.store.list(Some(&self.prefix)).await?
        };

        let new_objects = objects.map(|mut obj| {
            if let Ok(v) = &mut obj {
                v.location = self.remove_prefix_from_loc(&v.location)?;
            }

            obj
        });
        Ok(new_objects.boxed())
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> Result<ListResult> {
        let mut list_res = if let Some(loc) = prefix {
            let new_loc = self.add_prefix_to_loc(loc);
            self.store.list_with_delimiter(Some(&new_loc)).await?
        } else {
            self.store.list_with_delimiter(Some(&self.prefix)).await?
        };

        for dir in &mut list_res.common_prefixes {
            *dir = self.remove_prefix_from_loc(dir)?;
        }

        for object in &mut list_res.objects {
            object.location = self.remove_prefix_from_loc(&object.location)?;
        }

        Ok(list_res)
    }

    async fn copy(&self, from: &Path, to: &Path) -> Result<()> {
        let new_from = self.add_prefix_to_loc(from);
        let new_to = self.add_prefix_to_loc(to);
        self.store.copy(&new_from, &new_to).await
    }

    async fn copy_if_not_exists(&self, from: &Path, to: &Path) -> Result<()> {
        let new_from = self.add_prefix_to_loc(from);
        let new_to = self.add_prefix_to_loc(to);
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
        for (prefix, filename, expect_loc) in cases.clone() {
            let prefix_store =
                StoreWithPrefix::new(prefix.to_string(), local_store.clone()).unwrap();
            let real_loc = prefix_store.add_prefix_to_loc(&Path::from(filename));
            assert_eq!(expect_loc, real_loc.as_ref(), "prefix:{}", prefix);
        }

        for (prefix, expect_filename, loc) in cases {
            let prefix_store =
                StoreWithPrefix::new(prefix.to_string(), local_store.clone()).unwrap();
            let real_filename = prefix_store
                .remove_prefix_from_loc(&Path::from(loc))
                .unwrap();
            assert_eq!(expect_filename, real_filename.as_ref(), "prefix:{}", prefix);
        }
    }
}
