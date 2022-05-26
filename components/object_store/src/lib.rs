// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

//! # object_store
//!
//! This crate provides APIs for interacting with object storage services. It
//! currently supports PUT, GET, DELETE, and list for in-memory and
//! local file storage.
//!
//! Future compatibility will include Aliyun OSS.
//!
//! Fork from https://github.com/influxdata/influxdb_iox/tree/main/object_store

use std::time::SystemTime;

use async_trait::async_trait;
use futures::{stream::BoxStream, AsyncRead};
use path::ObjectStorePath;

pub mod disk;
pub mod path;

/// Universal API to multiple object store services.
// TODO(xikai): ObjectStore -> FileStore
#[async_trait]
pub trait ObjectStore: std::fmt::Debug + Send + Sync + 'static {
    /// The type of the locations used in interacting with this object store.
    type Path: ObjectStorePath;

    /// The error returned from fallible methods
    type Error: std::error::Error + Send + Sync + 'static;

    type Reader: AsyncRead + Send + Unpin;

    /// Return a new location path appropriate for this object storage
    fn new_path(&self) -> Self::Path;

    /// Save the provided bytes to the specified location.
    async fn put<R>(
        &self,
        location: &Self::Path,
        bytes: R,
        length: Option<usize>,
    ) -> Result<(), Self::Error>
    where
        R: AsyncRead + Send + Unpin;

    /// Return the bytes that are stored at the specified location.
    async fn get(&self, location: &Self::Path) -> Result<std::fs::File, Self::Error>;

    /// Delete the object at the specified location.
    async fn delete(&self, location: &Self::Path) -> Result<(), Self::Error>;

    /// List all the objects with the given prefix.
    async fn list<'a>(
        &'a self,
        prefix: Option<&'a Self::Path>,
    ) -> Result<BoxStream<'a, Result<Vec<Self::Path>, Self::Error>>, Self::Error>;

    /// List objects with the given prefix and an implementation specific
    /// delimiter. Returns common prefixes (directories) in addition to object
    /// metadata.
    async fn list_with_delimiter(
        &self,
        prefix: &Self::Path,
    ) -> Result<ListResult<Self::Path>, Self::Error>;
}

/// Result of a list call that includes objects, prefixes (directories) and a
/// token for the next set of results. Individual result sets may be limited to
/// 1,00 objects based on the underlying object storage's limitations.
#[derive(Debug)]
pub struct ListResult<P: ObjectStorePath> {
    /// Token passed to the API for the next page of list results.
    pub next_token: Option<String>,
    /// Prefixes that are common (like directories)
    pub common_prefixes: Vec<P>,
    /// Object metadata for the listing
    pub objects: Vec<ObjectMeta<P>>,
}

/// The metadata that describes an object.
#[derive(Debug)]
pub struct ObjectMeta<P: ObjectStorePath> {
    /// The full path to the object
    pub location: P,
    /// The last modified time
    pub last_modified: SystemTime,
    /// The size in bytes of the object
    pub size: usize,
}

#[cfg(test)]
mod tests {
    use std::io::Read;

    use bytes::Bytes;
    use futures::{stream, StreamExt, TryStreamExt};

    use super::*;
    use crate::path::{file::FilePath, parsed::DirsAndFileName};

    type Error = Box<dyn std::error::Error + Send + Sync + 'static>;
    type Result<T, E = Error> = std::result::Result<T, E>;

    async fn flatten_list_stream<
        P: path::ObjectStorePath,
        E: std::error::Error + Send + Sync + 'static,
        R: AsyncRead + Unpin,
    >(
        storage: &impl ObjectStore<Path = P, Error = E, Reader = R>,
        prefix: Option<&P>,
    ) -> Result<Vec<P>> {
        storage
            .list(prefix)
            .await?
            .map_ok(|v| stream::iter(v).map(Ok))
            .try_flatten()
            .try_collect()
            .await
    }

    pub(crate) async fn put_get_delete_list<
        P: path::ObjectStorePath,
        E: std::error::Error + Send + Sync + 'static,
        R: AsyncRead + Unpin,
    >(
        storage: &impl ObjectStore<Path = P, Error = E, Reader = R>,
    ) -> Result<()> {
        delete_fixtures(storage).await;

        let content_list = flatten_list_stream(storage, None).await?;
        assert!(
            content_list.is_empty(),
            "Expected list to be empty; found: {:?}",
            content_list
        );

        let data = Bytes::from("arbitrary data");
        let mut location = storage.new_path();
        location.push_dir("test_dir");
        location.set_file_name("test_file.json");

        storage
            .put(&location, data.as_ref(), Some(data.len()))
            .await?;

        // List everything
        let content_list = flatten_list_stream(storage, None).await?;
        assert_eq!(content_list, &[location.clone()]);

        // List everything starting with a prefix that should return results
        let mut prefix = storage.new_path();
        prefix.push_dir("test_dir");
        let content_list = flatten_list_stream(storage, Some(&prefix)).await?;
        assert_eq!(content_list, &[location.clone()]);

        // List everything starting with a prefix that shouldn't return results
        let mut prefix = storage.new_path();
        prefix.push_dir("something");
        let content_list = flatten_list_stream(storage, Some(&prefix)).await?;
        assert!(content_list.is_empty());

        let mut read_data = Vec::with_capacity(data.len());

        storage.get(&location).await?.read_to_end(&mut read_data)?;
        assert_eq!(&*read_data, data);

        storage.delete(&location).await?;

        let content_list = flatten_list_stream(storage, None).await?;
        assert!(content_list.is_empty());

        Ok(())
    }

    pub(crate) async fn list_with_delimiter<
        P: path::ObjectStorePath,
        E: std::error::Error + Send + Sync + 'static,
        R: AsyncRead + Unpin,
    >(
        storage: &impl ObjectStore<Path = P, Error = E, Reader = R>,
    ) -> Result<()> {
        delete_fixtures(storage).await;

        // ==================== check: store is empty ====================
        let content_list = flatten_list_stream(storage, None).await?;
        assert!(content_list.is_empty());

        // ==================== do: create files ====================
        let data = Bytes::from("arbitrary data");

        let files: Vec<_> = [
            "test_file",
            "mydb/wb/000/000/000.segment",
            "mydb/wb/000/000/001.segment",
            "mydb/wb/000/000/002.segment",
            "mydb/wb/001/001/000.segment",
            "mydb/wb/foo.json",
            "mydb/data/whatevs",
        ]
        .iter()
        .map(|&s| str_to_path(storage, s))
        .collect();

        for f in &files {
            storage
                .put(f, data.as_ref(), Some(data.len()))
                .await
                .unwrap();
        }

        // ==================== check: prefix-list `mydb/wb` (directory)
        // ====================
        let mut prefix = storage.new_path();
        prefix.push_all_dirs(&["mydb", "wb"]);

        let mut expected_000 = prefix.clone();
        expected_000.push_dir("000");
        let mut expected_001 = prefix.clone();
        expected_001.push_dir("001");
        let mut expected_location = prefix.clone();
        expected_location.set_file_name("foo.json");

        let result = storage.list_with_delimiter(&prefix).await.unwrap();

        assert_eq!(result.common_prefixes, vec![expected_000, expected_001]);
        assert_eq!(result.objects.len(), 1);

        let object = &result.objects[0];

        assert_eq!(object.location, expected_location);
        assert_eq!(object.size, data.len());

        // ==================== check: prefix-list `mydb/wb/000/000/001` (partial
        // filename) ====================
        let mut prefix = storage.new_path();
        prefix.push_all_dirs(&["mydb", "wb", "000", "000"]);
        prefix.set_file_name("001");

        let mut expected_location = storage.new_path();
        expected_location.push_all_dirs(&["mydb", "wb", "000", "000"]);
        expected_location.set_file_name("001.segment");

        let result = storage.list_with_delimiter(&prefix).await.unwrap();
        assert!(result.common_prefixes.is_empty());
        assert_eq!(result.objects.len(), 1);

        let object = &result.objects[0];

        assert_eq!(object.location, expected_location);

        // ==================== check: prefix-list `not_there` (non-existing prefix)
        // ====================
        let mut prefix = storage.new_path();
        prefix.push_all_dirs(&["not_there"]);

        let result = storage.list_with_delimiter(&prefix).await.unwrap();
        assert!(result.common_prefixes.is_empty());
        assert!(result.objects.is_empty());

        // ==================== do: remove all files ====================
        for f in &files {
            storage.delete(f).await.unwrap();
        }

        // ==================== check: store is empty ====================
        let content_list = flatten_list_stream(storage, None).await?;
        assert!(content_list.is_empty());

        Ok(())
    }

    /// Parse a str as a `CloudPath` into a `DirAndFileName`, even though the
    /// associated storage might not be cloud storage, to reuse the cloud
    /// path parsing logic. Then convert into the correct type of path for
    /// the given storage.
    fn str_to_path<
        P: path::ObjectStorePath,
        E: std::error::Error + Send + Sync,
        R: AsyncRead + Unpin,
    >(
        storage: &impl ObjectStore<Path = P, Error = E, Reader = R>,
        val: &str,
    ) -> P {
        let cloud_path = FilePath::raw(val, false);
        let parsed: DirsAndFileName = cloud_path.into();

        let mut new_path = storage.new_path();
        for part in parsed.directories {
            new_path.push_dir(part.to_string());
        }

        if let Some(file_name) = parsed.file_name {
            new_path.set_file_name(file_name.to_string());
        }
        new_path
    }

    async fn delete_fixtures<
        P: path::ObjectStorePath,
        E: std::error::Error + Send + Sync,
        R: AsyncRead + Unpin,
    >(
        storage: &impl ObjectStore<Path = P, Error = E, Reader = R>,
    ) {
        let files: Vec<_> = [
            "test_file",
            "mydb/wb/000/000/000.segment",
            "mydb/wb/000/000/001.segment",
            "mydb/wb/000/000/002.segment",
            "mydb/wb/001/001/000.segment",
            "mydb/wb/foo.json",
            "mydb/data/whatevs",
        ]
        .iter()
        .map(|&s| str_to_path(storage, s))
        .collect();

        for f in &files {
            // don't care if it errors, should fail elsewhere
            let _ = storage.delete(f).await;
        }
    }

    // Tests TODO:
    // GET nonexisting location (in_memory/file)
    // DELETE nonexisting location
    // PUT overwriting
}
