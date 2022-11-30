// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

//! An ObjectStore implementation with disk as cache.
//! The disk cache is a read-through caching, with page as its minimal cache
//! unit.
//!
//! Page is used for reasons below:
//! - reduce file size in case of there are too many request with small range.

use std::{collections::BTreeMap, fmt::Display, fs, ops::Range, sync::Arc};

use async_trait::async_trait;
use bytes::{Bytes, BytesMut};
use futures::stream::BoxStream;
use log::{error, info};
use lru::LruCache;
use prost::Message;
use snafu::{Backtrace, ResultExt, Snafu};
use tokio::{
    fs::File,
    io::{AsyncReadExt, AsyncWrite, AsyncWriteExt},
    sync::Mutex,
};
use upstream::{
    path::Path, Error as ObjectStoreError, GetResult, ListResult, MultipartId, ObjectMeta,
    ObjectStore, Result,
};

#[derive(Debug, Snafu)]
enum Error {
    #[snafu(display(
        "IO failed, file:{}, source:{}.\nbacktrace:\n{}",
        file,
        source,
        backtrace
    ))]
    Io {
        file: String,
        source: std::io::Error,
        backtrace: Backtrace,
    },

    #[snafu(display(
        "Failed persist cache, file:{}, source:{}.\nbacktrace:\n{}",
        file,
        source,
        backtrace
    ))]
    PersistCache {
        file: String,
        source: tokio::io::Error,
        backtrace: Backtrace,
    },

    #[snafu(display(
        "Failed to decode prost, file:{}, source:{}.\nbacktrace:\n{}",
        file,
        source,
        backtrace
    ))]
    DecodeCache {
        file: String,
        source: prost::DecodeError,
        backtrace: Backtrace,
    },
}

impl From<Error> for ObjectStoreError {
    fn from(source: Error) -> Self {
        Self::Generic {
            store: "DiskCacheStore",
            source: Box::new(source),
        }
    }
}

struct CachedBytes {
    /// file where bytes is saved on disk
    file_path: String,
}

impl Drop for CachedBytes {
    fn drop(&mut self) {
        info!("Remove disk cache, key:{}", &self.file_path);

        if let Err(e) = fs::remove_file(&self.file_path) {
            error!(
                "Remove disk cache failed, key:{}, err:{}",
                self.file_path, e
            );
        }
    }
}

impl CachedBytes {
    fn new(file_path: String) -> Self {
        Self { file_path }
    }

    async fn persist(&self, value: Bytes) -> Result<()> {
        let mut f = File::create(&self.file_path).await.with_context(|| Io {
            file: self.file_path.clone(),
        })?;

        let pb_bytes = proto::cache::Bytes {
            key: "".to_string(),
            value: value.to_vec(),
        };

        let bs = pb_bytes.encode_to_vec();

        f.write_all(&bs).await.with_context(|| PersistCache {
            file: self.file_path.clone(),
        })?;

        Ok(())
    }

    async fn to_bytes(&self) -> Result<Bytes> {
        let mut f = File::open(&self.file_path).await.with_context(|| Io {
            file: self.file_path.clone(),
        })?;
        let mut buf = Vec::new();
        f.read_to_end(&mut buf).await.with_context(|| Io {
            file: self.file_path.clone(),
        })?;

        let bytes = proto::cache::Bytes::decode(&*buf).with_context(|| DecodeCache {
            file: self.file_path.clone(),
        })?;

        Ok(bytes.value.into())
    }
}

#[derive(Debug)]
struct DiskCache {
    root_dir: String,
    cache: Mutex<LruCache<String, CachedBytes>>,
}

impl DiskCache {
    fn new(root_dir: String, cap: usize) -> Self {
        Self {
            root_dir,
            cache: Mutex::new(LruCache::new(cap)),
        }
    }

    async fn insert(&self, key: String, value: Bytes) -> Result<()> {
        let mut cache = self.cache.lock().await;
        let file_path = std::path::Path::new(&self.root_dir)
            .join(&key)
            .into_os_string()
            .into_string()
            .unwrap();
        let bytes = CachedBytes::new(file_path);
        bytes.persist(value).await?;
        cache.push(key, bytes);

        Ok(())
    }

    async fn get(&self, key: &str) -> Result<Option<Bytes>> {
        let mut cache = self.cache.lock().await;
        if let Some(cached_bytes) = cache.get(key) {
            // TODO: release lock when doing IO
            return cached_bytes.to_bytes().await.map(Some);
        }

        Ok(None)
    }
}

impl Display for DiskCache {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DiskCache")
            .field("path", &self.root_dir)
            .field("cache", &self.cache)
            .finish()
    }
}

#[derive(Debug)]
pub struct DiskCacheStore {
    cache: DiskCache,
    // Max disk capacity cache use can
    cap: usize,
    page_size: usize,
    size_cache: Arc<Mutex<LruCache<String, usize>>>,
    underlying_store: Arc<dyn ObjectStore>,
}

impl DiskCacheStore {
    #[allow(dead_code)]
    pub fn new(
        root_dir: String,
        cap: usize,
        page_size: usize,
        underlying_store: Arc<dyn ObjectStore>,
    ) -> Self {
        let cache = DiskCache::new(root_dir, cap);
        let meta_cache = Arc::new(Mutex::new(LruCache::new(cap / page_size)));

        Self {
            cache,
            size_cache: meta_cache,
            cap,
            page_size,
            underlying_store,
        }
    }

    fn normalize_range(&self, max_size: usize, range: &Range<usize>) -> Vec<Range<usize>> {
        let start = range.start / self.page_size * self.page_size;
        let end = (range.end + self.page_size - 1) / self.page_size * self.page_size;

        (start..end.min(max_size))
            .step_by(self.page_size)
            .map(|start| start..(start + self.page_size).min(max_size))
            .collect::<Vec<_>>()
    }

    fn cache_key(location: &Path, range: &Range<usize>) -> String {
        format!(
            "{}-{}-{}",
            location.as_ref().replace('/', "-"),
            range.start,
            range.end
        )
    }
}

impl Display for DiskCacheStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DiskCacheStore")
            .field("page_size", &self.page_size)
            .field("cap", &self.cap)
            .field("cache", &self.cache)
            .finish()
    }
}

#[async_trait]
impl ObjectStore for DiskCacheStore {
    async fn put(&self, location: &Path, bytes: Bytes) -> Result<()> {
        self.underlying_store.put(location, bytes).await
    }

    async fn put_multipart(
        &self,
        location: &Path,
    ) -> Result<(MultipartId, Box<dyn AsyncWrite + Unpin + Send>)> {
        self.underlying_store.put_multipart(location).await
    }

    async fn abort_multipart(&self, location: &Path, multipart_id: &MultipartId) -> Result<()> {
        self.underlying_store
            .abort_multipart(location, multipart_id)
            .await
    }

    // TODO(chenxiang): don't cache whole path for reasons below
    // In sst module, we only use get_range, get is not used
    async fn get(&self, location: &Path) -> Result<GetResult> {
        self.underlying_store.get(location).await
    }

    async fn get_range(&self, location: &Path, range: Range<usize>) -> Result<Bytes> {
        // TODO: aligned_range will larger than real file size, need to truncate
        let file_size = {
            let mut size_cache = self.size_cache.lock().await;
            if let Some(size) = size_cache.get(location.as_ref()) {
                *size
            } else {
                // release lock before doing IO
                drop(size_cache);

                // TODO: multiple threads may go here, how to fix?
                let object_meta = self.head(location).await?;
                {
                    let mut size_cache = self.size_cache.lock().await;
                    size_cache.put(location.to_string(), object_meta.size);
                }
                object_meta.size
            }
        };

        let aligned_ranges = self.normalize_range(file_size, &range);

        let mut ranged_bytes = BTreeMap::new();
        let mut missing_ranges = Vec::new();
        for range in aligned_ranges {
            let cache_key = Self::cache_key(location, &range);
            if let Some(bytes) = self.cache.get(&cache_key).await? {
                ranged_bytes.insert(range.start, bytes);
            } else {
                missing_ranges.push(range);
            }
        }

        for range in missing_ranges {
            let range_start = range.start;
            let cache_key = Self::cache_key(location, &range);
            let bytes = self.underlying_store.get_range(location, range).await?;
            self.cache.insert(cache_key, bytes.clone()).await?;
            ranged_bytes.insert(range_start, bytes);
        }

        // we get all bytes for each aligned_range, organize real bytes

        // fast path
        if ranged_bytes.len() == 1 {
            let (range_start, bytes) = ranged_bytes.pop_first().unwrap();
            let result = bytes.slice((range.start - range_start)..(range.end - range_start));
            return Ok(result);
        }

        // there are multiple aligned ranges for one request, such as
        // range = [3, 33), page_size = 16, then aligned ranges will be
        // [0, 16), [16, 32), [32, 48)
        // we need to combine those ranged bytes to get final result bytes

        let mut byte_buf = BytesMut::with_capacity(range.end - range.start);
        let (range_start, bytes) = ranged_bytes.pop_first().unwrap();
        byte_buf.extend(bytes.slice((range.start - range_start)..));
        let (range_start, bytes) = ranged_bytes.pop_last().unwrap();
        let last_part = bytes.slice(..(range.end - range_start));

        for bytes in ranged_bytes.into_values() {
            byte_buf.extend(bytes);
        }
        byte_buf.extend(last_part);

        Ok(byte_buf.freeze())
    }

    async fn head(&self, location: &Path) -> Result<ObjectMeta> {
        self.underlying_store.head(location).await
    }

    async fn delete(&self, location: &Path) -> Result<()> {
        self.underlying_store.delete(location).await
    }

    async fn list(&self, prefix: Option<&Path>) -> Result<BoxStream<'_, Result<ObjectMeta>>> {
        self.underlying_store.list(prefix).await
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> Result<ListResult> {
        self.underlying_store.list_with_delimiter(prefix).await
    }

    async fn copy(&self, from: &Path, to: &Path) -> Result<()> {
        self.underlying_store.copy(from, to).await
    }

    async fn copy_if_not_exists(&self, from: &Path, to: &Path) -> Result<()> {
        self.underlying_store.copy_if_not_exists(from, to).await
    }
}

#[cfg(test)]
mod test {
    use tempfile::tempdir;
    use upstream::local::LocalFileSystem;

    use super::*;

    fn prepare_store(page_size: usize, cap: usize) -> DiskCacheStore {
        let local_path = tempdir().unwrap();
        let local_store = Arc::new(LocalFileSystem::new_with_prefix(local_path.path()).unwrap());

        DiskCacheStore::new("/tmp".to_string(), cap, page_size, local_store)
    }

    #[test]
    fn test_normalize_range_less_than_file_size() {
        let page_size = 16;
        let testcases = vec![
            (0..1, vec![0..16]),
            (0..16, vec![0..16]),
            (0..17, vec![0..16, 16..32]),
            (16..32, vec![16..32]),
            (
                16..100,
                vec![16..32, 32..48, 48..64, 64..80, 80..96, 96..112],
            ),
        ];

        let store = prepare_store(page_size, 1000);
        for (input, expected) in testcases {
            assert_eq!(store.normalize_range(1024, &input), expected);
        }
    }

    #[test]
    fn test_normalize_range_great_than_file_size() {
        let page_size = 16;
        let testcases = vec![
            (0..1, vec![0..16]),
            (0..16, vec![0..16]),
            (0..17, vec![0..16, 16..20]),
            (16..32, vec![16..20]),
            (32..100, vec![]),
        ];

        let store = prepare_store(page_size, 1000);
        for (input, expected) in testcases {
            assert_eq!(store.normalize_range(20, &input), expected);
        }
    }

    #[tokio::test]
    async fn test_disk_cache_store_get_range() {
        let page_size = 16;
        // 51 byte
        let data = b"a b c d e f g h i j k l m n o p q r s t u v w x y z";
        let location = Path::from("1.sst");

        let store = prepare_store(page_size, 1000);
        let mut buf = BytesMut::with_capacity(data.len() * 4);
        // put 4 times, then location will be 200 bytes
        for _ in 0..4 {
            buf.extend_from_slice(data);
        }
        store.put(&location, buf.freeze()).await.unwrap();

        let testcases = vec![
            (0..6, "a b c "),
            (0..16, "a b c d e f g h "),
            // len of aligned ranges will be 2
            (0..17, "a b c d e f g h i"),
            (16..17, "i"),
            // len of aligned ranges will be 6
            (16..100, "i j k l m n o p q r s t u v w x y za b c d e f g h i j k l m n o p q r s t u v w x y"),
        ];

        for (input, expected) in testcases {
            assert_eq!(
                store.get_range(&location, input).await.unwrap(),
                Bytes::copy_from_slice(expected.as_bytes())
            );
        }

        // remove cached values, then get again
        {
            let mut data_cache = store.cache.cache.lock().await;
            for range in vec![0..16, 16..32, 32..48, 48..64, 64..80, 80..96, 96..112] {
                assert!(data_cache.contains(DiskCacheStore::cache_key(&location, &range).as_str()));
            }

            assert!(data_cache
                .pop(&DiskCacheStore::cache_key(&location, &(16..32)))
                .is_some());
            assert!(data_cache
                .pop(&DiskCacheStore::cache_key(&location, &(48..64)))
                .is_some());
            assert!(data_cache
                .pop(&DiskCacheStore::cache_key(&location, &(80..96)))
                .is_some());
        }

        assert_eq!(
            store.get_range(&location, 16..100).await.unwrap(),
            Bytes::copy_from_slice(
                b"i j k l m n o p q r s t u v w x y za b c d e f g h i j k l m n o p q r s t u v w x y"
            )
        );
    }
}
