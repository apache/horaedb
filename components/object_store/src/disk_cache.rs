// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

//! An ObjectStore implementation with disk as cache.
//! The disk cache is a read-through caching, with page as its minimal cache
//! unit.
//!
//! Page is used for reasons below:
//! - reduce file size in case of there are too many request with small range.

use std::{collections::BTreeMap, fmt::Display, ops::Range, sync::Arc};

use async_trait::async_trait;
use bytes::{Bytes, BytesMut};
use common_util::time::current_as_rfc3339;
use futures::stream::BoxStream;
use log::{debug, error, info};
use lru::LruCache;
use prost::Message;
use serde::{Deserialize, Serialize};
use snafu::{ensure, Backtrace, ResultExt, Snafu};
use tokio::{
    fs::{File, OpenOptions},
    io::{AsyncReadExt, AsyncWrite, AsyncWriteExt},
    sync::Mutex,
};
use upstream::{
    path::Path, Error as ObjectStoreError, GetResult, ListResult, MultipartId, ObjectMeta,
    ObjectStore, Result,
};

const MANIFEST_FILE: &str = "manifest.json";

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
        "Failed to deserialize manifest, source:{}.\nbacktrace:\n{}",
        source,
        backtrace
    ))]
    DeserializeManifest {
        source: serde_json::Error,
        backtrace: Backtrace,
    },

    #[snafu(display(
        "Failed to serialize manifest, source:{}.\nbacktrace:\n{}",
        source,
        backtrace
    ))]
    SerializeManifest {
        source: serde_json::Error,
        backtrace: Backtrace,
    },

    #[snafu(display("Invalid manifest page size, old:{}, new:{}.", old, new))]
    InvalidManifest { old: usize, new: usize },

    #[snafu(display(
        "Failed to persist cache, file:{}, source:{}.\nbacktrace:\n{}",
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

#[derive(Serialize, Deserialize)]
struct Manifest {
    create_at: String,
    page_size: usize,
}

#[derive(Debug)]
struct DiskCache {
    root_dir: String,
    cap: usize,
    // cache key is use as filename on disk
    cache: Mutex<LruCache<String, ()>>,
}

impl DiskCache {
    fn new(root_dir: String, cap: usize) -> Self {
        Self {
            root_dir,
            cap,
            cache: Mutex::new(LruCache::new(cap)),
        }
    }

    // TODO: We now hold lock when doing IO, possible to release it?
    // cache key is also used as filename stored in local disk
    async fn update_cache(&self, key: String, value: Option<Bytes>) -> Result<()> {
        let mut cache = self.cache.lock().await;
        debug!("key:{}, len:{}, cap:{}", &key, cache.len(), self.cap);

        if cache.len() >= self.cap {
            let (filename, _) = cache.pop_lru().unwrap();
            let file_path = std::path::Path::new(&self.root_dir)
                .join(filename)
                .into_os_string()
                .into_string()
                .unwrap();

            info!("Remove disk cache, filename:{}", &file_path);
            if let Err(e) = tokio::fs::remove_file(&file_path).await {
                error!("Remove disk cache failed, file:{}, err:{}", file_path, e);
            }
        }

        if let Some(value) = value {
            self.persist_bytes(&key, value).await?;
        }
        cache.push(key, ());

        Ok(())
    }

    async fn insert(&self, key: String, value: Bytes) -> Result<()> {
        self.update_cache(key, Some(value)).await
    }

    async fn recover(&self, filename: String) -> Result<()> {
        self.update_cache(filename, None).await
    }

    async fn get(&self, key: &str) -> Result<Option<Bytes>> {
        let mut cache = self.cache.lock().await;
        if cache.get(key).is_some() {
            // TODO: release lock when doing IO
            return self.read_bytes(key).await.map(Some);
        }

        Ok(None)
    }

    async fn persist_bytes(&self, filename: &str, value: Bytes) -> Result<()> {
        let file_path = std::path::Path::new(&self.root_dir)
            .join(filename)
            .into_os_string()
            .into_string()
            .unwrap();

        let mut f = File::create(&file_path).await.with_context(|| Io {
            file: file_path.clone(),
        })?;

        let pb_bytes = proto::cache::Bytes {
            // TODO: CRC checking
            crc: 0,
            value: value.to_vec(),
        };

        let bs = pb_bytes.encode_to_vec();
        f.write_all(&bs).await.with_context(|| PersistCache {
            file: file_path.clone(),
        })?;

        Ok(())
    }

    async fn read_bytes(&self, filename: &str) -> Result<Bytes> {
        let file_path = std::path::Path::new(&self.root_dir)
            .join(filename)
            .into_os_string()
            .into_string()
            .unwrap();

        let mut f = File::open(&file_path).await.with_context(|| Io {
            file: file_path.clone(),
        })?;
        let mut buf = Vec::new();
        f.read_to_end(&mut buf).await.with_context(|| Io {
            file: file_path.clone(),
        })?;

        let bytes = proto::cache::Bytes::decode(&*buf).with_context(|| DecodeCache {
            file: file_path.clone(),
        })?;

        Ok(bytes.value.into())
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

// TODO: support partition to reduce lock contention
#[derive(Debug)]
pub struct DiskCacheStore {
    cache: DiskCache,
    // Max disk capacity cache use can
    cap: usize,
    page_size: usize,
    // location path size cache
    size_cache: Arc<Mutex<LruCache<String, usize>>>,
    underlying_store: Arc<dyn ObjectStore>,
}

impl DiskCacheStore {
    #[allow(dead_code)]
    pub async fn try_new(
        cache_dir: String,
        cap: usize,
        page_size: usize,
        underlying_store: Arc<dyn ObjectStore>,
    ) -> Result<Self> {
        Self::create_manifest_if_not_exists(&cache_dir, page_size).await?;

        let cache = DiskCache::new(cache_dir.clone(), cap / page_size);
        Self::recover_cache(&cache_dir, &cache).await?;

        let size_cache = Arc::new(Mutex::new(LruCache::new(cap / page_size)));

        Ok(Self {
            cache,
            size_cache,
            cap,
            page_size,
            underlying_store,
        })
    }

    async fn create_manifest_if_not_exists(cache_dir: &str, page_size: usize) -> Result<()> {
        let mut file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(false)
            .open(std::path::Path::new(cache_dir).join(MANIFEST_FILE))
            .await
            .with_context(|| Io {
                file: MANIFEST_FILE.to_string(),
            })?;

        let metadata = file.metadata().await.with_context(|| Io {
            file: MANIFEST_FILE.to_string(),
        })?;

        // empty file, create a new one
        if metadata.len() == 0 {
            let manifest = Manifest {
                page_size,
                create_at: current_as_rfc3339(),
            };

            let buf = serde_json::to_vec_pretty(&manifest).context(SerializeManifest)?;
            file.write_all(&buf).await.with_context(|| Io {
                file: MANIFEST_FILE.to_string(),
            })?;

            return Ok(());
        }

        let mut buf = Vec::new();
        file.read_to_end(&mut buf).await.with_context(|| Io {
            file: MANIFEST_FILE.to_string(),
        })?;

        let manifest: Manifest = serde_json::from_slice(&buf).context(DeserializeManifest)?;

        ensure!(
            manifest.page_size == page_size,
            InvalidManifest {
                old: manifest.page_size,
                new: page_size
            }
        );

        Ok(())
    }

    async fn recover_cache(cache_dir: &str, cache: &DiskCache) -> Result<()> {
        let mut cache_dir = tokio::fs::read_dir(cache_dir).await.with_context(|| Io {
            file: cache_dir.to_string(),
        })?;

        // TODO: sort by access time
        while let Some(entry) = cache_dir.next_entry().await.with_context(|| Io {
            file: "entry when iter cache_dir".to_string(),
        })? {
            cache
                .recover(entry.file_name().into_string().unwrap())
                .await?;
        }

        Ok(())
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

    // TODO: don't cache whole path for reasons below
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
    use tempfile::{tempdir, TempDir};
    use upstream::local::LocalFileSystem;

    use super::*;

    struct StoreWithCacheDir {
        inner: DiskCacheStore,
        cache_dir: TempDir,
    }

    async fn prepare_store(page_size: usize, cap: usize) -> StoreWithCacheDir {
        let local_path = tempdir().unwrap();
        let local_store = Arc::new(LocalFileSystem::new_with_prefix(local_path.path()).unwrap());

        let cache_dir = tempdir().unwrap();
        let store = DiskCacheStore::try_new(
            cache_dir.as_ref().to_string_lossy().to_string(),
            cap,
            page_size,
            local_store,
        )
        .await
        .unwrap();

        StoreWithCacheDir {
            inner: store,
            cache_dir,
        }
    }

    #[tokio::test]
    async fn test_normalize_range_less_than_file_size() {
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

        let store = prepare_store(page_size, 1000).await;
        for (input, expected) in testcases {
            assert_eq!(store.inner.normalize_range(1024, &input), expected);
        }
    }

    #[tokio::test]
    async fn test_normalize_range_great_than_file_size() {
        let page_size = 16;
        let testcases = vec![
            (0..1, vec![0..16]),
            (0..16, vec![0..16]),
            (0..17, vec![0..16, 16..20]),
            (16..32, vec![16..20]),
            (32..100, vec![]),
        ];

        let store = prepare_store(page_size, 1000).await;
        for (input, expected) in testcases {
            assert_eq!(store.inner.normalize_range(20, &input), expected);
        }
    }

    fn test_file_exists(cache_dir: &TempDir, location: &Path, range: &Range<usize>) -> bool {
        cache_dir
            .path()
            .join(DiskCacheStore::cache_key(location, range))
            .exists()
    }

    #[tokio::test]
    async fn test_disk_cache_store_get_range() {
        let page_size = 16;
        // 51 byte
        let data = b"a b c d e f g h i j k l m n o p q r s t u v w x y z";
        let location = Path::from("1.sst");
        let store = prepare_store(page_size, 1000).await;

        let mut buf = BytesMut::with_capacity(data.len() * 4);
        // extend 4 times, then location will contain 200 bytes
        for _ in 0..4 {
            buf.extend_from_slice(data);
        }
        store.inner.put(&location, buf.freeze()).await.unwrap();

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
                store.inner.get_range(&location, input).await.unwrap(),
                Bytes::copy_from_slice(expected.as_bytes())
            );
        }

        // remove cached values, then get again
        {
            let mut data_cache = store.inner.cache.cache.lock().await;
            for range in vec![0..16, 16..32, 32..48, 48..64, 64..80, 80..96, 96..112] {
                assert!(data_cache.contains(DiskCacheStore::cache_key(&location, &range).as_str()));
                assert!(test_file_exists(&store.cache_dir, &location, &range));
            }

            for range in vec![16..32, 48..64, 80..96] {
                assert!(data_cache
                    .pop(&DiskCacheStore::cache_key(&location, &range))
                    .is_some());
            }
        }

        assert_eq!(
            store.inner.get_range(&location, 16..100).await.unwrap(),
            Bytes::copy_from_slice(
                b"i j k l m n o p q r s t u v w x y za b c d e f g h i j k l m n o p q r s t u v w x y"
            )
        );
    }

    #[tokio::test]
    async fn test_disk_cache_remove_cache_file() {
        let page_size = 16;
        // 51 byte
        let data = b"a b c d e f g h i j k l m n o p q r s t u v w x y z";
        let location = Path::from("remove_cache_file.sst");
        let store = prepare_store(page_size, 32).await;
        let mut buf = BytesMut::with_capacity(data.len() * 4);
        // extend 4 times, then location will contain 200 bytes, but cache cap is 32
        for _ in 0..4 {
            buf.extend_from_slice(data);
        }
        store.inner.put(&location, buf.freeze()).await.unwrap();

        let _ = store.inner.get_range(&location, 0..16).await.unwrap();
        let _ = store.inner.get_range(&location, 16..32).await.unwrap();
        // cache is full now
        assert!(test_file_exists(&store.cache_dir, &location, &(0..16)));
        assert!(test_file_exists(&store.cache_dir, &location, &(16..32)));

        // insert new cache, evict oldest entry
        let _ = store.inner.get_range(&location, 32..48).await.unwrap();
        assert!(!test_file_exists(&store.cache_dir, &location, &(0..16)));
        assert!(test_file_exists(&store.cache_dir, &location, &(32..48)));

        // insert new cache, evict oldest entry
        let _ = store.inner.get_range(&location, 48..64).await.unwrap();
        assert!(!test_file_exists(&store.cache_dir, &location, &(16..32)));
        assert!(test_file_exists(&store.cache_dir, &location, &(48..64)));
    }
}
