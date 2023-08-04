// Copyright 2023 The CeresDB Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! An ObjectStore implementation with disk as cache.
//! The disk cache is a read-through caching, with page as its minimal cache
//! unit.
//!
//! Page is used for reasons below:
//! - reduce file size in case of there are too many request with small range.

use std::{fmt::Display, ops::Range, sync::Arc};

use async_trait::async_trait;
use bytes::{Bytes, BytesMut};
use crc::{Crc, CRC_32_ISCSI};
use futures::stream::BoxStream;
use hash_ext::SeaHasherBuilder;
use log::{debug, error, info};
use lru::LruCache;
use partitioned_lock::PartitionedMutex;
use serde::{Deserialize, Serialize};
use snafu::{ensure, Backtrace, ResultExt, Snafu};
use time_ext;
use tokio::{
    fs::{File, OpenOptions},
    io::{AsyncReadExt, AsyncSeekExt, AsyncWrite, AsyncWriteExt},
};
use upstream::{
    path::Path, Error as ObjectStoreError, GetResult, ListResult, MultipartId, ObjectMeta,
    ObjectStore, Result,
};

const MANIFEST_FILE: &str = "manifest.json";
const CURRENT_VERSION: usize = 1;
pub const CASTAGNOLI: Crc<u32> = Crc::<u32>::new(&CRC_32_ISCSI);

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
        "Failed to decode cache pb value, file:{}, source:{}.\nbacktrace:\n{}",
        file,
        source,
        backtrace
    ))]
    DecodeCache {
        file: String,
        source: prost::DecodeError,
        backtrace: Backtrace,
    },
    #[snafu(display("disk cache cap must large than 0",))]
    InvalidCapacity,
}

impl From<Error> for ObjectStoreError {
    fn from(source: Error) -> Self {
        Self::Generic {
            store: "DiskCacheStore",
            source: Box::new(source),
        }
    }
}

#[derive(Debug)]
enum ReadBytesResult {
    Integrate(Vec<u8>),
    Corrupted { file_size: usize },
    OutOfRange,
}

#[derive(Debug, Serialize, Deserialize)]
struct Manifest {
    create_at: String,
    page_size: usize,
    version: usize,
}

struct CacheFileEncoding;

impl CacheFileEncoding {
    const MAGIC_FOOTER: [u8; 8] = [0, 0, 0, 0, b'c', b'e', b'r', b'e'];

    async fn encode_and_persist<W>(&self, writer: &mut W, payload: Bytes) -> std::io::Result<()>
    where
        W: AsyncWrite + std::marker::Unpin,
    {
        let _ = writer.write(&payload[..]).await?;
        let _ = writer.write(&Self::MAGIC_FOOTER).await?;
        writer.flush().await?;

        Ok(())
    }

    #[inline]
    fn encoded_size(&self, payload_len: usize) -> usize {
        payload_len + Self::MAGIC_FOOTER.len()
    }
}

/// The mapping is filename -> file_size.
/// TODO: Store the crc as the value of the cache for integration check.
type FileMetaCache = LruCache<String, usize>;

#[derive(Debug)]
struct DiskCache {
    root_dir: String,
    meta_cache: PartitionedMutex<FileMetaCache, SeaHasherBuilder>,
}

impl DiskCache {
    fn try_new(root_dir: String, cap: usize, partition_bits: usize) -> Result<Self> {
        let init_lru = |partition_num: usize| -> Result<_> {
            let cap_per_part = cap / partition_num;
            ensure!(cap_per_part != 0, InvalidCapacity);
            Ok(LruCache::new(cap_per_part))
        };

        Ok(Self {
            root_dir,
            meta_cache: PartitionedMutex::try_new(init_lru, partition_bits, SeaHasherBuilder {})?,
        })
    }

    fn insert_meta(&self, filename: String, size: usize) -> Option<String> {
        let mut cache = self.meta_cache.lock(&filename);
        debug!(
            "Update the meta cache, file:{filename}, len:{}, cap_per_part:{}",
            cache.cap(),
            cache.len()
        );

        cache.push(filename, size).map(|(filename, _)| filename)
    }

    async fn insert_data(&self, filename: String, value: Bytes) {
        let file_size = CacheFileEncoding.encoded_size(value.len());
        let evicted_file = self.insert_meta(filename.clone(), file_size);

        let do_persist = || async {
            if let Err(e) = self.persist_bytes(&filename, value).await {
                error!("Failed to persist cache, file:{filename}, err:{e}");
            }
        };

        if let Some(evicted_file) = evicted_file {
            if evicted_file == filename {
                // No need to do persist and removal.
                return;
            }

            // Persist the new bytes.
            do_persist().await;

            // Remove the evicted file.
            let evicted_file_path = std::path::Path::new(&self.root_dir)
                .join(evicted_file)
                .into_os_string()
                .into_string()
                .unwrap();

            debug!("Evicted file:{evicted_file_path} is to be removed");
            if let Err(e) = tokio::fs::remove_file(&evicted_file_path).await {
                error!("Failed to remove evicted file:{evicted_file_path}, err:{e}");
            }
        } else {
            do_persist().await;
        }
    }

    /// Get the bytes from the disk cache.
    ///
    /// If the bytes is invalid (its size is different from the recorded one),
    /// remove it and return None.
    async fn get_data(&self, filename: &str, range: &Range<usize>) -> Option<Bytes> {
        let file_size = {
            let mut cache = self.meta_cache.lock(&filename);
            match cache.get(filename) {
                Some(file_size) => *file_size,
                None => return None,
            }
        };

        match self.read_bytes(filename, range, file_size).await {
            Ok(ReadBytesResult::Integrate(v)) => Some(v.into()),
            Ok(ReadBytesResult::Corrupted {
                file_size: real_file_size,
            }) => {
                error!(
                    "File:{filename} is corrupted, expect:{file_size}, got:{real_file_size}, and it will be removed",
                );

                {
                    let mut cache = self.meta_cache.lock(&filename);
                    cache.pop(filename);
                }

                self.remove_file_by_name(filename).await;

                None
            }
            Ok(ReadBytesResult::OutOfRange) => {
                error!(
                    "File:{filename} is not enough to read, range:{range:?}, file_size:{file_size}, and it will be removed",
                );

                {
                    let mut cache = self.meta_cache.lock(&filename);
                    cache.pop(filename);
                }

                self.remove_file_by_name(filename).await;

                None
            }
            Err(e) => {
                error!("Failed to read file:{filename} from the disk cache, err:{e}");
                None
            }
        }
    }

    async fn remove_file_by_name(&self, filename: &str) {
        debug!("Try to remove file:{filename}");

        let file_path = std::path::Path::new(&self.root_dir)
            .join(filename)
            .into_os_string()
            .into_string()
            .unwrap();

        if let Err(e) = tokio::fs::remove_file(&file_path).await {
            error!("Failed to remove evicted file:{file_path}, err:{e}");
        }
    }

    async fn persist_bytes(&self, filename: &str, payload: Bytes) -> Result<()> {
        let file_path = std::path::Path::new(&self.root_dir)
            .join(filename)
            .into_os_string()
            .into_string()
            .unwrap();

        let mut file = File::create(&file_path)
            .await
            .context(Io { file: &file_path })?;

        let encoding = CacheFileEncoding;
        encoding
            .encode_and_persist(&mut file, payload)
            .await
            .context(Io { file: &file_path })?;

        Ok(())
    }

    /// Read the bytes from the cached file.
    ///
    /// If the file size is different from the `expect_file_size`, it'll be
    /// thought as corrupted file.
    async fn read_bytes(
        &self,
        filename: &str,
        range: &Range<usize>,
        expect_file_size: usize,
    ) -> std::io::Result<ReadBytesResult> {
        if CacheFileEncoding.encoded_size(range.len()) > expect_file_size {
            return Ok(ReadBytesResult::OutOfRange);
        }

        let file_path = std::path::Path::new(&self.root_dir)
            .join(filename)
            .into_os_string()
            .into_string()
            .unwrap();

        let mut f = File::open(&file_path).await?;
        let file_size = f.metadata().await?.len() as usize;
        if expect_file_size != file_size {
            return Ok(ReadBytesResult::Corrupted { file_size });
        }

        f.seek(std::io::SeekFrom::Start(range.start as u64)).await?;
        let mut buf = vec![0; range.len()];
        let n = f.read_exact(&mut buf).await?;
        if n != range.len() {
            return Ok(ReadBytesResult::OutOfRange);
        }

        Ok(ReadBytesResult::Integrate(buf))
    }
}

impl Display for DiskCache {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DiskCache")
            .field("path", &self.root_dir)
            .field("cache", &self.meta_cache)
            .finish()
    }
}

struct PagedRanger {
    page_size: usize,
}

struct PagedRangeResult {
    aligned_start: usize,
    num_pages: usize,
}

impl PagedRanger {
    fn paged_range(&self, range: &Range<usize>) -> PagedRangeResult {
        // inclusive start
        let aligned_start = range.start / self.page_size * self.page_size;
        // exclusive end
        let aligned_end = (range.end + self.page_size - 1) / self.page_size * self.page_size;
        let num_pages = (aligned_end - aligned_start) / self.page_size;

        PagedRangeResult {
            aligned_start,
            num_pages,
        }
    }
}

/// There will be two kinds of file in this cache:
/// 1. manifest.json, which contains metadata, like
/// ```json
/// {
///     "create_at": "2022-12-01T08:51:15.167795+00:00",
///     "page_size": 1048576,
///     "version": 1
/// }
/// ```
/// 2. ${sst-path}-${range.start}-${range.end}, which contains bytes of given
/// range, start/end are aligned to page_size.
#[derive(Debug)]
pub struct DiskCacheStore {
    cache: DiskCache,
    // Max disk capacity cache use can
    cap: usize,
    // Size of each cached bytes
    page_size: usize,
    underlying_store: Arc<dyn ObjectStore>,
}

impl DiskCacheStore {
    pub async fn try_new(
        cache_dir: String,
        cap: usize,
        page_size: usize,
        underlying_store: Arc<dyn ObjectStore>,
        partition_bits: usize,
    ) -> Result<Self> {
        let page_num = cap / page_size;
        ensure!(page_num != 0, InvalidCapacity);

        let _ = Self::create_manifest_if_not_exists(&cache_dir, page_size).await?;
        let cache = DiskCache::try_new(cache_dir.clone(), page_num, partition_bits)?;
        Self::recover_cache(&cache_dir, &cache).await?;

        Ok(Self {
            cache,
            cap,
            page_size,
            underlying_store,
        })
    }

    async fn create_manifest_if_not_exists(cache_dir: &str, page_size: usize) -> Result<Manifest> {
        let mut file = OpenOptions::new()
            .write(true)
            .create(true)
            .read(true)
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
                create_at: time_ext::current_as_rfc3339(),
                version: CURRENT_VERSION,
            };

            let buf = serde_json::to_vec_pretty(&manifest).context(SerializeManifest)?;
            file.write_all(&buf).await.with_context(|| Io {
                file: MANIFEST_FILE.to_string(),
            })?;

            return Ok(manifest);
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
        // TODO: check version

        Ok(manifest)
    }

    async fn recover_cache(cache_dir: &str, cache: &DiskCache) -> Result<()> {
        let mut cache_dir = tokio::fs::read_dir(cache_dir).await.with_context(|| Io {
            file: cache_dir.to_string(),
        })?;

        // TODO: sort by access time
        while let Some(entry) = cache_dir.next_entry().await.with_context(|| Io {
            file: "entry when iter cache_dir".to_string(),
        })? {
            let file_name = entry.file_name().into_string().unwrap();
            if file_name == MANIFEST_FILE {
                // Skip the manifest file.
                continue;
            }

            let file_size = match entry.metadata().await {
                Ok(metadata) => metadata.len() as usize,
                Err(e) => {
                    error!("Failed to get the size of file:{file_name}, and it will be skipped for recover, err:{e}");
                    // TODO: Shall we remove such file.
                    continue;
                }
            };
            info!("Disk cache recover_cache, filename:{file_name}, size:{file_size}");
            cache.insert_meta(file_name, file_size);
        }

        Ok(())
    }

    fn cached_filename(location: &Path, range: &Range<usize>) -> String {
        format!(
            "{}-{}-{}",
            location.as_ref().replace('/', "-"),
            range.start,
            range.end
        )
    }

    async fn fetch_and_cache(
        &self,
        location: &Path,
        aligned_range: &Range<usize>,
    ) -> Result<Bytes> {
        let bytes = self
            .underlying_store
            .get_range(location, aligned_range.clone())
            .await?;

        let filename = Self::cached_filename(location, aligned_range);
        self.cache.insert_data(filename, bytes.clone()).await;
        Ok(bytes)
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
        let PagedRangeResult {
            aligned_start,
            num_pages,
        } = {
            let paged_ranger = PagedRanger {
                page_size: self.page_size,
            };
            paged_ranger.paged_range(&range)
        };
        assert!(num_pages > 0);

        let file_size = self.head(location).await?.size;
        // Fast path for only one page involved.
        if num_pages == 1 {
            let aligned_end = (aligned_start + self.page_size).min(file_size);
            let aligned_range = aligned_start..aligned_end;
            let filename = Self::cached_filename(location, &aligned_range);
            let range_in_file = (range.start - aligned_start)..(range.end - aligned_start);
            if let Some(bytes) = self.cache.get_data(&filename, &range_in_file).await {
                return Ok(bytes);
            }
            // This page is missing from the disk cache, let's fetch it from the
            // underlying store and insert it to the disk cache.
            let aligned_bytes = self.fetch_and_cache(location, &aligned_range).await?;

            // Allocate a new buffer instead of the `aligned_bytes` to avoid memory
            // overhead.
            let mut bytes_buf = BytesMut::with_capacity(range.len());
            bytes_buf.extend_from_slice(
                &aligned_bytes[(range.start - aligned_start)..(range.end - aligned_start)],
            );
            return Ok(bytes_buf.freeze());
        }

        // The queried range involves multiple ranges.
        let mut ranged_bytes: Vec<Option<Bytes>> = vec![None; num_pages];
        let mut num_missing_pages = 0;
        {
            let mut page_start = aligned_start;
            let mut page_idx = 0;
            while page_idx < num_pages {
                let page_end = (page_start + self.page_size).min(file_size);
                let range_in_file = {
                    let real_start = page_start.max(range.start);
                    let real_end = page_end.min(range.end);
                    (real_start - page_start)..(real_end - page_start)
                };
                let filename = Self::cached_filename(location, &(page_start..page_end));
                if let Some(bytes) = self.cache.get_data(&filename, &range_in_file).await {
                    ranged_bytes[page_idx] = Some(bytes);
                } else {
                    num_missing_pages += 1;
                }

                page_start += self.page_size;
                page_idx += 1;
            }
        }

        // Fetch all the missing pages from the underlying store.
        if num_missing_pages > 0 {
            let mut missing_ranges = Vec::with_capacity(num_missing_pages);
            let mut missing_range_idx = Vec::with_capacity(num_missing_pages);
            for (idx, cache_miss) in ranged_bytes.iter().map(|v| v.is_none()).enumerate() {
                if cache_miss {
                    let missing_range_start = aligned_start + idx * self.page_size;
                    let missing_range_end = (missing_range_start + self.page_size).min(file_size);
                    missing_ranges.push(missing_range_start..missing_range_end);
                    missing_range_idx.push(idx);
                }
            }

            let missing_ranged_bytes = self
                .underlying_store
                .get_ranges(location, &missing_ranges)
                .await?;
            assert_eq!(missing_ranged_bytes.len(), missing_ranges.len());

            for ((missing_range, missing_range_idx), bytes) in missing_ranges
                .into_iter()
                .zip(missing_range_idx.into_iter())
                .zip(missing_ranged_bytes.into_iter())
            {
                let filename = Self::cached_filename(location, &missing_range);
                self.cache.insert_data(filename, bytes.clone()).await;

                let offset = missing_range.start;
                let truncated_range = (missing_range.start.max(range.start) - offset)
                    ..(missing_range.end.min(range.end) - offset);

                ranged_bytes[missing_range_idx] = Some(bytes.slice(truncated_range));
            }
        }

        // There are multiple aligned ranges for one request, such as range = [3, 33),
        // page_size = 16, then aligned ranges will be [0, 16), [16, 32), [32,
        // 48), and we need to combine those ranged bytes to get final result
        // bytes
        let mut byte_buf = BytesMut::with_capacity(range.len());
        for bytes in ranged_bytes {
            byte_buf.extend(bytes);
        }
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

    async fn prepare_store(
        page_size: usize,
        cap: usize,
        partition_bits: usize,
    ) -> StoreWithCacheDir {
        let local_path = tempdir().unwrap();
        let local_store = Arc::new(LocalFileSystem::new_with_prefix(local_path.path()).unwrap());

        let cache_dir = tempdir().unwrap();
        let store = DiskCacheStore::try_new(
            cache_dir.as_ref().to_string_lossy().to_string(),
            cap,
            page_size,
            local_store,
            partition_bits,
        )
        .await
        .unwrap();

        StoreWithCacheDir {
            inner: store,
            cache_dir,
        }
    }

    fn test_file_exists(cache_dir: &TempDir, location: &Path, range: &Range<usize>) -> bool {
        cache_dir
            .path()
            .join(DiskCacheStore::cached_filename(location, range))
            .exists()
    }

    #[tokio::test]
    async fn test_disk_cache_store_get_range() {
        let page_size = 16;
        // 51 byte
        let data = b"a b c d e f g h i j k l m n o p q r s t u v w x y z";
        let location = Path::from("1.sst");
        let store = prepare_store(page_size, 1024, 0).await;

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
            for range in vec![0..16, 16..32, 32..48, 48..64, 64..80, 80..96, 96..112] {
                let data_cache = store
                    .inner
                    .cache
                    .meta_cache
                    .lock(&DiskCacheStore::cached_filename(&location, &range).as_str());
                assert!(data_cache
                    .contains(DiskCacheStore::cached_filename(&location, &range).as_str()));
                assert!(test_file_exists(&store.cache_dir, &location, &range));
            }

            for range in vec![16..32, 48..64, 80..96] {
                let mut data_cache = store
                    .inner
                    .cache
                    .meta_cache
                    .lock(&DiskCacheStore::cached_filename(&location, &range).as_str());
                assert!(data_cache
                    .pop(&DiskCacheStore::cached_filename(&location, &range))
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
        let store = prepare_store(page_size, 32, 0).await;
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

    #[tokio::test]
    async fn test_disk_cache_remove_cache_file_two_partition() {
        let page_size = 16;
        // 51 byte
        let data = b"a b c d e f g h i j k l m n o p q r s t u v w x y z";
        let location = Path::from("remove_cache_file_two_partition.sst");
        // partition_cap: 64 / 16 / 2 = 2
        let store = prepare_store(page_size, 64, 1).await;
        let mut buf = BytesMut::with_capacity(data.len() * 8);
        // extend 8 times
        for _ in 0..8 {
            buf.extend_from_slice(data);
        }
        store.inner.put(&location, buf.freeze()).await.unwrap();
        // use seahash
        // 0..16: partition 1
        // 16..32 partition 1
        // 32..48 partition 0
        // 48..64 partition 1
        // 64..80 partition 1
        // 80..96 partition 0
        // 96..112 partition 0
        // 112..128 partition 0
        // 128..144 partition 0
        let _ = store.inner.get_range(&location, 0..16).await.unwrap();
        let _ = store.inner.get_range(&location, 16..32).await.unwrap();
        // partition 1 cache is full now
        assert!(test_file_exists(&store.cache_dir, &location, &(0..16)));
        assert!(test_file_exists(&store.cache_dir, &location, &(16..32)));

        let _ = store.inner.get_range(&location, 32..48).await.unwrap();
        let _ = store.inner.get_range(&location, 80..96).await.unwrap();
        // partition 0 cache is full now

        assert!(test_file_exists(&store.cache_dir, &location, &(32..48)));
        assert!(test_file_exists(&store.cache_dir, &location, &(80..96)));

        // insert new entry into partition 0, evict partition 0's oldest entry
        let _ = store.inner.get_range(&location, 96..112).await.unwrap();
        assert!(!test_file_exists(&store.cache_dir, &location, &(32..48)));
        assert!(test_file_exists(&store.cache_dir, &location, &(80..96)));

        assert!(test_file_exists(&store.cache_dir, &location, &(0..16)));
        assert!(test_file_exists(&store.cache_dir, &location, &(16..32)));

        // insert new entry into partition 0, evict partition 0's oldest entry
        let _ = store.inner.get_range(&location, 128..144).await.unwrap();
        assert!(!test_file_exists(&store.cache_dir, &location, &(80..96)));
        assert!(test_file_exists(&store.cache_dir, &location, &(96..112)));
        assert!(test_file_exists(&store.cache_dir, &location, &(128..144)));

        assert!(test_file_exists(&store.cache_dir, &location, &(0..16)));
        assert!(test_file_exists(&store.cache_dir, &location, &(16..32)));

        // insert new entry into partition 1, evict partition 1's oldest entry
        let _ = store.inner.get_range(&location, 64..80).await.unwrap();
        assert!(!test_file_exists(&store.cache_dir, &location, &(0..16)));
        assert!(test_file_exists(&store.cache_dir, &location, &(16..32)));
        assert!(test_file_exists(&store.cache_dir, &location, &(64..80)));

        assert!(test_file_exists(&store.cache_dir, &location, &(96..112)));
        assert!(test_file_exists(&store.cache_dir, &location, &(128..144)));
    }

    #[tokio::test]
    async fn test_disk_cache_manifest() {
        let cache_dir = tempdir().unwrap();
        let cache_root_dir = cache_dir.as_ref().to_string_lossy().to_string();
        let page_size = 8;
        let first_create_time = {
            let _store = {
                let local_path = tempdir().unwrap();
                let local_store =
                    Arc::new(LocalFileSystem::new_with_prefix(local_path.path()).unwrap());
                DiskCacheStore::try_new(cache_root_dir.clone(), 160, 8, local_store, 0)
                    .await
                    .unwrap()
            };
            let manifest =
                DiskCacheStore::create_manifest_if_not_exists(&cache_root_dir, page_size)
                    .await
                    .unwrap();

            assert_eq!(manifest.page_size, 8);
            assert_eq!(manifest.version, 1);
            manifest.create_at
        };

        // open again
        {
            let _store = {
                let local_path = tempdir().unwrap();
                let local_store =
                    Arc::new(LocalFileSystem::new_with_prefix(local_path.path()).unwrap());
                DiskCacheStore::try_new(cache_root_dir.clone(), 160, 8, local_store, 0)
                    .await
                    .unwrap()
            };

            let manifest =
                DiskCacheStore::create_manifest_if_not_exists(&cache_root_dir, page_size)
                    .await
                    .unwrap();
            assert_eq!(manifest.create_at, first_create_time);
            assert_eq!(manifest.page_size, 8);
            assert_eq!(manifest.version, 1);
        }

        // open again, but with different page_size
        {
            let local_path = tempdir().unwrap();
            let local_store =
                Arc::new(LocalFileSystem::new_with_prefix(local_path.path()).unwrap());
            let store = DiskCacheStore::try_new(
                cache_dir.as_ref().to_string_lossy().to_string(),
                160,
                page_size * 2,
                local_store,
                0,
            )
            .await;

            assert!(store.is_err())
        }
    }

    #[tokio::test]
    async fn test_disk_cache_recovery() {
        let cache_dir = tempdir().unwrap();
        let cache_root_dir = cache_dir.as_ref().to_string_lossy().to_string();
        let page_size = 16;
        let location = Path::from("recovery.sst");
        {
            let store = {
                let local_path = tempdir().unwrap();
                let local_store =
                    Arc::new(LocalFileSystem::new_with_prefix(local_path.path()).unwrap());
                DiskCacheStore::try_new(cache_root_dir.clone(), 10240, page_size, local_store, 0)
                    .await
                    .unwrap()
            };
            let data = b"abcd";
            let mut buf = BytesMut::with_capacity(data.len() * 1024);
            for _ in 0..1024 {
                buf.extend_from_slice(data);
            }
            let buf = buf.freeze();
            store.put(&location, buf.clone()).await.unwrap();
            let read_range = 16..100;
            let bytes = store
                .get_range(&location, read_range.clone())
                .await
                .unwrap();
            assert_eq!(bytes.len(), read_range.len());
            assert_eq!(bytes[..], buf[read_range])
        };

        // recover
        {
            let store = {
                let local_path = tempdir().unwrap();
                let local_store =
                    Arc::new(LocalFileSystem::new_with_prefix(local_path.path()).unwrap());
                DiskCacheStore::try_new(cache_root_dir.clone(), 160, page_size, local_store, 0)
                    .await
                    .unwrap()
            };
            for range in vec![16..32, 32..48, 48..64, 64..80, 80..96, 96..112] {
                let filename = DiskCacheStore::cached_filename(&location, &range);
                let cache = store.cache.meta_cache.lock(&filename);
                assert!(cache.contains(&filename));
                assert!(test_file_exists(&cache_dir, &location, &range));
            }
        };
    }

    #[test]
    fn test_disk_cache_bytes_crc() {
        let testcases = vec![("abc", 910901175), ("hello ceresdb", 2026251212)];

        for (input, expect) in testcases {
            let actual = CASTAGNOLI.checksum(input.as_bytes());
            assert_eq!(actual, expect);
        }
    }

    #[tokio::test]
    async fn test_corrupt_disk_cache() {
        for page_size in [1, 2, 4, 8, 16, 32, 64, 128] {
            corrupt_disk_cache(page_size).await;
        }
    }

    async fn corrupt_disk_cache(page_size: usize) {
        let StoreWithCacheDir {
            inner: store,
            cache_dir,
        } = prepare_store(page_size, 1024, 0).await;
        let test_file_name = "corrupted_disk_cache_file";
        let test_file_path = Path::from(test_file_name);
        let test_file_bytes = Bytes::from("corrupted_disk_cache_file_data");

        // Put data into store and get it to let the cache load the data.
        store
            .put(&test_file_path, test_file_bytes.clone())
            .await
            .unwrap();

        // The data should be in the cache.
        let got_bytes = store
            .get_range(&test_file_path, 0..test_file_bytes.len())
            .await
            .unwrap();
        assert_eq!(got_bytes, test_file_bytes);

        // Corrupt files in the cache dir.
        let mut cache_read_dir = tokio::fs::read_dir(cache_dir.as_ref()).await.unwrap();
        while let Some(entry) = cache_read_dir.next_entry().await.unwrap() {
            let path_buf = entry.path();
            let path = path_buf.to_str().unwrap();
            if path.contains(test_file_name) {
                let mut file = tokio::fs::OpenOptions::new()
                    .write(true)
                    .truncate(true)
                    .open(path)
                    .await
                    .unwrap();
                // TODO: currently the data integrity is checked based on the file size, so here
                // we give a bytes with designed length to make the check failed.
                file.write_all(b"corrupt").await.unwrap();
            }
        }

        // The data should be removed from the cache.
        let got_bytes = store
            .get_range(&test_file_path, 0..test_file_bytes.len())
            .await
            .unwrap();
        assert_eq!(got_bytes, test_file_bytes);
        // The cache should be updated.
        let mut cache_read_dir = tokio::fs::read_dir(cache_dir.as_ref()).await.unwrap();
        while let Some(entry) = cache_read_dir.next_entry().await.unwrap() {
            let path_buf = entry.path();
            let path = path_buf.to_str().unwrap();
            if path.contains(test_file_name) {
                let mut file = tokio::fs::OpenOptions::new()
                    .read(true)
                    .open(path)
                    .await
                    .unwrap();
                let mut buffer = Vec::new();
                file.read_to_end(&mut buffer).await.unwrap();
                assert_ne!(buffer, b"corrupted");
            }
        }
    }
}
