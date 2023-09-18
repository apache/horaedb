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

use std::{fmt::Display, ops::Range, result::Result as StdResult, sync::Arc};

use async_trait::async_trait;
use bytes::{Bytes, BytesMut};
use chrono::{DateTime, Utc};
use crc::{Crc, CRC_32_ISCSI};
use futures::stream::BoxStream;
use hash_ext::SeaHasherBuilder;
use log::{debug, error, info, warn};
use lru::LruCache;
use notifier::notifier::{ExecutionGuard, RequestNotifiers};
use partitioned_lock::PartitionedMutex;
use serde::{Deserialize, Serialize};
use snafu::{ensure, Backtrace, ResultExt, Snafu};
use time_ext;
use tokio::{
    fs::{self, File, OpenOptions},
    io::{AsyncReadExt, AsyncSeekExt, AsyncWrite, AsyncWriteExt},
    sync::oneshot::{self, error::RecvError, Receiver},
};
use upstream::{
    path::Path, Error as ObjectStoreError, GetResult, ListResult, MultipartId, ObjectMeta,
    ObjectStore, Result,
};

use crate::metrics::DISK_CACHE_DEDUP_COUNT;

const FILE_SIZE_CACHE_CAP: usize = 1 << 18;
const FILE_SIZE_CACHE_PARTITION_BITS: usize = 8;
pub const CASTAGNOLI: Crc<u32> = Crc::<u32>::new(&CRC_32_ISCSI);

#[derive(Debug, Snafu)]
enum Error {
    #[snafu(display("IO failed, file:{file}, source:{source}.\nbacktrace:\n{backtrace}",))]
    Io {
        file: String,
        source: std::io::Error,
        backtrace: Backtrace,
    },

    #[snafu(display("Access is out of range, range:{range:?}, file_size:{file_size}, last_modified:{last_modified:?}, file:{file}.\nbacktrace:\n{backtrace}"))]
    OutOfRange {
        range: Range<usize>,
        file_size: usize,
        file: String,
        last_modified: DateTime<Utc>,
        backtrace: Backtrace,
    },

    #[snafu(display(
        "Partial write, expect bytes:{expect}, written:{written}.\nbacktrace:\n{backtrace}",
    ))]
    PartialWrite {
        expect: usize,
        written: usize,
        backtrace: Backtrace,
    },

    #[snafu(display("Failed to deserialize manifest, source:{source}.\nbacktrace:\n{backtrace}"))]
    DeserializeManifest {
        source: serde_json::Error,
        backtrace: Backtrace,
    },

    #[snafu(display("Failed to serialize manifest, source:{source}.\nbacktrace:\n{backtrace}"))]
    SerializeManifest {
        source: serde_json::Error,
        backtrace: Backtrace,
    },

    #[snafu(display(
        "Failed to receive bytes from channel, source:{source}.\nbacktrace:\n{backtrace}"
    ))]
    ReceiveBytesFromChannel {
        backtrace: Backtrace,
        source: RecvError,
    },

    #[snafu(display("Fetch data failed, error.\nbacktrace:\n{backtrace}"))]
    FetchDataFromObjectStore { backtrace: Backtrace },

    #[snafu(display("Wait notifier failed, message:{message}."))]
    WaitNotifier { message: String },

    #[snafu(display("Invalid manifest page size, old:{old}, new:{new}."))]
    InvalidManifest { old: usize, new: usize },

    #[snafu(display(
        "Failed to persist cache, file:{file}, source:{source}.\nbacktrace:\n{backtrace}",
    ))]
    PersistCache {
        file: String,
        source: tokio::io::Error,
        backtrace: Backtrace,
    },

    #[snafu(display(
        "Failed to decode cache pb value, file:{file}, source:{source}.\nbacktrace:\n{backtrace}",
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

/// The result of read bytes of a page file.
#[derive(Debug)]
enum ReadBytesResult {
    Integrate(Vec<u8>),
    /// The page file is corrupted.
    Corrupted {
        file_size: usize,
    },
    /// The read range exceeds the file size.
    OutOfRange,
}

/// The manifest for describing the meta of the disk cache.
#[derive(Debug, Serialize, Deserialize)]
struct Manifest {
    create_at: String,
    page_size: usize,
    version: usize,
}

impl Manifest {
    const CURRENT_VERSION: usize = 2;
    const FILE_NAME: &'static str = "manifest.json";

    #[inline]
    fn is_valid(&self, version: usize, page_size: usize) -> bool {
        self.page_size == page_size && self.version == version
    }
}

/// The writer of the page file in the disk cache.
///
/// Following the payload, a footer [`PageFileEncoder::MAGIC_FOOTER`] is
/// appended.
struct PageFileWriter {
    output: String,
}

impl PageFileWriter {
    const MAGIC_FOOTER: [u8; 8] = [0, 0, 0, 0, b'c', b'e', b'r', b'e'];

    fn new(output: String) -> Self {
        Self { output }
    }

    fn tmp_file(input: &str) -> String {
        format!("{}.tmp", input)
    }

    async fn write_inner(&self, tmp_file: &str, bytes: Bytes) -> Result<()> {
        let mut writer = File::create(&tmp_file)
            .await
            .context(Io { file: tmp_file })?;
        writer
            .write_all(&bytes)
            .await
            .context(Io { file: tmp_file })?;

        writer
            .write_all(&Self::MAGIC_FOOTER)
            .await
            .context(Io { file: tmp_file })?;

        writer.flush().await.context(Io { file: tmp_file })?;

        tokio::fs::rename(tmp_file, &self.output)
            .await
            .context(Io { file: &self.output })?;

        Ok(())
    }

    // When write bytes to file, the cache lock is released, so when one thread is
    // reading, another thread may update it, so we write to tmp file first,
    // then rename to expected filename to avoid other threads see partial
    // content.
    async fn write_and_flush(self, bytes: Bytes) -> Result<()> {
        let tmp_file = Self::tmp_file(&self.output);
        let write_result = self.write_inner(&tmp_file, bytes).await;
        if write_result.is_err() {
            // we don't care this result.
            _ = tokio::fs::remove_file(&tmp_file).await;
        }

        write_result
    }

    #[inline]
    fn encoded_size(payload_len: usize) -> usize {
        payload_len + Self::MAGIC_FOOTER.len()
    }
}

/// The mapping is PageFileName -> PageMeta.
type PageMetaCache = LruCache<String, PageMeta>;

#[derive(Clone, Debug)]
struct PageMeta {
    file_size: usize,
    // TODO: Introduce the CRC for integration check.
}

#[derive(Debug)]
struct DiskCache {
    root_dir: String,
    meta_cache: PartitionedMutex<PageMetaCache, SeaHasherBuilder>,
}

#[derive(Debug, Clone)]
struct FileMeta {
    last_modified: DateTime<Utc>,
    size: usize,
}

impl From<ObjectMeta> for FileMeta {
    fn from(v: ObjectMeta) -> Self {
        FileMeta {
            last_modified: v.last_modified,
            size: v.size,
        }
    }
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

    fn insert_page_meta(&self, filename: String, page_meta: PageMeta) -> Option<String> {
        let mut cache = self.meta_cache.lock(&filename);
        debug!(
            "Update the meta cache, file:{filename}, len:{}, cap_per_part:{}",
            cache.cap(),
            cache.len()
        );

        cache
            .push(filename, page_meta)
            .map(|(filename, _)| filename)
    }

    async fn insert_data(&self, filename: String, value: Bytes) {
        let page_meta = {
            let file_size = PageFileWriter::encoded_size(value.len());
            PageMeta { file_size }
        };
        let evicted_file = self.insert_page_meta(filename.clone(), page_meta);

        let do_persist = || async {
            if let Err(e) = self.persist_bytes(&filename, value).await {
                warn!("Failed to persist cache, file:{filename}, err:{e}");
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
            debug!("Evicted file:{evicted_file} is to be removed");
            self.remove_file_by_name(&evicted_file).await;
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
                Some(page_meta) => page_meta.file_size,
                None => return None,
            }
        };

        match self.read_bytes(filename, range, file_size).await {
            Ok(ReadBytesResult::Integrate(v)) => Some(v.into()),
            Ok(ReadBytesResult::Corrupted {
                file_size: real_file_size,
            }) => {
                warn!(
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
                warn!(
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
                warn!("Failed to read file:{filename} from the disk cache, err:{e}");
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
            warn!("Failed to remove file:{file_path}, err:{e}");
        }
    }

    async fn persist_bytes(&self, filename: &str, payload: Bytes) -> Result<()> {
        let dest_filepath = std::path::Path::new(&self.root_dir)
            .join(filename)
            .into_os_string()
            .into_string()
            .unwrap();

        let writer = PageFileWriter::new(dest_filepath);
        writer.write_and_flush(payload).await?;

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
        if PageFileWriter::encoded_size(range.len()) > expect_file_size {
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

#[derive(Debug, Clone)]
struct Paging {
    page_size: usize,
}

#[derive(Debug, Clone)]
struct PageRangeResult {
    aligned_start: usize,
    num_pages: usize,
}

impl Paging {
    fn page_range(&self, range: &Range<usize>) -> PageRangeResult {
        // inclusive start
        let aligned_start = range.start / self.page_size * self.page_size;
        // exclusive end
        let aligned_end = (range.end + self.page_size - 1) / self.page_size * self.page_size;
        let num_pages = (aligned_end - aligned_start) / self.page_size;

        PageRangeResult {
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
    cap: usize,
    page_size: usize,
    meta_cache: PartitionedMutex<LruCache<Path, FileMeta>, SeaHasherBuilder>,
    underlying_store: Arc<dyn ObjectStore>,
    request_notifiers: Arc<RequestNotifiers<String, oneshot::Sender<StdResult<Bytes, Error>>>>,
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

        let manifest = Self::create_manifest_if_not_exists(&cache_dir, page_size).await?;
        if !manifest.is_valid(Manifest::CURRENT_VERSION, page_size) {
            Self::reset_cache(&cache_dir, page_size).await?;
        }

        let cache = DiskCache::try_new(cache_dir.clone(), page_num, partition_bits)?;
        Self::recover_cache(&cache_dir, &cache).await?;

        let init_size_lru = |partition_num| -> Result<_> {
            let cap_per_part = FILE_SIZE_CACHE_CAP / partition_num;
            assert!(cap_per_part > 0);
            Ok(LruCache::new(cap_per_part))
        };
        let meta_cache = PartitionedMutex::try_new(
            init_size_lru,
            FILE_SIZE_CACHE_PARTITION_BITS,
            SeaHasherBuilder,
        )?;

        let request_notifiers = Arc::new(RequestNotifiers::default());

        Ok(Self {
            cache,
            cap,
            page_size,
            meta_cache,
            underlying_store,
            request_notifiers,
        })
    }

    async fn reset_cache(cache_dir_path: &str, page_size: usize) -> Result<()> {
        warn!("The manifest is outdated, the object store cache will be cleared");

        fs::remove_dir_all(cache_dir_path).await.context(Io {
            file: cache_dir_path,
        })?;

        Self::create_manifest_if_not_exists(cache_dir_path, page_size).await?;

        Ok(())
    }

    async fn create_manifest_if_not_exists(
        cache_dir_path: &str,
        page_size: usize,
    ) -> Result<Manifest> {
        // TODO: introduce the manifest lock to avoid multiple process to modify the
        // cache file data.

        let mut file = OpenOptions::new()
            .write(true)
            .create(true)
            .read(true)
            .truncate(false)
            .open(std::path::Path::new(cache_dir_path).join(Manifest::FILE_NAME))
            .await
            .context(Io {
                file: Manifest::FILE_NAME,
            })?;

        let metadata = file.metadata().await.context(Io {
            file: Manifest::FILE_NAME,
        })?;

        // Initialize the manifest if it doesn't exist.
        if metadata.len() == 0 {
            let manifest = Manifest {
                page_size,
                create_at: time_ext::current_as_rfc3339(),
                version: Manifest::CURRENT_VERSION,
            };

            let buf = serde_json::to_vec_pretty(&manifest).context(SerializeManifest)?;
            file.write_all(&buf).await.context(Io {
                file: Manifest::FILE_NAME,
            })?;

            return Ok(manifest);
        }

        let mut buf = Vec::new();
        file.read_to_end(&mut buf).await.context(Io {
            file: Manifest::FILE_NAME,
        })?;

        // TODO: Maybe we should clear all the cache when the manifest is corrupted.
        let manifest: Manifest = serde_json::from_slice(&buf).context(DeserializeManifest)?;
        ensure!(
            manifest.page_size == page_size,
            InvalidManifest {
                old: manifest.page_size,
                new: page_size
            }
        );

        Ok(manifest)
    }

    async fn recover_cache(cache_dir_path: &str, cache: &DiskCache) -> Result<()> {
        let mut cache_dir = tokio::fs::read_dir(cache_dir_path).await.context(Io {
            file: cache_dir_path,
        })?;

        while let Some(entry) = cache_dir.next_entry().await.with_context(|| Io {
            file: format!("a file in the cache_dir:{cache_dir_path}"),
        })? {
            let file_name = entry.file_name().into_string().unwrap();
            if file_name == Manifest::FILE_NAME {
                // Skip the manifest file.
                continue;
            }

            let file_size = match entry.metadata().await {
                Ok(metadata) => metadata.len() as usize,
                Err(e) => {
                    warn!("Failed to get the size of file:{file_name}, and it will be skipped for recover, err:{e}");
                    // TODO: Maybe we should remove this file.
                    continue;
                }
            };
            info!("Disk cache recover_cache, filename:{file_name}, size:{file_size}");
            let page_meta = PageMeta { file_size };
            cache.insert_page_meta(file_name, page_meta);
        }

        Ok(())
    }

    /// Generate the filename of a page file.
    fn page_cache_name(location: &Path, range: &Range<usize>) -> String {
        format!(
            "{}-{}-{}",
            location.as_ref().replace('/', "-"),
            range.start,
            range.end
        )
    }

    async fn deduped_fetch_data(
        &self,
        location: &Path,
        aligned_ranges: impl IntoIterator<Item = Range<usize>>,
    ) -> Result<Vec<Receiver<StdResult<Bytes, Error>>>> {
        let aligned_ranges = aligned_ranges.into_iter();
        let (size, _) = aligned_ranges.size_hint();
        let mut rxs = Vec::with_capacity(size);
        let mut need_fetch_block = Vec::new();
        let mut need_fetch_block_cache_key = Vec::new();

        for aligned_range in aligned_ranges {
            let (tx, rx) = oneshot::channel();
            let cache_key = Self::page_cache_name(location, &aligned_range);

            if let notifier::notifier::RequestResult::First = self
                .request_notifiers
                .insert_notifier(cache_key.to_owned(), tx)
            {
                need_fetch_block.push(aligned_range);
                need_fetch_block_cache_key.push(cache_key);
            } else {
                DISK_CACHE_DEDUP_COUNT.inc();
            }

            rxs.push(rx);
        }

        if need_fetch_block.is_empty() {
            // All ranges are not first request, return directly.
            return Ok(rxs);
        }

        let fetched_bytes = {
            // This guard will ensure notifiers got released when futures get cancelled
            // during `get_ranges`.
            let mut guard = ExecutionGuard::new(|| {
                for cache_key in &need_fetch_block_cache_key {
                    let _ = self.request_notifiers.take_notifiers(cache_key);
                }
            });

            let bytes = self
                .underlying_store
                .get_ranges(location, &need_fetch_block)
                .await;

            guard.cancel();
            bytes
        };

        // Take all cache_key's notifiers out from request_notifiers immediately.
        let notifiers_vec: Vec<_> = need_fetch_block_cache_key
            .iter()
            .map(|cache_key| self.request_notifiers.take_notifiers(cache_key).unwrap())
            .collect();

        let fetched_bytes = match fetched_bytes {
            Err(err) => {
                for notifiers in notifiers_vec {
                    for notifier in notifiers {
                        if let Err(e) = notifier.send(
                            WaitNotifier {
                                message: err.to_string(),
                            }
                            .fail(),
                        ) {
                            error!("Failed to send notifier error result, err:{e:?}.");
                        }
                    }
                }

                return Err(err);
            }
            Ok(v) => v,
        };

        for ((bytes, notifiers), cache_key) in fetched_bytes
            .into_iter()
            .zip(notifiers_vec.into_iter())
            .zip(need_fetch_block_cache_key.into_iter())
        {
            self.cache.insert_data(cache_key, bytes.clone()).await;
            for notifier in notifiers {
                if let Err(e) = notifier.send(Ok(bytes.clone())) {
                    error!("Failed to send notifier success result, err:{e:?}.");
                }
            }
        }

        Ok(rxs)
    }

    /// Fetch the data from the underlying store and then cache it.
    async fn fetch_and_cache_data(
        &self,
        location: &Path,
        aligned_range: &Range<usize>,
    ) -> Result<Bytes> {
        let mut rxs = self
            .deduped_fetch_data(location, [aligned_range.clone()])
            .await?;

        assert_eq!(rxs.len(), 1);

        let rx = rxs.remove(0);
        let bytes = rx.await.context(ReceiveBytesFromChannel)??;
        Ok(bytes)
    }

    /// Fetch the file meta from the cache or the underlying store.
    async fn fetch_file_meta(&self, location: &Path) -> Result<FileMeta> {
        {
            let mut cache = self.meta_cache.lock(location);
            if let Some(file_meta) = cache.get(location) {
                return Ok(file_meta.clone());
            }
        }
        // The file meta is miss from the cache, let's fetch it from the
        // underlying store.

        let meta = self.underlying_store.head(location).await?;
        let file_meta = FileMeta::from(meta);
        {
            let mut cache = self.meta_cache.lock(location);
            cache.push(location.clone(), file_meta.clone());
        }
        Ok(file_meta)
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

    async fn get(&self, location: &Path) -> Result<GetResult> {
        // In sst module, we only use get_range, fetching a whole file is not used, and
        // it is not good for disk cache.
        self.underlying_store.get(location).await
    }

    async fn get_range(&self, location: &Path, range: Range<usize>) -> Result<Bytes> {
        let file_meta = self.fetch_file_meta(location).await?;
        ensure!(
            file_meta.size >= range.end,
            OutOfRange {
                range,
                file_size: file_meta.size,
                last_modified: file_meta.last_modified,
                file: location.to_string()
            }
        );

        let PageRangeResult {
            aligned_start,
            num_pages,
        } = {
            let paging = Paging {
                page_size: self.page_size,
            };
            paging.page_range(&range)
        };
        assert!(num_pages > 0);

        // Fast path for only one page involved.
        if num_pages == 1 {
            let aligned_end = (aligned_start + self.page_size).min(file_meta.size);
            let aligned_range = aligned_start..aligned_end;
            let filename = Self::page_cache_name(location, &aligned_range);
            let range_in_file = (range.start - aligned_start)..(range.end - aligned_start);
            if let Some(bytes) = self.cache.get_data(&filename, &range_in_file).await {
                return Ok(bytes);
            }
            // This page is missing from the disk cache, let's fetch it from the
            // underlying store and insert it to the disk cache.
            let aligned_bytes = self.fetch_and_cache_data(location, &aligned_range).await?;

            // Allocate a new buffer instead of the `aligned_bytes` to avoid memory
            // overhead.
            let mut bytes_buf = BytesMut::with_capacity(range.len());
            bytes_buf.extend_from_slice(
                &aligned_bytes[(range.start - aligned_start)..(range.end - aligned_start)],
            );
            return Ok(bytes_buf.freeze());
        }

        // The queried range involves multiple ranges.
        // Here is an example to explain the paged bytes, saying range = [3, 33),
        // page_size = 16, then aligned ranges will be [0, 16), [16, 32), [32,
        // 48), and we need to combine those ranged bytes to get final result bytes.
        let mut paged_bytes: Vec<Option<Bytes>> = vec![None; num_pages];
        let mut num_missing_pages = 0;
        {
            let mut page_start = aligned_start;
            let mut page_idx = 0;
            while page_idx < num_pages {
                let page_end = (page_start + self.page_size).min(file_meta.size);
                let range_in_file = {
                    let real_start = page_start.max(range.start);
                    let real_end = page_end.min(range.end);
                    (real_start - page_start)..(real_end - page_start)
                };
                let filename = Self::page_cache_name(location, &(page_start..page_end));
                if let Some(bytes) = self.cache.get_data(&filename, &range_in_file).await {
                    paged_bytes[page_idx] = Some(bytes);
                } else {
                    num_missing_pages += 1;
                }

                page_start += self.page_size;
                page_idx += 1;
            }
        }

        let concat_paged_bytes = |paged_bytes: Vec<Option<Bytes>>| {
            // Concat the paged bytes.
            let mut byte_buf = BytesMut::with_capacity(range.len());
            for bytes in paged_bytes {
                byte_buf.extend(bytes);
            }
            Ok(byte_buf.freeze())
        };

        if num_missing_pages == 0 {
            return concat_paged_bytes(paged_bytes);
        }

        // Fetch all the missing pages from the underlying store.
        let mut missing_ranges = Vec::with_capacity(num_missing_pages);
        let mut missing_range_idx = Vec::with_capacity(num_missing_pages);
        for (idx, cache_miss) in paged_bytes.iter().map(|v| v.is_none()).enumerate() {
            if cache_miss {
                let missing_range_start = aligned_start + idx * self.page_size;
                let missing_range_end = (missing_range_start + self.page_size).min(file_meta.size);
                missing_ranges.push(missing_range_start..missing_range_end);
                missing_range_idx.push(idx);
            }
        }

        let mut missing_ranged_bytes = Vec::with_capacity(missing_ranges.len());
        let rxs = self
            .deduped_fetch_data(location, missing_ranges.clone())
            .await?;
        for rx in rxs {
            let bytes = rx.await.context(ReceiveBytesFromChannel)??;
            missing_ranged_bytes.push(bytes);
        }

        assert_eq!(missing_ranged_bytes.len(), missing_ranges.len());

        for ((missing_range, missing_range_idx), bytes) in missing_ranges
            .into_iter()
            .zip(missing_range_idx.into_iter())
            .zip(missing_ranged_bytes.into_iter())
        {
            let offset = missing_range.start;
            let truncated_range = (missing_range.start.max(range.start) - offset)
                ..(missing_range.end.min(range.end) - offset);

            paged_bytes[missing_range_idx] = Some(bytes.slice(truncated_range));
        }

        return concat_paged_bytes(paged_bytes);
    }

    async fn head(&self, location: &Path) -> Result<ObjectMeta> {
        let file_meta = self.fetch_file_meta(location).await?;
        Ok(ObjectMeta {
            location: location.clone(),
            last_modified: file_meta.last_modified,
            size: file_meta.size,
        })
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
    use crate::test_util::MemoryStore;

    struct StoreWithCacheDir {
        inner: DiskCacheStore,
        cache_dir: TempDir,
    }

    async fn prepare_store(
        page_size: usize,
        cap: usize,
        partition_bits: usize,
    ) -> StoreWithCacheDir {
        let local_store = Arc::new(MemoryStore::default());

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
            .join(DiskCacheStore::page_cache_name(location, range))
            .exists()
    }

    #[tokio::test]
    async fn test_disk_cache_out_of_range() {
        let page_size = 16;
        // 51 byte
        let data = b"a b c d e f g h i j k l m n o p q r s t u v w x y z";
        let location = Path::from("out_of_range_test.sst");
        let store = prepare_store(page_size, 32, 0).await;
        let buf = Bytes::from_static(data);
        store.inner.put(&location, buf.clone()).await.unwrap();

        // Read one page out of range.
        let res = store.inner.get_range(&location, 48..54).await;
        assert!(res.is_err());

        // Read multiple pages out of range.
        let res = store.inner.get_range(&location, 24..54).await;
        assert!(res.is_err());
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
            for range in [0..16, 16..32, 32..48, 48..64, 64..80, 80..96, 96..112] {
                let data_cache = store
                    .inner
                    .cache
                    .meta_cache
                    .lock(&DiskCacheStore::page_cache_name(&location, &range).as_str());
                assert!(data_cache
                    .contains(DiskCacheStore::page_cache_name(&location, &range).as_str()));
                assert!(test_file_exists(&store.cache_dir, &location, &range));
            }

            for range in [16..32, 48..64, 80..96] {
                let mut data_cache = store
                    .inner
                    .cache
                    .meta_cache
                    .lock(&DiskCacheStore::page_cache_name(&location, &range).as_str());
                assert!(data_cache
                    .pop(&DiskCacheStore::page_cache_name(&location, &range))
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
    async fn test_disk_cache_multi_thread_fetch_same_block() {
        let page_size = 16;
        // 51 byte
        let data = b"a b c d e f g h i j k l m n o p q r s t u v w x y z";
        let location = Path::from("1.sst");
        let store = Arc::new(prepare_store(page_size, 32, 0).await);

        let mut buf = BytesMut::with_capacity(data.len() * 4);
        // extend 4 times, then location will contain 200 bytes
        for _ in 0..4 {
            buf.extend_from_slice(data);
        }
        store.inner.put(&location, buf.freeze()).await.unwrap();

        let testcases = vec![
            (0..6, "a b c "),
            (0..16, "a b c d e f g h "),
            (0..17, "a b c d e f g h i"),
            (16..17, "i"),
            (16..100, "i j k l m n o p q r s t u v w x y za b c d e f g h i j k l m n o p q r s t u v w x y"),
        ];
        let testcases = testcases
            .iter()
            .cycle()
            .take(testcases.len() * 100)
            .cloned()
            .collect::<Vec<_>>();

        let mut tasks = Vec::with_capacity(testcases.len());
        for (input, _) in &testcases {
            let store = store.clone();
            let location = location.clone();
            let input = input.clone();

            tasks.push(tokio::spawn(async move {
                store.inner.get_range(&location, input).await.unwrap()
            }));
        }

        let actual = futures::future::join_all(tasks).await;
        for (actual, (_, expected)) in actual.into_iter().zip(testcases.into_iter()) {
            assert_eq!(actual.unwrap(), Bytes::from(expected))
        }
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
            assert_eq!(manifest.version, Manifest::CURRENT_VERSION);
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
            assert_eq!(manifest.version, Manifest::CURRENT_VERSION);
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
            for range in [16..32, 32..48, 48..64, 64..80, 80..96, 96..112] {
                let filename = DiskCacheStore::page_cache_name(&location, &range);
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
