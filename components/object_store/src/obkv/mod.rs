// Copyright 2023 CeresDB Project Authors. Licensed under Apache-2.0.

use std::{
    collections::{hash_map::DefaultHasher, HashSet},
    hash::{Hash, Hasher},
    ops::Range,
    sync::{
        atomic::{AtomicI64, AtomicU64, Ordering},
        Arc,
    },
    time::{SystemTime, UNIX_EPOCH},
};

use async_trait::async_trait;
use bytes::Bytes;
use chrono::{TimeZone, Utc};
use common_util::error::{BoxError, GenericError};
use futures::{
    stream::{BoxStream, FuturesOrdered},
    StreamExt,
};
use snafu::{ResultExt, Snafu};
use table_kv::{TableKv, WriteBatch, WriteContext};
use tokio::io::AsyncWrite;
use upstream::{
    multipart::{CloudMultiPartUpload, CloudMultiPartUploadImpl, UploadPart},
    path::{Path, DELIMITER},
    Error as StoreError, GetResult, ListResult, MultipartId, ObjectMeta, ObjectStore, Result,
};

use self::meta::{MetaManager, ObkvObjectMeta};

mod meta;

/// The object store type of obkv
pub const OBKV: &str = "OBKV";

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Failed to scan data, namespace:{}, err:{}", namespace, source,))]
    ScanData {
        namespace: String,
        source: GenericError,
    },

    #[snafu(display(
        "Failed to put data, namespace:{}, path:{}, err:{}",
        namespace,
        path,
        source,
    ))]
    PutData {
        namespace: String,
        path: String,
        source: GenericError,
    },

    #[snafu(display(
        "Failed to create shard table, namespace:{}, table_name:{}, err:{}",
        namespace,
        table_name,
        source,
    ))]
    CreateShardTable {
        namespace: String,
        table_name: String,
        source: GenericError,
    },

    #[snafu(display("Failed to read meta, path:{}, err:{}", path, source))]
    ReadMeta { path: String, source: GenericError },

    #[snafu(display("Empty data found in path:{}, index:{}", path, index))]
    EmptyDataPart { path: String, index: usize },

    #[snafu(display("No meta found, path:{}", path,))]
    MetaNotExists { path: String },
}

#[derive(Debug, Clone)]
pub struct ShardManager<T> {
    _client: Arc<T>,
    namespace: String,
    shard_num: usize,
    table_names: Vec<String>,
}

impl<T: TableKv> ShardManager<T> {
    fn try_new(
        client: Arc<T>,
        namespace: String,
        shard_num: usize,
    ) -> std::result::Result<Self, Error> {
        let mut table_names = Vec::with_capacity(shard_num);

        for shard_id in 0..shard_num {
            let table_name = format!("object_store_{shard_id}");
            Self::create_table_if_not_exists(client.clone(), &namespace, &table_name)?;
            table_names.push(table_name);
        }

        Ok(Self {
            _client:client,
            namespace,
            shard_num,
            table_names,
        })
    }

    fn create_table_if_not_exists(
        table_kv: Arc<T>,
        namespace: &str,
        table_name: &str,
    ) -> std::result::Result<(), Error> {
        let table_exists =
            table_kv
                .table_exists(table_name)
                .box_err()
                .context(CreateShardTable {
                    namespace,
                    table_name,
                })?;
        if !table_exists {
            table_kv
                .create_table(table_name)
                .box_err()
                .context(CreateShardTable {
                    namespace,
                    table_name,
                })?;
        }

        Ok(())
    }

    #[inline]
    pub fn oss_shard_table(&self, path: &Path) -> &str {
        let mut hasher = DefaultHasher::new();
        path.as_ref().as_bytes().hash(&mut hasher);
        let hash = hasher.finish();
        let index = hash % (self.shard_num as u64);
        &self.table_names[index as usize]
    }
}

impl<T: TableKv> std::fmt::Display for ShardManager<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "TableManager({},{})", self.namespace, self.shard_num)?;
        Ok(())
    }
}

#[derive(Debug)]
pub struct ObkvObjectStore<T> {
    shard_manager: ShardManager<T>,
    meta_manager: Arc<MetaManager<T>>,
    client: Arc<T>,
    upload_id: AtomicI64,
    part_size: usize,
}

impl<T: TableKv> std::fmt::Display for ObkvObjectStore<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "ObkvObjectStore({:?},{:?})",
            self.client, self.shard_manager
        )?;
        Ok(())
    }
}

impl<T: TableKv> ObkvObjectStore<T> {
    pub fn try_new(
        namespace: String,
        shard_num: usize,
        part_size: usize,
        client: Arc<T>,
    ) -> Result<Self> {
        let shard_manager =
            ShardManager::try_new(client.clone(), namespace, shard_num).map_err(|source| {
                StoreError::Generic {
                    store: OBKV,
                    source: Box::new(source),
                }
            })?;
        let meta_manager = MetaManager {
            client: client.clone(),
        };
        let upload_id = AtomicI64::new(0);
        Ok(Self {
            shard_manager,
            meta_manager: Arc::new(meta_manager),
            client,
            upload_id,
            part_size,
        })
    }

    #[inline]
    fn format_path(location: Option<&Path>) -> Path {
        if let Some(path) = location {
            if !path.as_ref().ends_with(DELIMITER) {
                return Path::from(format!("{}{}", path.as_ref(), DELIMITER));
            }
            return path.clone();
        } else {
            Path::from("")
        }
    }
}

impl<T: TableKv> ObkvObjectStore<T> {
    async fn read_meta(&self, location: &Path) -> std::result::Result<ObkvObjectMeta, Error> {
        let meta = self
            .meta_manager
            .read_meta(location)
            .await
            .box_err()
            .context(ReadMeta {
                path: location.as_ref().to_string(),
            })?;

        if let Some(m) = meta {
            Ok(m)
        } else {
            MetaNotExists {
                path: location.as_ref().to_string(),
            }
            .fail()
        }
    }

    async fn get_internal(&self, location: &Path) -> std::result::Result<GetResult, Error> {
        let meta = self.read_meta(location).await?;
        let table_name = self.shard_manager.oss_shard_table(&location);
        let mut futures = FuturesOrdered::new();
        for path in meta.parts {
            let client = self.client.clone();
            let table_name = table_name.to_string();
            let future = async move {
                match client.get(&table_name, path.as_bytes()) {
                    Ok(res) => Ok(Bytes::from(res.unwrap())),
                    Err(err) => Err(StoreError::Generic {
                        store: OBKV,
                        source: Box::new(err),
                    }),
                }
            };
            futures.push_back(future);
        }

        let boxed = futures.boxed();

        Ok(GetResult::Stream(boxed))
    }
}

#[async_trait]
impl<T: TableKv> ObjectStore for ObkvObjectStore<T> {
    async fn put(&self, location: &Path, bytes: Bytes) -> Result<()> {
        let table_name = self.shard_manager.oss_shard_table(&location);
        let mut batch = T::WriteBatch::default();
        batch.insert(location.as_ref().as_bytes(), bytes.as_ref());
        self.client
            .write(WriteContext::default(), table_name, batch)
            .map_err(|source| StoreError::Generic {
                store: OBKV,
                source: Box::new(source),
            })?;
        Ok(())
    }

    async fn put_multipart(
        &self,
        location: &Path,
    ) -> Result<(MultipartId, Box<dyn AsyncWrite + Unpin + Send>)> {
        let upload_id = self.upload_id.fetch_add(1, Ordering::SeqCst);
        let id = MultipartId::from(format!("{}_{}", self.shard_manager.namespace, upload_id));
        let table_name = self.shard_manager.oss_shard_table(&location);

        let upload = ObkvMultiPartUpload {
            location: location.clone(),
            _upload_id: id.clone(),
            table_name: table_name.to_string(),
            size: AtomicU64::new(0),
            client: Arc::clone(&self.client),
            part_size: self.part_size,
            meta_manager: self.meta_manager.clone(),
        };
        let multi_part_upload = CloudMultiPartUpload::new_with_part_size(upload, 8, self.part_size);
        Ok((id, Box::new(multi_part_upload)))
    }

    async fn abort_multipart(&self, location: &Path, _multipart_id: &MultipartId) -> Result<()> {
        // To abort multipart, we need to delete all data with path `location`.
        // TODO: delete by version
        self.delete(location).await?;
        Ok(())
    }

    async fn get(&self, location: &Path) -> Result<GetResult> {
        self.get_internal(location)
            .await
            .box_err()
            .map_err(|source| StoreError::NotFound {
                path: location.to_string(),
                source: source,
            })
    }

    async fn get_range(&self, location: &Path, range: Range<usize>) -> Result<Bytes> {
        let table_name = self.shard_manager.oss_shard_table(&location);
        let meta =
            self.read_meta(location)
                .await
                .box_err()
                .map_err(|source| StoreError::NotFound {
                    path: location.to_string(),
                    source: source,
                })?;

        let batch_size = meta.part_size;
        let key_list = meta.parts;
        let start_index = range.start / batch_size;
        let start_offset = range.start % batch_size;
        let end_index = range.end / batch_size;
        let end_offset = range.end % batch_size;
        let mut range_buffer = Vec::with_capacity(range.end - range.start);

        for index in start_index..=end_index {
            let key = &key_list[index];
            let values = self
                .client
                .get(table_name, key.as_bytes())
                .map_err(|source| StoreError::NotFound {
                    path: location.to_string(),
                    source: Box::new(source),
                })?;

            if let Some(bytes) = values {
                let bytes = if index == start_index {
                    &bytes[start_offset..]
                } else if index == end_index {
                    &bytes[0..end_offset]
                } else {
                    &bytes
                };

                range_buffer.extend_from_slice(bytes);
            }
        }

        Ok(range_buffer.into())
    }

    /// Return the metadata for the specified location
    async fn head(&self, location: &Path) -> Result<ObjectMeta> {
        let meta =
            self.read_meta(location)
                .await
                .box_err()
                .map_err(|source| StoreError::NotFound {
                    path: location.to_string(),
                    source: source,
                })?;

        Ok(ObjectMeta {
            location: (*location).clone(),
            last_modified: Utc.timestamp_millis_opt(meta.last_modified as i64).unwrap(),
            size: meta.size,
            e_tag: meta.e_tag,
        })
    }

    /// Delete the object at the specified location.
    async fn delete(&self, location: &Path) -> Result<()> {
        // TODO: maybe coerruption here, should not delete data when data is reading.
        let table_name = self.shard_manager.oss_shard_table(&location);
        let meta =
            self.read_meta(location)
                .await
                .box_err()
                .map_err(|source| StoreError::NotFound {
                    path: location.to_string(),
                    source: source,
                })?;

        // delete every part of data
        for part in &meta.parts {
            let key = part.as_bytes();
            self.client
                .delete(table_name, key)
                .map_err(|source| StoreError::Generic {
                    store: OBKV,
                    source: Box::new(source),
                })?;
        }
        // delete meta info
        self.meta_manager
            .delete_meta(meta, location)
            .await
            .map_err(|source| StoreError::Generic {
                store: OBKV,
                source: Box::new(source),
            })?;
        Ok(())
    }

    /// List all the objects with the given prefix.
    /// Prefixes are evaluated on a path segment basis, i.e. `foo/bar/` is a
    /// prefix of `foo/bar/x` but not of `foo/bar_baz/x`.
    /// todo: Currently this method may return lots of object meta, we should
    /// limit the count of return ojects in future.
    async fn list(&self, prefix: Option<&Path>) -> Result<BoxStream<'_, Result<ObjectMeta>>> {
        let path = Self::format_path(prefix);
        let raw_metas =
            self.meta_manager
                .list_meta(&path)
                .await
                .map_err(|source| StoreError::Generic {
                    store: OBKV,
                    source: Box::new(source),
                })?;

        let mut meta_list = Vec::new();
        for meta in raw_metas {
            meta_list.push(Ok(ObjectMeta {
                location: Path::from(meta.location),
                last_modified: Utc.timestamp_millis_opt(meta.last_modified).unwrap(),
                size: meta.size,
                e_tag: meta.e_tag,
            }));
        }

        let iter = futures::stream::iter(meta_list.into_iter());
        Ok(iter.boxed())
    }

    /// List all the objects and common paths(directories) with the given
    /// prefix. Prefixes are evaluated on a path segment basis, i.e.
    /// `foo/bar/` is a prefix of `foo/bar/x` but not of `foo/bar_baz/x`.
    /// see https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListObjectsV2.html
    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> Result<ListResult> {
        let path = Self::format_path(prefix);
        let metas =
            self.meta_manager
                .list_meta(&path)
                .await
                .map_err(|source| StoreError::Generic {
                    store: OBKV,
                    source: Box::new(source),
                })?;

        let mut common_prefixes = HashSet::new();
        let mut objects = Vec::new();
        for meta in metas {
            let location = meta.location;
            let subfix = &location[path.as_ref().len()..];
            if let Some(pos) = subfix.find(DELIMITER) {
                // common_prefix endswith '/'
                let common_prefix = &subfix[0..pos + 1];
                common_prefixes.insert(Path::from(common_prefix));
            } else {
                objects.push(ObjectMeta {
                    location: Path::from(location),
                    last_modified: Utc.timestamp_millis_opt(meta.last_modified).unwrap(),
                    size: meta.size,
                    e_tag: meta.e_tag,
                })
            }
        }

        let common_prefixes = Vec::from_iter(common_prefixes.into_iter());
        Ok(ListResult {
            common_prefixes,
            objects,
        })
    }

    async fn copy(&self, _from: &Path, _to: &Path) -> Result<()> {
        // todo
        Err(StoreError::NotImplemented)
    }

    async fn copy_if_not_exists(&self, _from: &Path, _to: &Path) -> Result<()> {
        // todo
        Err(StoreError::NotImplemented)
    }
}

struct ObkvMultiPartUpload<T> {
    /// The full path to the object
    location: Path,
    /// The id of multi upload tasks
    _upload_id: String,
    /// The table name of obkv to save data
    table_name: String,
    /// The client
    client: Arc<T>,
    /// The size of object
    size: AtomicU64,
    /// The size in bytes of one part. Note: maybe the size of last part less
    /// than part_size.
    part_size: usize,
    /// The mananger to process meta info
    meta_manager: Arc<MetaManager<T>>,
}

#[async_trait]
impl<T: TableKv> CloudMultiPartUploadImpl for ObkvMultiPartUpload<T> {
    async fn put_multipart_part(
        &self,
        buf: Vec<u8>,
        part_idx: usize,
    ) -> Result<UploadPart, std::io::Error> {
        let mut batch = T::WriteBatch::default();
        let key = format!("{}@{}", self.location, part_idx);
        batch.insert(key.as_bytes(), buf.as_ref());
        self.client
            .write(WriteContext::default(), &self.table_name, batch)
            .map_err(|source| StoreError::Generic {
                store: OBKV,
                source: Box::new(source),
            })?;
        // record size of object
        self.size.fetch_add(buf.len() as u64, Ordering::SeqCst);
        Ok(UploadPart { content_id: key })

        // use reqwest::header::ETAG;
        // let part = (part_idx + 1).to_string();

        // let response = self
        //     .client
        //     .put_request(
        //         &self.location,
        //         Some(buf.into()),
        //         &[("partNumber", &part), ("uploadId", &self.upload_id)],
        //     )
        //     .await?;

        // let etag = response
        //     .headers()
        //     .get(ETAG)
        //     .context(MissingEtagSnafu)
        //     .map_err(crate::Error::from)?;

        // let etag = etag
        //     .to_str()
        //     .context(BadHeaderSnafu)
        //     .map_err(crate::Error::from)?;

        // Ok(UploadPart {
        //     content_id: etag.to_string(),
        // })
    }

    async fn complete(&self, completed_parts: Vec<UploadPart>) -> Result<(), std::io::Error> {
        // We should save meta info after finish save data
        let mut paths = Vec::with_capacity(completed_parts.len());
        for upload_part in completed_parts {
            paths.push(upload_part.content_id);
        }

        let now = SystemTime::now();
        let since_epoch = now.duration_since(UNIX_EPOCH).expect("Time went backwards");
        let last_modified = since_epoch.as_millis() as i64;

        let meta = ObkvObjectMeta {
            location: self.location.as_ref().to_string(),
            last_modified,
            size: self.size.load(Ordering::SeqCst) as usize,
            e_tag: None,
            part_size: self.part_size,
            parts: paths,
        };
        // save meta info to specify table
        self.meta_manager
            .save_meta(meta)
            .await
            .map_err(|source| StoreError::Generic {
                store: OBKV,
                source: Box::new(source),
            })?;
        Ok(())
    }
}

// impl<T: TableKv> ObkvMultiPartUpload<T>{
//     pub async fn save_meta(&self, meta: ObkvObjectMeta) -> Result<(),
// std::io::Error>{         let table_name = meta::meta_table();

//         let mut batch = T::WriteBatch::default();
//         let json = meta.encode().map_err(|source| StoreError::Generic {
//             store: "OBKV",
//             source: Box::new(source),
//         })?;
//         batch.insert(self.location.as_ref().as_bytes(), json.as_ref());
//         self.client
//             .write(WriteContext::default(), &table_name, batch)
//             .map_err(|source| StoreError::Generic {
//                 store: "OBKV",
//                 source: Box::new(source),
//             })?;
//             Ok(())
//     }
// }
