// Copyright 2023 CeresDB Project Authors. Licensed under Apache-2.0.

use std::{
    collections::hash_map::DefaultHasher,
    fmt::format,
    hash::{Hash, Hasher},
    ops::Range,
    sync::{
        atomic::{AtomicI64, AtomicU64, Ordering},
        Arc,
    },
    time::{self, Instant, SystemTime, UNIX_EPOCH},
};

use async_trait::async_trait;
use bytes::Bytes;
use chrono::{DateTime, Duration, Utc};
use common_util::error::{BoxError, GenericError};
use futures::{
    stream::{BoxStream, StreamFuture},
    Stream, StreamExt,
};
use snafu::{ResultExt, Snafu};
use table_kv::{
    ScanContext as KvScanContext, ScanIter, ScanRequest, TableError, TableKv, WriteBatch,
    WriteContext,
};
use tokio::io::AsyncWrite;
use upstream::{
    aws::AmazonS3,
    multipart::{CloudMultiPartUpload, CloudMultiPartUploadImpl, UploadPart},
    path::{self, Path, DELIMITER},
    Error as StoreError, GetResult, ListResult, MultipartId, ObjectMeta, ObjectStore, Result,
};

use self::meta::ObkvObjectMeta;
use crate::ObjectStoreRef;

mod meta;

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

    #[snafu(display("Failed to get meta, namespace:{}, path:{}", namespace, path,))]
    MetaNotExists { namespace: String, path: String },
}

#[derive(Debug, Clone)]
pub struct TableManager {
    namespace: String,
    shard_num: usize,
    table_names: Vec<String>,
}

impl TableManager {
    fn new(namespace: String, shard_num: usize) -> Result<Self> {
        let mut table_names = Vec::with_capacity(shard_num);

        for shard_id in 0..shard_num {
            let table_name = format!("ceresdb_oss_{shard_id}");
            table_names.push(table_name);
        }

        Ok(Self {
            namespace,
            shard_num,
            table_names,
        })
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

impl std::fmt::Display for TableManager {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "TableManager({},{})", self.namespace, self.shard_num);
        Ok(())
    }
}

#[derive(Debug)]
pub struct ObkvObjectStore<T> {
    manager: TableManager,
    client: Arc<T>,
    upload_id: AtomicI64,
    part_size: u64,
}

impl<T: TableKv> std::fmt::Display for ObkvObjectStore<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "ObkvObjectStore({:?},{:?})", self.client, self.manager)?;
        Ok(())
    }
}

impl<T: TableKv> ObkvObjectStore<T> {
    pub fn try_new(
        namespace: String,
        shard_num: usize,
        part_size: u64,
        client: Arc<T>,
    ) -> Result<Self> {
        let manager = TableManager::new(namespace, shard_num)?;
        let upload_id = AtomicI64::new(0);
        Ok(Self {
            manager,
            client,
            upload_id,
            part_size,
        })
    }
}

#[async_trait]
impl<T: TableKv> ObjectStore for ObkvObjectStore<T> {
    async fn put(&self, location: &Path, bytes: Bytes) -> Result<()> {
        let table_name = self.manager.oss_shard_table(&location);
        let mut batch = T::WriteBatch::default();
        batch.insert(location.as_ref().as_bytes(), bytes.as_ref());
        self.client
            .write(WriteContext::default(), table_name, batch)
            .map_err(|source| StoreError::Generic {
                store: "OBKV",
                source: Box::new(source),
            })?;
        Ok(())
    }

    async fn put_multipart(
        &self,
        location: &Path,
    ) -> Result<(MultipartId, Box<dyn AsyncWrite + Unpin + Send>)> {
        let upload_id = self.upload_id.fetch_add(1, Ordering::SeqCst);
        let id = MultipartId::from(format!("{}_{}", self.manager.namespace, upload_id));
        let table_name = self.manager.oss_shard_table(&location);

        let upload = ObkvMultiPartUpload {
            location: location.clone(),
            upload_id: id.clone(),
            table_name: table_name.to_string(),
            size: AtomicU64::new(0),
            client: Arc::clone(&self.client),
            part_size: self.part_size,
        };
        Ok((id, Box::new(CloudMultiPartUpload::new(upload, 8))))
    }

    async fn abort_multipart(&self, location: &Path, multipart_id: &MultipartId) -> Result<()> {
        // To abort multipart, we need to delete all data with path `location`.
        self.delete(location).await?;
        Ok(())
    }

    async fn get(&self, location: &Path) -> Result<GetResult> {
        // Will need dynamodb_lock
        let table_name = self.manager.oss_shard_table(&location);
        let meta = meta::ObkvObjectMeta::read_meta(Arc::new(self.client), location)
            .await
            .map_err(|source| StoreError::Generic {
                store: "OBKV",
                source: Box::new(source),
            })?;

        let client = self.client.clone();
        if let Some(m) = meta {
            let mut streams = Vec::new();
            for path in m.parts {
                let client = self.client.clone();
                let table_name = table_name.clone();
                let stream = async move { client.get(table_name, path.as_bytes()).await }.map_err(
                    |source| StoreError::Generic {
                        store: "OBKV",
                        source: Box::new(source),
                    },
                );
                streams.push(stream.boxed());
            }

            let stream = futures::stream::select_all(streams)
                .map(|res| {
                    res.map_err(|source| StoreError::Generic {
                        store: "OBKV",
                        source: Box::new(source),
                    })
                })
                .map(|res| res.map(Bytes::from));

            Ok(GetResult::Stream(stream.boxed()))
        } else {
            Err(MetaNotExists {
                namespace: self.namespace,
                path: location.to_string(),
            })
        }
    }

    async fn get_range(&self, location: &Path, range: Range<usize>) -> Result<Bytes> {
        let table_name = self.manager.oss_shard_table(&location);
        let meta = meta::ObkvObjectMeta::read_meta(Arc::new(self.client), location)
            .await
            .map_err(|source| StoreError::Generic {
                store: "OBKV",
                source: Box::new(source),
            })?;

        if let Some(m) = meta {
            let batch_size = m.part_size;
            let key_list = m.parts;
            let start_index = range.start / batch_size;
            let start_offset = range.start % batch_size;
            let end_index = range.end / batch_size;
            let end_offset = range.end % batch_size;

            for index in start_index..end_index + 1 {
                let key = key_list[index];
                let values = self.client.get(table_name, key.as_bytes()).await?;
                if index == start_index {
                    range_buffer.append(values[start_offset..]);
                } else if index == end_index {
                    range_buffer.append(values[0..end_offset]);
                } else {
                    range_buffer.append(values);
                }
            }
            Ok(range_buffer.into())
        } else {
            MetaNotExists {
                namespace: self.namespace,
                path: location.as_ref().to_string(),
            }
        }
    }

    /// Return the metadata for the specified location
    async fn head(&self, location: &Path) -> Result<ObjectMeta> {
        // Will need dynamodb_lock
        let table_name = self.manager.oss_shard_table(&location);
        let meta = meta::ObkvObjectMeta::read_meta(Arc::new(self.client), location)
            .await
            .map_err(|source| StoreError::Generic {
                store: "OBKV",
                source: Box::new(source),
            })?;
        if let Some(m) = meta {
            ObjectMeta {
                location: location,
                last_modified: m.last_modified,
                size: m.size,
                e_tag: m.e_tag,
            }
        } else {
            MetaNotExists {
                namespace: self.namespace,
                path: location.as_ref().to_string(),
            }
        }
    }

    /// Delete the object at the specified location.
    async fn delete(&self, location: &Path) -> Result<()> {
        let table_name = self.manager.oss_shard_table(&location);

        let scan_context: KvScanContext = KvScanContext {
            timeout: time::Duration::from_secs(10),
            batch_size: 1000,
        };
        let start = table_kv::KeyBoundary::included(b"min");
        let end = table_kv::KeyBoundary::excluded(b"max");
        let request = ScanRequest {
            start,
            end,
            reverse: false,
        };
        let mut iter = self
            .client
            .scan(scan_context, table_name, request)
            .map_err(|source| StoreError::Generic {
                store: "OBKV",
                source: Box::new(source),
            })?;

        // delete every part of data
        while iter.next().unwrap() {
            let key = iter.key();
            self.client
                .delete(table_name, key)
                .map_err(|source| StoreError::Generic {
                    store: "OBKV",
                    source: Box::new(source),
                })?;
            let value = iter.value();
        }

        // delete meta info
        meta::ObkvObjectMeta::delete_meta(self.client.clone(), location).await?;

        Ok(())
    }

    /// List all the objects with the given prefix.
    ///
    /// Prefixes are evaluated on a path segment basis, i.e. `foo/bar/` is a
    /// prefix of `foo/bar/x` but not of `foo/bar_baz/x`.
    async fn list(&self, prefix: Option<&Path>) -> Result<BoxStream<'_, Result<ObjectMeta>>> {
        // todo
        Err(StoreError::NotImplemented)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> Result<ListResult> {
        // todo
        Err(StoreError::NotImplemented)
    }

    async fn copy(&self, from: &Path, to: &Path) -> Result<()> {
        // todo
        Err(StoreError::NotImplemented)
    }

    async fn copy_if_not_exists(&self, from: &Path, to: &Path) -> Result<()> {
        // todo
        Err(StoreError::NotImplemented)
    }
}

struct ObkvMultiPartUpload<T> {
    /// The full path to the object
    location: Path,
    /// The id of multi upload tasks
    upload_id: String,
    /// The table name of obkv to save data
    table_name: String,
    /// The client
    client: Arc<T>,
    /// The size of object
    size: AtomicU64,
    /// The size in bytes of one part. Note: maybe the size of last part less
    /// than part_size.
    part_size: u64,
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
                store: "OBKV",
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
        // self.client
        //     .complete_multipart(&self.location, &self.upload_id, completed_parts)
        //     .await?;
        // Do nothing, because multi part upload had been completed in
        // @put_multipart_part; Maybe we can save multiupload part info here.
        let mut paths = Vec::with_capacity(completed_parts.len());
        for upload_part in completed_parts {
            paths.push(upload_part.content_id);
        }

        let now = SystemTime::now();
        let since_epoch = now.duration_since(UNIX_EPOCH).expect("Time went backwards");
        let last_modified = since_epoch.as_millis() as u64;

        let meta = ObkvObjectMeta {
            location: self.location.as_ref().to_string(),
            last_modified,
            size: self.size.load(Ordering::SeqCst),
            e_tag: None,
            part_size: self.part_size,
            parts: paths,
        };
        // save meta info to specify table
        meta.save_meta(self.client.clone())
            .await
            .map_err(|source| StoreError::Generic {
                store: "OBKV",
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
