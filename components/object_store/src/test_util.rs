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

use std::{collections::HashMap, fmt::Display, ops::Range, sync::RwLock};

use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::{self, BoxStream};
use tokio::io::AsyncWrite;
use upstream::{path::Path, GetResult, ListResult, MultipartId, ObjectMeta, ObjectStore, Result};

#[derive(Debug)]
struct StoreError {
    path: Path,
    msg: String,
}

impl Display for StoreError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StoreError")
            .field("path", &self.path)
            .field("msg", &self.msg)
            .finish()
    }
}

impl std::error::Error for StoreError {}

/// A memory based object store implementation, mainly used for testing.
#[derive(Debug, Default)]
pub struct MemoryStore {
    files: RwLock<HashMap<Path, Bytes>>,
    get_range_counts: RwLock<HashMap<Path, usize>>,
}

impl Display for MemoryStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MemoryStore")
            .field("counts", &self.get_counts())
            .finish()
    }
}

impl MemoryStore {
    pub fn get_counts(&self) -> HashMap<Path, usize> {
        let counts = self.get_range_counts.read().unwrap();
        counts.clone().into_iter().collect()
    }
}

#[async_trait]
impl ObjectStore for MemoryStore {
    async fn put(&self, location: &Path, bytes: Bytes) -> Result<()> {
        let mut files = self.files.write().unwrap();
        files.insert(location.clone(), bytes);
        Ok(())
    }

    async fn get(&self, location: &Path) -> Result<GetResult> {
        let files = self.files.read().unwrap();
        if let Some(bs) = files.get(location) {
            let bs = bs.clone();
            Ok(GetResult::Stream(Box::pin(stream::once(
                async move { Ok(bs) },
            ))))
        } else {
            let source = Box::new(StoreError {
                msg: "not found".to_string(),
                path: location.clone(),
            });
            Err(upstream::Error::Generic {
                store: "get",
                source,
            })
        }
    }

    async fn get_range(&self, location: &Path, range: Range<usize>) -> Result<Bytes> {
        {
            let mut counts = self.get_range_counts.write().unwrap();
            counts
                .entry(location.clone())
                .and_modify(|c| *c += 1)
                .or_insert(1);
        }

        let files = self.files.read().unwrap();
        if let Some(bs) = files.get(location) {
            Ok(bs.slice(range))
        } else {
            let source = Box::new(StoreError {
                msg: "not found".to_string(),
                path: location.clone(),
            });
            Err(upstream::Error::Generic {
                store: "get_range",
                source,
            })
        }
    }

    async fn head(&self, location: &Path) -> Result<ObjectMeta> {
        let files = self.files.read().unwrap();

        if let Some(bs) = files.get(location) {
            Ok(ObjectMeta {
                location: location.clone(),
                size: bs.len(),
                last_modified: Default::default(),
            })
        } else {
            let source = Box::new(StoreError {
                msg: "not found".to_string(),
                path: location.clone(),
            });
            Err(upstream::Error::Generic {
                store: "head",
                source,
            })
        }
    }

    async fn put_multipart(
        &self,
        _location: &Path,
    ) -> Result<(MultipartId, Box<dyn AsyncWrite + Unpin + Send>)> {
        unimplemented!()
    }

    async fn abort_multipart(&self, _location: &Path, _multipart_id: &MultipartId) -> Result<()> {
        unimplemented!()
    }

    async fn delete(&self, _location: &Path) -> Result<()> {
        unimplemented!()
    }

    async fn list(&self, _prefix: Option<&Path>) -> Result<BoxStream<'_, Result<ObjectMeta>>> {
        unimplemented!()
    }

    async fn list_with_delimiter(&self, _prefix: Option<&Path>) -> Result<ListResult> {
        unimplemented!()
    }

    async fn copy(&self, _from: &Path, _to: &Path) -> Result<()> {
        unimplemented!()
    }

    async fn rename(&self, _from: &Path, _to: &Path) -> Result<()> {
        unimplemented!()
    }

    async fn copy_if_not_exists(&self, _from: &Path, _to: &Path) -> Result<()> {
        unimplemented!()
    }

    async fn rename_if_not_exists(&self, _from: &Path, _to: &Path) -> Result<()> {
        unimplemented!()
    }
}
