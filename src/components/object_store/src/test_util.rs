// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use std::{collections::HashMap, fmt::Display, ops::Range, sync::RwLock};

use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::{self, BoxStream};
use upstream::{
    path::Path, GetOptions, GetResult, GetResultPayload, ListResult, MultipartUpload, ObjectMeta,
    ObjectStore, PutMultipartOpts, PutOptions, PutPayload, PutResult, Result,
};

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
    async fn put(&self, location: &Path, payload: PutPayload) -> Result<PutResult> {
        let mut files = self.files.write().unwrap();
        files.insert(location.clone(), Bytes::from(payload));
        Ok(PutResult {
            e_tag: None,
            version: None,
        })
    }

    async fn get(&self, location: &Path) -> Result<GetResult> {
        let files = self.files.read().unwrap();
        if let Some(bs) = files.get(location) {
            let bs = bs.clone();
            let size = bs.len();
            let payload = GetResultPayload::Stream(Box::pin(stream::once(async move { Ok(bs) })));
            Ok(GetResult {
                payload,
                meta: ObjectMeta {
                    location: location.clone(),
                    last_modified: Default::default(),
                    size,
                    e_tag: None,
                    version: None,
                },
                range: Default::default(),
                attributes: Default::default(),
            })
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
                e_tag: None,
                last_modified: Default::default(),
                version: None,
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

    async fn put_multipart(&self, _location: &Path) -> Result<Box<dyn MultipartUpload>> {
        unimplemented!()
    }

    async fn delete(&self, _location: &Path) -> Result<()> {
        unimplemented!()
    }

    fn list(&self, _prefix: Option<&Path>) -> BoxStream<'_, Result<ObjectMeta>> {
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

    async fn put_opts(
        &self,
        _location: &Path,
        _payload: PutPayload,
        _opts: PutOptions,
    ) -> Result<PutResult> {
        unimplemented!()
    }

    async fn put_multipart_opts(
        &self,
        _location: &Path,
        _opts: PutMultipartOpts,
    ) -> Result<Box<dyn MultipartUpload>> {
        unimplemented!()
    }

    async fn get_opts(&self, _location: &Path, _options: GetOptions) -> Result<GetResult> {
        unimplemented!()
    }
}
