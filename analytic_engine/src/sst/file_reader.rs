// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

use std::{
    ops::Range,
    sync::{Arc, RwLock},
};

use async_trait::async_trait;
use bytes::Bytes;
use common_util::error::{GenericError, GenericResult};
use object_store::{ObjectStoreRef, Path};

pub type AsyncFileReaderRef = Arc<dyn AsyncFileChunkReader>;

#[async_trait]
pub trait AsyncFileChunkReader: Send + Sync {
    async fn file_size(&self) -> GenericResult<usize>;

    async fn get_byte_range(&self, range: Range<usize>) -> GenericResult<Bytes>;

    async fn get_byte_ranges(&self, ranges: &[Range<usize>]) -> GenericResult<Vec<Bytes>>;
}

pub struct FileReaderOnObjectStore {
    path: Path,
    store: ObjectStoreRef,
    cached_file_size: RwLock<Option<usize>>,
}

impl FileReaderOnObjectStore {
    pub fn new(path: Path, store: ObjectStoreRef) -> Self {
        Self {
            path,
            store,
            cached_file_size: RwLock::new(None),
        }
    }
}

#[async_trait]
impl AsyncFileChunkReader for FileReaderOnObjectStore {
    async fn file_size(&self) -> GenericResult<usize> {
        // check cached filed_size first
        {
            let file_size = self.cached_file_size.read().unwrap();
            if let Some(s) = file_size.as_ref() {
                return Ok(*s);
            }
        }

        // fetch the size from the underlying store
        let head = self
            .store
            .head(&self.path)
            .await
            .map_err(|e| Box::new(e) as GenericError)?;
        *self.cached_file_size.write().unwrap() = Some(head.size);
        Ok(head.size)
    }

    async fn get_byte_range(&self, range: Range<usize>) -> GenericResult<Bytes> {
        self.store
            .get_range(&self.path, range)
            .await
            .map_err(|e| Box::new(e) as _)
    }

    async fn get_byte_ranges(&self, ranges: &[Range<usize>]) -> GenericResult<Vec<Bytes>> {
        self.store
            .get_ranges(&self.path, ranges)
            .await
            .map_err(|e| Box::new(e) as _)
    }
}
