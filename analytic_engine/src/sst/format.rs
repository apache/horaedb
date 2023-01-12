// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

use std::{
    collections::VecDeque,
    fmt,
    ops::Range,
    sync::{Arc, Mutex, RwLock},
};

use async_trait::async_trait;
use bytes::Bytes;
use common_util::error::{GenericError, GenericResult, MsgErrWithBacktrace};
use object_store::{ObjectStoreRef, Path};

pub type FileChunkReaderRef = Arc<dyn FileChunkReader>;

#[async_trait]
pub trait FileChunkReader: Send + Sync + fmt::Debug {
    async fn file_size(&self) -> GenericResult<usize>;

    async fn get_byte_range(&self, range: Range<usize>) -> GenericResult<Bytes>;

    async fn get_byte_ranges(&self, ranges: &[Range<usize>]) -> GenericResult<Vec<Bytes>>;
}

pub type FileChunkWriteRef = Arc<dyn FileChunkWriter>;

#[async_trait]
pub trait FileChunkWriter: Send + Sync + fmt::Debug {
    async fn append(&self, bytes: Vec<u8>) -> GenericResult<()>;

    async fn finish(&self) -> GenericResult<usize>;
}

#[derive(Debug)]
/// A [`FileChunkReader`] implementation based on [`ObjectStore`].
pub struct FileChunkReaderOnObjectStore {
    path: Path,
    store: ObjectStoreRef,
    cached_file_size: RwLock<Option<usize>>,
}

impl FileChunkReaderOnObjectStore {
    pub fn new(path: Path, store: ObjectStoreRef) -> Self {
        Self {
            path,
            store,
            cached_file_size: RwLock::new(None),
        }
    }
}

#[async_trait]
impl FileChunkReader for FileChunkReaderOnObjectStore {
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

#[derive(Debug)]
/// A [`FileChunkWriter`] implementation based on [`ObjectStore`].
pub struct FileChunkWriterOnObjectStore {
    path: Path,
    store: ObjectStoreRef,
    chunks: Mutex<VecDeque<Vec<u8>>>,
}

impl FileChunkWriterOnObjectStore {
    pub fn new(path: Path, store: ObjectStoreRef) -> Self {
        Self {
            path,
            store,
            chunks: Mutex::new(VecDeque::new()),
        }
    }
}

#[async_trait]
impl FileChunkWriter for FileChunkWriterOnObjectStore {
    async fn append(&self, chunk: Vec<u8>) -> GenericResult<()> {
        self.chunks.lock().unwrap().push_front(chunk);
        Ok(())
    }

    async fn finish(&self) -> GenericResult<usize> {
        let payload = {
            let mut chunks = self.chunks.lock().unwrap();
            if chunks.is_empty() {
                return Err(
                    MsgErrWithBacktrace::new("empty payload to persist".to_string()).boxed(),
                );
            }

            let total_size: usize = chunks.iter().map(|v| v.len()).sum();
            let mut first_chunk = chunks.pop_front().unwrap();
            first_chunk.reserve(total_size - first_chunk.len());
            let remaining_chunks = chunks.iter();
            for chunk in remaining_chunks {
                first_chunk.extend_from_slice(chunk);
            }
            first_chunk
        };

        let size = payload.len();
        self.store
            .put(&self.path, payload.into())
            .await
            .map_err(Box::new)?;
        Ok(size)
    }
}
