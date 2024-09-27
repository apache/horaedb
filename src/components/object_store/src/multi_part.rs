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

use std::{
    io::Error as IoError,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use bytes::Bytes;
use futures::{future::BoxFuture, ready, Future, FutureExt};
use tokio::{io::AsyncWrite, sync::Mutex, task::JoinSet};
pub use upstream::PutPayloadMut;
use upstream::{path::Path, Error, MultipartUpload, PutPayload, PutResult};

use crate::ObjectStoreRef;

// TODO: remove Mutex and make ConcurrentMultipartUpload thread-safe
pub type MultiUploadRef = Arc<Mutex<ConcurrentMultipartUpload>>;

const CHUNK_SIZE: usize = 5 * 1024 * 1024;
const MAX_CONCURRENCY: usize = 10;

#[derive(Debug)]
pub struct ConcurrentMultipartUpload {
    upload: Box<dyn MultipartUpload>,

    buffer: PutPayloadMut,

    chunk_size: usize,

    tasks: JoinSet<Result<(), Error>>,
}

impl ConcurrentMultipartUpload {
    pub fn new(upload: Box<dyn MultipartUpload>, chunk_size: usize) -> Self {
        Self {
            upload,
            chunk_size,
            buffer: PutPayloadMut::new(),
            tasks: Default::default(),
        }
    }

    pub fn poll_tasks(
        &mut self,
        cx: &mut Context<'_>,
        max_concurrency: usize,
    ) -> Poll<Result<(), Error>> {
        while !self.tasks.is_empty() && self.tasks.len() >= max_concurrency {
            ready!(self.tasks.poll_join_next(cx)).unwrap()??
        }
        Poll::Ready(Ok(()))
    }

    fn put_part(&mut self, part: PutPayload) {
        self.tasks.spawn(self.upload.put_part(part));
    }

    pub fn put(&mut self, mut bytes: Bytes) {
        while !bytes.is_empty() {
            let remaining = self.chunk_size - self.buffer.content_length();
            if bytes.len() < remaining {
                self.buffer.push(bytes);
                return;
            }
            self.buffer.push(bytes.split_to(remaining));
            let buffer = std::mem::take(&mut self.buffer);
            self.put_part(buffer.into())
        }
    }

    pub fn write(&mut self, mut buf: &[u8]) {
        while !buf.is_empty() {
            let remaining = self.chunk_size - self.buffer.content_length();
            let to_read = buf.len().min(remaining);
            self.buffer.extend_from_slice(&buf[..to_read]);
            if to_read == remaining {
                let buffer = std::mem::take(&mut self.buffer);
                self.put_part(buffer.into())
            }
            buf = &buf[to_read..]
        }
    }

    pub async fn flush(&mut self, max_concurrency: usize) -> Result<(), Error> {
        futures::future::poll_fn(|cx| self.poll_tasks(cx, max_concurrency)).await
    }

    pub async fn finish(&mut self) -> Result<PutResult, Error> {
        if !self.buffer.is_empty() {
            let part = std::mem::take(&mut self.buffer);
            self.put_part(part.into())
        }

        self.flush(0).await?;
        self.upload.complete().await
    }

    pub async fn abort(&mut self) -> Result<(), Error> {
        self.tasks.shutdown().await;
        self.upload.abort().await
    }
}

pub struct MultiUploadWriter {
    pub multi_upload: MultiUploadRef,
    upload_task: Option<BoxFuture<'static, std::result::Result<usize, IoError>>>,
    flush_task: Option<BoxFuture<'static, std::result::Result<(), IoError>>>,
    completion_task: Option<BoxFuture<'static, std::result::Result<(), IoError>>>,
}

impl<'a> MultiUploadWriter {
    pub async fn new(object_store: &'a ObjectStoreRef, location: &'a Path) -> Result<Self, Error> {
        let upload_writer = object_store.put_multipart(location).await?;

        let multi_upload = Arc::new(Mutex::new(ConcurrentMultipartUpload::new(
            upload_writer,
            CHUNK_SIZE,
        )));

        let multi_upload = Self {
            multi_upload,
            upload_task: None,
            flush_task: None,
            completion_task: None,
        };

        Ok(multi_upload)
    }

    pub fn aborter(&self) -> MultiUploadRef {
        self.multi_upload.clone()
    }
}

impl AsyncWrite for MultiUploadWriter {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> Poll<Result<usize, IoError>> {
        let multi_upload = self.multi_upload.clone();
        let buf = buf.to_owned();

        let upload_task = self.upload_task.insert(
            async move {
                multi_upload
                    .lock()
                    .await
                    .flush(MAX_CONCURRENCY)
                    .await
                    .map_err(IoError::other)?;

                multi_upload.lock().await.write(&buf);
                Ok(buf.len())
            }
            .boxed(),
        );

        Pin::new(upload_task).poll(cx)
    }

    fn poll_flush(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Result<(), IoError>> {
        let multi_upload = self.multi_upload.clone();

        let flush_task = self.flush_task.insert(
            async move {
                multi_upload
                    .lock()
                    .await
                    .flush(0)
                    .await
                    .map_err(IoError::other)?;

                Ok(())
            }
            .boxed(),
        );

        Pin::new(flush_task).poll(cx)
    }

    fn poll_shutdown(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Result<(), IoError>> {
        let multi_upload = self.multi_upload.clone();

        let completion_task = self.completion_task.get_or_insert_with(|| {
            async move {
                multi_upload
                    .lock()
                    .await
                    .finish()
                    .await
                    .map_err(IoError::other)?;

                Ok(())
            }
            .boxed()
        });

        Pin::new(completion_task).poll(cx)
    }
}
