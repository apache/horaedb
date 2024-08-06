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

use std::task::{Context, Poll};

use bytes::Bytes;
use futures::ready;
use tokio::task::JoinSet;
pub use upstream::PutPayloadMut;
use upstream::{Error, MultipartUpload, PutPayload, PutResult};

#[derive(Debug)]
pub struct WriteMultipart {
    upload: Box<dyn MultipartUpload>,

    buffer: PutPayloadMut,

    chunk_size: usize,

    tasks: JoinSet<Result<(), Error>>,
}

impl WriteMultipart {
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
