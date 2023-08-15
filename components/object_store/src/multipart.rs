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

//! Implement multipart upload of [ObjectStore](upstream::ObjectStore), and most
//! of the codes are forked from `arrow-rs`:https://github.com/apache/arrow-rs/blob/master/object_store/src/multipart.rs

use std::{io, pin::Pin, sync::Arc, task::Poll};

use async_trait::async_trait;
use futures::{stream::FuturesUnordered, Future, StreamExt};
use tokio::io::AsyncWrite;
use upstream::Result;

type BoxedTryFuture<T> = Pin<Box<dyn Future<Output = Result<T, io::Error>> + Send>>;

/// A trait that can be implemented by cloud-based object stores
/// and used in combination with [`CloudMultiPartUpload`] to provide
/// multipart upload support.
#[async_trait]
pub trait CloudMultiPartUploadImpl: 'static {
    /// Upload a single part
    async fn put_multipart_part(
        &self,
        buf: Vec<u8>,
        part_idx: usize,
    ) -> Result<UploadPart, io::Error>;

    /// Complete the upload with the provided parts
    ///
    /// `completed_parts` is in order of part number
    async fn complete(&self, completed_parts: Vec<UploadPart>) -> Result<(), io::Error>;
}

#[derive(Debug, Clone)]
pub struct UploadPart {
    pub content_id: String,
}

pub struct CloudMultiPartUpload<T>
where
    T: CloudMultiPartUploadImpl,
{
    inner: Arc<T>,
    /// A list of completed parts, in sequential order.
    completed_parts: Vec<Option<UploadPart>>,
    /// Part upload tasks currently running.
    ///
    /// Every task uploads data with `part_size` to objectstore.
    tasks: FuturesUnordered<BoxedTryFuture<(usize, UploadPart)>>,
    /// Maximum number of upload tasks to run concurrently
    max_concurrency: usize,
    /// Buffer that will be sent in next upload.
    ///
    /// TODO: Maybe we can use a list of Vec<u8> to ensure every buffer is
    /// aligned with the part_size to avoid any extra copy in the future.
    current_buffer: Vec<u8>,
    /// Size of a part in bytes, size of last part may be smaller than
    /// `part_size`.
    part_size: usize,
    /// Index of current part
    current_part_idx: usize,
    /// The completion task
    completion_task: Option<BoxedTryFuture<()>>,
}

impl<T> CloudMultiPartUpload<T>
where
    T: CloudMultiPartUploadImpl,
{
    pub fn new(inner: T, max_concurrency: usize, part_size: usize) -> Self {
        Self {
            inner: Arc::new(inner),
            completed_parts: Vec::new(),
            tasks: FuturesUnordered::new(),
            max_concurrency,
            current_buffer: Vec::new(),
            part_size,
            current_part_idx: 0,
            completion_task: None,
        }
    }

    pub fn poll_tasks(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Result<(), io::Error> {
        if self.tasks.is_empty() {
            return Ok(());
        }
        while let Poll::Ready(Some(res)) = self.tasks.poll_next_unpin(cx) {
            let (part_idx, part) = res?;
            let total_parts = self.completed_parts.len();
            self.completed_parts
                .resize(std::cmp::max(part_idx + 1, total_parts), None);
            self.completed_parts[part_idx] = Some(part);
        }
        Ok(())
    }
}

/// **Note: Methods in this impl are added by ceresdb, not included in the
/// `object_store` crate.**
impl<T> CloudMultiPartUpload<T>
where
    T: CloudMultiPartUploadImpl + Send + Sync,
{
    /// Send all buffer data to object store in final stage.
    fn final_flush_buffer(mut self: Pin<&mut Self>) {
        while !self.current_buffer.is_empty() {
            let size = self.part_size.min(self.current_buffer.len());
            let out_buffer = self.current_buffer.drain(0..size).collect::<Vec<_>>();

            self.as_mut().submit_part_upload_task(out_buffer);
        }
    }

    /// Send buffer data to object store in write stage.
    fn flush_buffer(mut self: Pin<&mut Self>) {
        let part_size = self.part_size;

        // We will continuously submit tasks until size of the buffer is smaller than
        // `part_size`.
        while self.current_buffer.len() >= part_size {
            let out_buffer = self.current_buffer.drain(0..part_size).collect::<Vec<_>>();
            self.as_mut().submit_part_upload_task(out_buffer);
        }
    }

    fn submit_part_upload_task(mut self: Pin<&mut Self>, out_buffer: Vec<u8>) {
        let inner = Arc::clone(&self.inner);
        let part_idx = self.current_part_idx;
        self.tasks.push(Box::pin(async move {
            let upload_part = inner.put_multipart_part(out_buffer, part_idx).await?;

            Ok((part_idx, upload_part))
        }));
        self.current_part_idx += 1;
    }
}

/// The process of ObjectStore write multipart upload is:
/// - Obtain a `AsyncWrite` by `ObjectStore::multi_upload` to begin multipart
///   upload;
/// - Write all the data parts by `AsyncWrite::poll_write`;
/// - Call `AsyncWrite::poll_shutdown` to finish current mulipart upload;
///
/// The `multi_upload` is used in
/// [`analytic_engine::sst::parquet::writer::ParquetSstWriter::write`].
impl<T> CloudMultiPartUpload<T>
where
    T: CloudMultiPartUploadImpl + Send + Sync,
{
    /// Compared with `poll_flush` which only flushes the in-progress tasks,
    /// `final_flush` is called during `poll_shutdown`, and will flush the
    /// `current_buffer` along with in-progress tasks.
    fn final_flush(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Result<(), io::Error>> {
        // Poll current tasks
        self.as_mut().poll_tasks(cx)?;

        // If current_buffer is not empty, see if it can be submitted
        if self.tasks.len() < self.max_concurrency {
            self.as_mut().final_flush_buffer();
        }

        self.as_mut().poll_tasks(cx)?;

        // If tasks and current_buffer are empty, return Ready
        if self.tasks.is_empty() && self.current_buffer.is_empty() {
            Poll::Ready(Ok(()))
        } else {
            Poll::Pending
        }
    }
}

impl<T> AsyncWrite for CloudMultiPartUpload<T>
where
    T: CloudMultiPartUploadImpl + Send + Sync,
{
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> Poll<Result<usize, io::Error>> {
        // Poll current tasks
        self.as_mut().poll_tasks(cx)?;

        // If adding buf to pending buffer would trigger send, check
        // whether we have capacity for another task.
        let enough_to_send = (buf.len() + self.current_buffer.len()) >= self.part_size;
        // The current buffer is not enough to send.
        if !enough_to_send {
            self.current_buffer.extend_from_slice(buf);
            return Poll::Ready(Ok(buf.len()));
        }

        if self.tasks.len() < self.max_concurrency {
            // If we do, copy into the buffer and submit the task, and return ready.
            self.current_buffer.extend_from_slice(buf);
            // Flush buffer data, use custom method
            self.as_mut().flush_buffer();
            // We need to poll immediately after adding to setup waker
            self.as_mut().poll_tasks(cx)?;

            Poll::Ready(Ok(buf.len()))
        } else {
            // Waker registered by call to poll_tasks at beginning
            Poll::Pending
        }
    }

    fn poll_flush(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Result<(), io::Error>> {
        // Poll current tasks
        self.as_mut().poll_tasks(cx)?;

        // If tasks is empty, return Ready
        if self.tasks.is_empty() {
            Poll::Ready(Ok(()))
        } else {
            Poll::Pending
        }
    }

    fn poll_shutdown(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Result<(), io::Error>> {
        // First, poll flush all buffer data to object store.
        match self.as_mut().final_flush(cx) {
            Poll::Pending => return Poll::Pending,
            Poll::Ready(res) => res?,
        };

        // If shutdown task is not set, set it.
        let parts = std::mem::take(&mut self.completed_parts);
        let parts = parts
            .into_iter()
            .enumerate()
            .map(|(idx, part)| {
                part.ok_or_else(|| {
                    io::Error::new(
                        io::ErrorKind::Other,
                        format!("Missing information for upload part {idx}"),
                    )
                })
            })
            .collect::<Result<_, _>>()?;

        let inner = Arc::clone(&self.inner);
        // Last, do completion task in inner.
        let completion_task = self.completion_task.get_or_insert_with(|| {
            Box::pin(async move {
                inner.complete(parts).await?;
                Ok(())
            })
        });

        Pin::new(completion_task).poll(cx)
    }
}
