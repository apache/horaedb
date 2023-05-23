// Copyright 2023 CeresDB Project Authors. Licensed under Apache-2.0.

use std::{io, pin::Pin, sync::Arc, task::Poll};

/// Implement multipart upload of Object Store, which refer to `object-store` of
/// `arrow-rs`:https://github.com/apache/arrow-rs/blob/master/object_store/src/multipart.rs
use async_trait::async_trait;
use futures::{stream::FuturesUnordered, Future, StreamExt};
use tokio::io::AsyncWrite;
use upstream::Result;

type BoxedTryFuture<T> = Pin<Box<dyn Future<Output = Result<T, io::Error>> + Send>>;

/// A trait that can be implemented by cloud-based objectstores
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
    /// Every task upload part_size data to objectstore.
    tasks: FuturesUnordered<BoxedTryFuture<(usize, UploadPart)>>,
    /// Maximum number of upload tasks to run concurrently
    max_concurrency: usize,
    /// Buffer that will be sent in next upload.
    current_buffer: Vec<u8>,
    /// Size of a part in bytes, size of last part may less than `part_size`.
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

/// [ Methods in this impl are added by ceresdb, not naive `object_store`
/// method ].
impl<T> CloudMultiPartUpload<T>
where
    T: CloudMultiPartUploadImpl + Send + Sync,
{
    /// Send all buffer data to object store in final stage.
    fn final_flush_buffer(mut self: Pin<&mut Self>) {
        let part_size = self.part_size;
        while !self.current_buffer.is_empty() {
            let size = part_size.min(self.current_buffer.len());
            let out_buffer = self.current_buffer.drain(0..size).collect::<Vec<_>>();

            let inner = Arc::clone(&self.inner);
            let part_idx = self.current_part_idx;
            self.tasks.push(Box::pin(async move {
                let upload_part = inner.put_multipart_part(out_buffer, part_idx).await?;
                Ok((part_idx, upload_part))
            }));
            self.current_part_idx += 1;
        }
    }

    /// Send buffer data to object store in write stage.
    fn flush_buffer(mut self: Pin<&mut Self>) {
        let part_size = self.part_size;

        // We will continuously submit tasks until size of the buffer is smaller than
        // `part_size`.
        while self.current_buffer.len() >= part_size {
            let out_buffer = self.current_buffer.drain(0..part_size).collect::<Vec<_>>();
            let inner = Arc::clone(&self.inner);
            let part_idx = self.current_part_idx;
            self.tasks.push(Box::pin(async move {
                let upload_part = inner.put_multipart_part(out_buffer, part_idx).await?;
                Ok((part_idx, upload_part))
            }));
            self.current_part_idx += 1;
            // *enough_to_send = self.current_buffer.len() >= part_size;
        }
    }
}

/// The process of ObjectStore write multipart upload is:
/// First, Call ObjectStore::multi_upload to begin multipart upload.
/// Second, Call AsyncWrite::poll_write to wrtie buffer data to oject,this
/// fuction can be called many times. Last, Call AsyncWrite::poll_shutdown to
/// finish current mulipart upload. See more in
/// [`analytic_engine::sst::parquet::writer::ParquetSstWriter::write`]
impl<T> CloudMultiPartUpload<T>
where
    T: CloudMultiPartUploadImpl + Send + Sync,
{
    // The `poll_flush` function will only flush the in-progress tasks.
    // The `final_flush` method called during `poll_shutdown` will flush
    // the `current_buffer` along with in-progress tasks.
    fn final_flush(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Result<(), io::Error>> {
        // Poll current tasks
        self.as_mut().poll_tasks(cx)?;

        // If current_buffer is not empty, see if it can be submitted
        if !self.current_buffer.is_empty() && self.tasks.len() < self.max_concurrency {
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
        let part_size = self.part_size;
        let enough_to_send = (buf.len() + self.current_buffer.len()) >= part_size;
        if enough_to_send && self.tasks.len() < self.max_concurrency {
            // If we do, copy into the buffer and submit the task, and return ready.
            self.current_buffer.extend_from_slice(buf);
            // Flush buffer data, use custom method
            self.as_mut().flush_buffer();
            // We need to poll immediately after adding to setup waker
            self.as_mut().poll_tasks(cx)?;

            Poll::Ready(Ok(buf.len()))
        } else if !enough_to_send {
            self.current_buffer.extend_from_slice(buf);
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
