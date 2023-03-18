// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

//! Async arrow writer for parquet file.

use std::{
    io::Write,
    sync::{Arc, Mutex},
};

use arrow::{datatypes::SchemaRef, record_batch::RecordBatch};
use parquet::{
    arrow::ArrowWriter,
    errors::{ParquetError, Result},
    file::properties::WriterProperties,
    format::{FileMetaData, KeyValue},
};
use tokio::io::{AsyncWrite, AsyncWriteExt};

#[derive(Clone, Default)]
pub struct SharedBuffer {
    /// The buffer to store the data to be written.
    ///
    /// The lock is used to obtain internal mutability, so no worry about the
    /// lock contention.
    buffer: Arc<Mutex<Vec<u8>>>,
}

impl Write for SharedBuffer {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let mut buffer = self.buffer.lock().unwrap();
        Write::write(&mut *buffer, buf)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        let mut buffer = self.buffer.lock().unwrap();
        Write::flush(&mut *buffer)
    }
}

/// Async arrow writer for parquet file.
///
/// A shared buffer is provided to the sync writer [ArrowWriter] and it will
/// accept the data from the sync writer and flush the received data to the
/// async writer when the buffer size exceeds the threshold.
pub struct AsyncArrowWriter<W> {
    sync_writer: ArrowWriter<SharedBuffer>,
    async_writer: W,
    shared_buffer: SharedBuffer,
    max_buffer_size: usize,
}

impl<W: AsyncWrite + Unpin + Send> AsyncArrowWriter<W> {
    pub fn try_new(
        writer: W,
        arrow_schema: SchemaRef,
        max_buffer_size: usize,
        props: Option<WriterProperties>,
    ) -> Result<Self> {
        let shared_buffer = SharedBuffer::default();
        let sync_writer = ArrowWriter::try_new(shared_buffer.clone(), arrow_schema, props)?;

        Ok(Self {
            sync_writer,
            async_writer: writer,
            shared_buffer,
            max_buffer_size,
        })
    }

    pub async fn write(&mut self, batch: &RecordBatch) -> Result<()> {
        self.sync_writer.write(batch)?;
        Self::flush(
            &self.shared_buffer,
            &mut self.async_writer,
            self.max_buffer_size,
        )
        .await
    }

    pub fn append_key_value_metadata(&mut self, kv_metadata: KeyValue) {
        self.sync_writer.append_key_value_metadata(kv_metadata);
    }

    pub async fn close(mut self) -> Result<FileMetaData> {
        let metadata = self.sync_writer.close()?;

        // flush the remaining data.
        Self::flush(&self.shared_buffer, &mut self.async_writer, 0).await?;
        self.async_writer
            .shutdown()
            .await
            .map_err(|e| ParquetError::External(Box::new(e)))?;

        Ok(metadata)
    }

    async fn flush(
        shared_buffer: &SharedBuffer,
        async_writer: &mut W,
        threshold: usize,
    ) -> Result<()> {
        let mut buffer = {
            let mut buffer = shared_buffer.buffer.lock().unwrap();

            if buffer.is_empty() || buffer.len() < threshold {
                // no need to flush
                return Ok(());
            }
            std::mem::take(&mut *buffer)
        };

        async_writer
            .write(&buffer)
            .await
            .map_err(|e| ParquetError::External(Box::new(e)))?;

        // reuse the buffer.
        buffer.clear();
        *shared_buffer.buffer.lock().unwrap() = buffer;
        Ok(())
    }
}
