// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

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

pub struct AsyncArrowWriter<W> {
    sync_writer: ArrowWriter<SharedBuffer>,
    async_writer: W,
    shared_buffer: SharedBuffer,
}

impl<W: AsyncWrite + Unpin + Send> AsyncArrowWriter<W> {
    pub fn try_new(
        writer: W,
        arrow_schema: SchemaRef,
        props: Option<WriterProperties>,
    ) -> Result<Self> {
        let shared_buffer = SharedBuffer::default();
        let sync_writer = ArrowWriter::try_new(shared_buffer.clone(), arrow_schema, props)?;

        Ok(Self {
            sync_writer,
            async_writer: writer,
            shared_buffer,
        })
    }

    pub async fn write(&mut self, batch: &RecordBatch) -> Result<()> {
        self.sync_writer.write(batch)?;
        Self::flush(&self.shared_buffer, &mut self.async_writer).await
    }

    pub async fn append_key_value_metadata(&mut self, kv_metadata: KeyValue) {
        self.sync_writer.append_key_value_metadata(kv_metadata);
    }

    pub async fn close(mut self) -> Result<FileMetaData> {
        let metadata = self.sync_writer.close()?;
        Self::flush(&self.shared_buffer, &mut self.async_writer).await?;
        self.async_writer
            .shutdown()
            .await
            .map_err(|e| ParquetError::External(Box::new(e)))?;

        Ok(metadata)
    }

    async fn flush(shared_buffer: &SharedBuffer, async_writer: &mut W) -> Result<()> {
        let mut buffer = {
            let mut buffer = shared_buffer.buffer.lock().unwrap();

            if buffer.is_empty() {
                return Ok(());
            }
            std::mem::take(&mut *buffer)
        };

        async_writer
            .write(&buffer)
            .await
            .map_err(|e| ParquetError::External(Box::new(e)))?;

        buffer.clear();
        *shared_buffer.buffer.lock().unwrap() = buffer;
        Ok(())
    }
}
