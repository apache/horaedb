// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

//! Utilities for `RecordBatch` serialization using Arrow IPC

use std::io::Cursor;

use arrow::{
    ipc::{reader::StreamReader, writer::StreamWriter},
    record_batch::RecordBatch,
};
use snafu::{Backtrace, OptionExt, ResultExt, Snafu};

#[derive(Snafu, Debug)]
#[snafu(visibility(pub))]
pub enum Error {
    #[snafu(display("Arrow error, err:{}.\nBacktrace:\n{}", source, backtrace))]
    ArrowError {
        source: arrow::error::ArrowError,
        backtrace: Backtrace,
    },

    #[snafu(display("Zstd decode error, err:{}.\nBacktrace:\n{}", source, backtrace))]
    ZstdError {
        source: std::io::Error,
        backtrace: Backtrace,
    },

    #[snafu(display("Try to encode without record batches.\nBacktrace:\n{}", backtrace))]
    EncodeWithoutRecordBatch { backtrace: Backtrace },

    #[snafu(display("Failed to decode record batch.\nBacktrace:\n{}", backtrace))]
    Decode { backtrace: Backtrace },
}

type Result<T> = std::result::Result<T, Error>;

#[derive(Copy, Clone)]
pub enum Compression {
    None,
    Zstd,
}

// https://facebook.github.io/zstd/zstd_manual.html
// The lower the level, the faster the speed (at the cost of compression).
const ZSTD_LEVEL: i32 = 3;

/// Encoder that can encode a batch of record batches with specific compression
/// method.
pub struct RecordBatchesEncoder {
    stream_writer: Option<StreamWriter<Vec<u8>>>,
    compression: Compression,
    num_rows: usize,
}

impl RecordBatchesEncoder {
    pub fn new(compression: Compression) -> Self {
        Self {
            stream_writer: None,
            compression,
            num_rows: 0,
        }
    }

    /// Get the number of rows that have been encoded.
    pub fn num_rows(&self) -> usize {
        self.num_rows
    }

    /// Append one batch into the encoder for encoding.
    pub fn write(&mut self, batch: &RecordBatch) -> Result<()> {
        let stream_writer = if let Some(v) = &mut self.stream_writer {
            v
        } else {
            let buffer: Vec<u8> = Vec::new();
            let stream_writer =
                StreamWriter::try_new(buffer, &batch.schema()).context(ArrowError)?;
            self.stream_writer = Some(stream_writer);
            self.stream_writer.as_mut().unwrap()
        };

        stream_writer.write(batch).context(ArrowError)?;
        self.num_rows += batch.num_rows();
        Ok(())
    }

    /// Finish encoding and generate the final encoded bytes.
    pub fn finish(mut self) -> Result<Vec<u8>> {
        let stream_writer = self
            .stream_writer
            .take()
            .context(EncodeWithoutRecordBatch)?;

        let encoded_bytes = stream_writer.into_inner().context(ArrowError)?;

        match self.compression {
            Compression::None => Ok(encoded_bytes),
            Compression::Zstd => {
                zstd::stream::encode_all(Cursor::new(encoded_bytes), ZSTD_LEVEL).context(ZstdError)
            }
        }
    }
}

pub fn encode_record_batch(batch: &RecordBatch, compression: Compression) -> Result<Vec<u8>> {
    let mut encoder = RecordBatchesEncoder::new(compression);
    encoder.write(batch)?;
    encoder.finish()
}

pub fn decode_record_batch(bytes: Vec<u8>, compression: Compression) -> Result<RecordBatch> {
    let bytes = match compression {
        Compression::None => bytes,
        Compression::Zstd => zstd::stream::decode_all(Cursor::new(bytes)).context(ZstdError)?,
    };

    let mut stream_reader = StreamReader::try_new(Cursor::new(bytes), None).context(ArrowError)?;
    stream_reader.next().context(Decode)?.context(ArrowError)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::{
        array::{Int32Array, StringArray},
        datatypes::{DataType, Field, Schema},
    };

    use super::*;

    fn create_batch(rows: usize) -> RecordBatch {
        let schema = Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Utf8, false),
        ]);

        let a = Int32Array::from_iter_values(0..rows as i32);
        let b = StringArray::from_iter_values((0..rows).map(|i| i.to_string()));

        RecordBatch::try_new(Arc::new(schema), vec![Arc::new(a), Arc::new(b)]).unwrap()
    }

    #[test]
    fn test_ipc_encode_decode() {
        let batch = create_batch(1024);
        for compression in &[Compression::None, Compression::Zstd] {
            let bytes = encode_record_batch(&batch, *compression).unwrap();
            assert_eq!(batch, decode_record_batch(bytes, *compression).unwrap());
        }
    }
}
