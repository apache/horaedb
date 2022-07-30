// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

//! Encoding for Wal logs

use common_types::bytes::BytesMut;
use common_util::codec::{Decoder, Encoder};
use snafu::ResultExt;

use crate::{
    kv_encoder::{
        LogKey, LogKeyEncoder, LogValueDecoder, LogValueEncoder, Namespace,
        NEWEST_LOG_KEY_ENCODING_VERSION, NEWEST_LOG_VALUE_ENCODING_VERSION,
    },
    log_batch::Payload,
    manager::{self},
};

#[derive(Debug, Clone)]
pub struct LogEncoding {
    key_enc: LogKeyEncoder,
    value_enc: LogValueEncoder,
    // value decoder is created dynamically from the version,
    value_enc_version: u8,
}

impl LogEncoding {
    pub fn newest() -> Self {
        Self {
            key_enc: LogKeyEncoder {
                version: NEWEST_LOG_KEY_ENCODING_VERSION,
                namespace: Namespace::Log,
            },
            value_enc: LogValueEncoder {
                version: NEWEST_LOG_VALUE_ENCODING_VERSION,
            },
            value_enc_version: NEWEST_LOG_VALUE_ENCODING_VERSION,
        }
    }

    // Encode [LogKey] into `buf` and caller should knows that the keys are ordered
    // by ([RegionId], [SequenceNum]) so the caller can use this method to
    // generate min/max key in specific scope(global or in some region).
    pub fn encode_key(&self, buf: &mut BytesMut, log_key: &LogKey) -> manager::Result<()> {
        buf.clear();
        buf.reserve(self.key_enc.estimate_encoded_size(log_key));
        self.key_enc
            .encode(buf, log_key)
            .map_err(|e| Box::new(e) as _)
            .context(manager::Encoding)?;

        Ok(())
    }

    pub fn encode_value(&self, buf: &mut BytesMut, payload: &dyn Payload) -> manager::Result<()> {
        buf.clear();
        buf.reserve(self.value_enc.estimate_encoded_size(payload));
        self.value_enc
            .encode(buf, payload)
            .map_err(|e| Box::new(e) as _)
            .context(manager::Encoding)
    }

    pub fn is_log_key(&self, mut buf: &[u8]) -> manager::Result<bool> {
        self.key_enc
            .is_valid(&mut buf)
            .map_err(|e| Box::new(e) as _)
            .context(manager::Decoding)
    }

    pub fn decode_key(&self, mut buf: &[u8]) -> manager::Result<LogKey> {
        self.key_enc
            .decode(&mut buf)
            .map_err(|e| Box::new(e) as _)
            .context(manager::Decoding)
    }

    pub fn decode_value<'a>(&self, buf: &'a [u8]) -> manager::Result<&'a [u8]> {
        let value_dec = LogValueDecoder {
            version: self.value_enc_version,
        };

        value_dec
            .decode(buf)
            .map_err(|e| Box::new(e) as _)
            .context(manager::Decoding)
    }
}
