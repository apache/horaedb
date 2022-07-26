// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

//! Constants and utils for encoding.

use chrono::{TimeZone, Utc};
use common_types::{bytes::BytesMut, time::Timestamp};
use common_util::{
    codec::{Decoder, Encoder},
    config::ReadableDuration,
};
use table_kv::{KeyBoundary, ScanRequest};

use crate::{
    kv_encoder::{
        LogKey, LogKeyEncoder, LogValueDecoder, LogValueEncoder, Result,
        NEWEST_LOG_VALUE_ENCODING_VERSION,
    },
    log_batch::Payload,
    manager::RegionId,
};

/// Key prefix for namespace in meta table.
const META_NAMESPACE_PREFIX: &str = "v1/namespace";
/// Key prefix for bucket in meta table.
const META_BUCKET_PREFIX: &str = "v1/bucket";
/// Format of bucket timestamp.
const BUCKET_TIMESTAMP_FORMAT: &str = "%Y-%m-%dT%H:%M:%S";
/// Key prefix of region meta.
const REGION_META_PREFIX: &str = "v1/region";
/// Format of timestamp in wal table name.
const WAL_SHARD_TIMESTAMP_FORMAT: &str = "%Y%m%d%H%M%S";

#[inline]
pub fn scan_request_for_prefix(prefix: &str) -> ScanRequest {
    ScanRequest {
        start: KeyBoundary::excluded(prefix.as_bytes()),
        end: KeyBoundary::max_included(),
        reverse: false,
    }
}

#[inline]
pub fn format_namespace_key(namespace: &str) -> String {
    format!("{}/{}", META_NAMESPACE_PREFIX, namespace)
}

#[inline]
pub fn bucket_key_prefix(namespace: &str) -> String {
    format!("{}/{}/", META_BUCKET_PREFIX, namespace)
}

pub fn format_timed_bucket_key(
    namespace: &str,
    bucket_duration: ReadableDuration,
    gmt_start_ms: Timestamp,
) -> String {
    let duration = bucket_duration.to_string();

    let dt = Utc.timestamp_millis(gmt_start_ms.as_i64());
    format!(
        "{}/{}/{}/{}",
        META_BUCKET_PREFIX,
        namespace,
        duration,
        dt.format(BUCKET_TIMESTAMP_FORMAT)
    )
}

pub fn format_permanent_bucket_key(namespace: &str) -> String {
    format!("{}/{}/permanent", META_BUCKET_PREFIX, namespace)
}

#[inline]
pub fn format_region_meta_name(namespace: &str, shard_id: usize) -> String {
    format!("region_meta_{}_{:0>6}", namespace, shard_id)
}

#[inline]
pub fn format_timed_wal_name(namespace: &str, gmt_start_ms: Timestamp, shard_id: usize) -> String {
    let dt = Utc.timestamp_millis(gmt_start_ms.as_i64());

    format!(
        "wal_{}_{}_{:0>6}",
        namespace,
        dt.format(WAL_SHARD_TIMESTAMP_FORMAT),
        shard_id
    )
}

#[inline]
pub fn format_permanent_wal_name(namespace: &str, shard_id: usize) -> String {
    format!("wal_{}_permanent_{:0>6}", namespace, shard_id)
}

#[inline]
pub fn format_region_key(region_id: RegionId) -> String {
    format!("{}/{}", REGION_META_PREFIX, region_id)
}

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
            key_enc: LogKeyEncoder::newest(),
            value_enc: LogValueEncoder::newest(),
            value_enc_version: NEWEST_LOG_VALUE_ENCODING_VERSION,
        }
    }

    // Encode [LogKey] into `buf` and caller should knows that the keys are ordered
    // by ([RegionId], [SequenceNum]) so the caller can use this method to
    // generate min/max key in specific scope(global or in some region).
    pub fn encode_key(&self, buf: &mut BytesMut, log_key: &LogKey) -> Result<()> {
        buf.clear();
        buf.reserve(self.key_enc.estimate_encoded_size(log_key));
        self.key_enc.encode(buf, log_key)?;

        Ok(())
    }

    pub fn encode_value(&self, buf: &mut BytesMut, payload: &impl Payload) -> Result<()> {
        buf.clear();
        buf.reserve(self.value_enc.estimate_encoded_size(payload));
        self.value_enc.encode(buf, payload)
    }

    pub fn is_log_key(&self, mut buf: &[u8]) -> Result<bool> {
        self.key_enc.is_valid(&mut buf)
    }

    pub fn decode_key(&self, mut buf: &[u8]) -> Result<LogKey> {
        self.key_enc.decode(&mut buf)
    }

    pub fn decode_value<'a>(&self, buf: &'a [u8]) -> Result<&'a [u8]> {
        let value_dec = LogValueDecoder {
            version: self.value_enc_version,
        };

        value_dec.decode(buf)
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;
    use crate::{
        log_batch::PayloadDecoder,
        table_kv_impl::namespace,
        tests::util::{TestPayload, TestPayloadDecoder},
    };

    #[test]
    fn test_format_namespace_key() {
        let ns = "aabbcc";
        let key = format_namespace_key(ns);
        assert_eq!("v1/namespace/aabbcc", key);
    }

    #[test]
    fn test_format_bucket_key() {
        let ns = "aabbcc";
        let prefix = bucket_key_prefix(ns);
        assert_eq!("v1/bucket/aabbcc/", prefix);

        // gmt time 2022-03-28T00:00:00
        let ts = Timestamp::new(1648425600000);
        let bucket_duration =
            ReadableDuration(Duration::from_millis(namespace::BUCKET_DURATION_MS as u64));
        let key = format_timed_bucket_key(ns, bucket_duration, ts);
        assert_eq!("v1/bucket/aabbcc/1d/2022-03-28T00:00:00", key);

        let key = format_permanent_bucket_key(ns);
        assert_eq!("v1/bucket/aabbcc/permanent", key);
    }

    #[test]
    fn test_format_region_meta() {
        let ns = "abcdef";
        let name = format_region_meta_name(ns, 0);
        assert_eq!("region_meta_abcdef_000000", name);

        let name = format_region_meta_name(ns, 124);
        assert_eq!("region_meta_abcdef_000124", name);

        let name = format_region_meta_name(ns, 999999);
        assert_eq!("region_meta_abcdef_999999", name);

        let name = format_region_meta_name(ns, 1234567);
        assert_eq!("region_meta_abcdef_1234567", name);
    }

    #[test]
    fn test_format_permanent_wal_name() {
        let ns = "mywal";
        let name = format_permanent_wal_name(ns, 0);
        assert_eq!("wal_mywal_permanent_000000", name);

        let name = format_permanent_wal_name(ns, 124);
        assert_eq!("wal_mywal_permanent_000124", name);

        let name = format_permanent_wal_name(ns, 999999);
        assert_eq!("wal_mywal_permanent_999999", name);

        let name = format_permanent_wal_name(ns, 1234567);
        assert_eq!("wal_mywal_permanent_1234567", name);
    }

    #[test]
    fn test_format_timed_wal_name() {
        let ns = "mywal";

        let name = format_timed_wal_name(ns, Timestamp::ZERO, 0);
        assert_eq!("wal_mywal_19700101000000_000000", name);

        // gmt time 2022-03-28T00:00:00
        let ts = Timestamp::new(1648425600000);

        let name = format_timed_wal_name(ns, ts, 124);
        assert_eq!("wal_mywal_20220328000000_000124", name);

        let name = format_timed_wal_name(ns, ts, 999999);
        assert_eq!("wal_mywal_20220328000000_999999", name);

        let name = format_timed_wal_name(ns, ts, 1234567);
        assert_eq!("wal_mywal_20220328000000_1234567", name);
    }

    #[test]
    fn test_format_region_key() {
        let key = format_region_key(0);
        assert_eq!("v1/region/0", key);
        let key = format_region_key(1);
        assert_eq!("v1/region/1", key);

        let key = format_region_key(12345);
        assert_eq!("v1/region/12345", key);

        let key = format_region_key(RegionId::MIN);
        assert_eq!("v1/region/0", key);

        let key = format_region_key(RegionId::MAX);
        assert_eq!("v1/region/18446744073709551615", key);
    }

    #[test]
    fn test_log_encoding() {
        let region_id = 1234;

        let sequences = [1000, 1001, 1002, 1003];
        let mut buf = BytesMut::new();
        let encoding = LogEncoding::newest();
        for seq in sequences {
            let log_key = (region_id, seq);
            encoding.encode_key(&mut buf, &log_key).unwrap();

            assert!(encoding.is_log_key(&buf).unwrap());

            let decoded_key = encoding.decode_key(&buf).unwrap();
            assert_eq!(log_key, decoded_key);
        }

        let decoder = TestPayloadDecoder;
        for val in 0..8 {
            let payload = TestPayload { val };

            encoding.encode_value(&mut buf, &payload).unwrap();

            let mut value = encoding.decode_value(&buf).unwrap();
            let decoded_value = decoder.decode(&mut value).unwrap();

            assert_eq!(payload, decoded_value);
        }
    }
}
