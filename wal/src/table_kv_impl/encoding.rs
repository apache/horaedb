// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

//! Constants and utils for encoding.

use chrono::{TimeZone, Utc};
use common_types::time::Timestamp;
use common_util::config::ReadableDuration;
use table_kv::{KeyBoundary, ScanRequest};

use crate::manager::RegionId;

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

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;
    use crate::table_kv_impl::namespace;

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
}
