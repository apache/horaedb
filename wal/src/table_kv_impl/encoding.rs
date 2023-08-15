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

//! Constants and utils for encoding.

use chrono::{TimeZone, Utc};
use common_types::{table::TableId, time::Timestamp};
use macros::define_result;
use snafu::Snafu;
use table_kv::{KeyBoundary, ScanRequest};
use time_ext::ReadableDuration;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Timestamp is invalid, timestamp:{}", timestamp))]
    InValidTimestamp { timestamp: i64 },
}

define_result!(Error);

/// Key prefix for namespace in meta table.
const META_NAMESPACE_PREFIX: &str = "v1/namespace";
/// Key prefix for bucket in meta table.
const META_BUCKET_PREFIX: &str = "v1/bucket";
/// Format of bucket timestamp.
const BUCKET_TIMESTAMP_FORMAT: &str = "%Y-%m-%dT%H:%M:%S";
/// Key prefix of table unit meta.
const TABLE_UNIT_META_PREFIX: &str = "v1/table";
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
    format!("{META_NAMESPACE_PREFIX}/{namespace}")
}

#[inline]
pub fn bucket_key_prefix(namespace: &str) -> String {
    format!("{META_BUCKET_PREFIX}/{namespace}/")
}

pub fn format_timed_bucket_key(
    namespace: &str,
    bucket_duration: ReadableDuration,
    gmt_start_ms: Timestamp,
) -> Result<String> {
    let duration = bucket_duration.to_string();
    let dt = match Utc.timestamp_millis_opt(gmt_start_ms.as_i64()).single() {
        None => InValidTimestamp {
            timestamp: gmt_start_ms.as_i64(),
        }
        .fail()?,
        Some(v) => v,
    };
    Ok(format!(
        "{}/{}/{}/{}",
        META_BUCKET_PREFIX,
        namespace,
        duration,
        dt.format(BUCKET_TIMESTAMP_FORMAT)
    ))
}

pub fn format_permanent_bucket_key(namespace: &str) -> String {
    format!("{META_BUCKET_PREFIX}/{namespace}/permanent")
}

#[inline]
pub fn format_table_unit_meta_name(namespace: &str, shard_id: usize) -> String {
    format!("table_unit_meta_{namespace}_{shard_id:0>6}")
}

#[inline]
pub fn format_timed_wal_name(
    namespace: &str,
    gmt_start_ms: Timestamp,
    shard_id: usize,
) -> Result<String> {
    let dt = match Utc.timestamp_millis_opt(gmt_start_ms.as_i64()).single() {
        None => InValidTimestamp {
            timestamp: gmt_start_ms.as_i64(),
        }
        .fail()?,
        Some(v) => v,
    };
    Ok(format!(
        "wal_{}_{}_{:0>6}",
        namespace,
        dt.format(WAL_SHARD_TIMESTAMP_FORMAT),
        shard_id
    ))
}

#[inline]
pub fn format_permanent_wal_name(namespace: &str, shard_id: usize) -> String {
    format!("wal_{namespace}_permanent_{shard_id:0>6}")
}

#[inline]
pub fn format_table_unit_key(table_id: TableId) -> String {
    format!("{TABLE_UNIT_META_PREFIX}/{table_id}")
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
        let key = format_timed_bucket_key(ns, bucket_duration, ts).unwrap();
        assert_eq!("v1/bucket/aabbcc/1d/2022-03-28T00:00:00", key);

        let key = format_permanent_bucket_key(ns);
        assert_eq!("v1/bucket/aabbcc/permanent", key);
    }

    #[test]
    fn test_format_table_unit_meta() {
        let ns = "abcdef";
        let name = format_table_unit_meta_name(ns, 0);
        assert_eq!("table_unit_meta_abcdef_000000", name);

        let name = format_table_unit_meta_name(ns, 124);
        assert_eq!("table_unit_meta_abcdef_000124", name);

        let name = format_table_unit_meta_name(ns, 999999);
        assert_eq!("table_unit_meta_abcdef_999999", name);

        let name = format_table_unit_meta_name(ns, 1234567);
        assert_eq!("table_unit_meta_abcdef_1234567", name);
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

        let name = format_timed_wal_name(ns, Timestamp::ZERO, 0).unwrap();
        assert_eq!("wal_mywal_19700101000000_000000", name);

        // gmt time 2022-03-28T00:00:00
        let ts = Timestamp::new(1648425600000);

        let name = format_timed_wal_name(ns, ts, 124).unwrap();
        assert_eq!("wal_mywal_20220328000000_000124", name);

        let name = format_timed_wal_name(ns, ts, 999999).unwrap();
        assert_eq!("wal_mywal_20220328000000_999999", name);

        let name = format_timed_wal_name(ns, ts, 1234567).unwrap();
        assert_eq!("wal_mywal_20220328000000_1234567", name);
    }

    #[test]
    fn test_format_table_unit_key() {
        let key = format_table_unit_key(0);
        assert_eq!("v1/table/0", key);
        let key = format_table_unit_key(1);
        assert_eq!("v1/table/1", key);

        let key = format_table_unit_key(12345);
        assert_eq!("v1/table/12345", key);

        let key = format_table_unit_key(u64::MIN);
        assert_eq!("v1/table/0", key);

        let key = format_table_unit_key(u64::MAX);
        assert_eq!("v1/table/18446744073709551615", key);
    }
}
