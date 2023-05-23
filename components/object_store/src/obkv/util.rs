// Copyright 2023 CeresDB Project Authors. Licensed under Apache-2.0.

use table_kv::{KeyBoundary, ScanRequest};

/// Generate ScanRequest with prefix
pub fn scan_request_with_prefix(prefix: &str) -> ScanRequest {
    let key_buffer = prefix.as_bytes();
    let mut start_key = Vec::with_capacity(key_buffer.len() + 1);
    let mut end_key = Vec::with_capacity(key_buffer.len() + 1);
    start_key.extend(key_buffer);
    end_key.extend(key_buffer);
    // push the 0xffffffff into end_key
    let value = usize::MAX.to_be_bytes();
    end_key.extend(value);
    let start = KeyBoundary::included(start_key.as_ref());
    let end = KeyBoundary::excluded(end_key.as_ref());
    table_kv::ScanRequest {
        start,
        end,
        reverse: false,
    }
}
