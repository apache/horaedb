use table_kv::{KeyBoundary, ScanRequest};

/// Generate ScanRequest with prefix
pub fn scan_request_with_prefix(prefix: &str) -> ScanRequest {
    let key_buffer = prefix.as_bytes();
    let mut start_key = Vec::with_capacity(key_buffer.len() + 1);
    let mut end_key = Vec::with_capacity(key_buffer.len() + 1);
    start_key.extend(key_buffer);
    start_key.push(0x00);
    end_key.extend(key_buffer);
    end_key.push(0xff);
    let start = KeyBoundary::included(start_key.as_ref());
    let end = KeyBoundary::excluded(end_key.as_ref());
    table_kv::ScanRequest {
        start,
        end,
        reverse: false,
    }
}
