// Copyright 2023 CeresDB Project Authors. Licensed under Apache-2.0.

use table_kv::{KeyBoundary, ScanRequest};

use super::meta::ObkvObjectMeta;

/// Generate ScanRequest with prefix
pub fn scan_request_with_prefix(prefix_bytes: &[u8]) -> ScanRequest {
    let mut start_key = Vec::with_capacity(prefix_bytes.len());
    start_key.extend(prefix_bytes);
    let start = KeyBoundary::included(start_key.as_ref());

    let mut end_key = Vec::with_capacity(prefix_bytes.len());
    end_key.extend(prefix_bytes);
    let carry = inc_by_one(&mut end_key);
    // Check add one operation overflow.
    let end = if carry == 1 {
        KeyBoundary::MaxIncluded
    } else {
        KeyBoundary::excluded(end_key.as_ref())
    };
    table_kv::ScanRequest {
        start,
        end,
        reverse: false,
    }
}

/// Increment one to the byte array, and return the carry.
fn inc_by_one(nums: &mut [u8]) -> u8 {
    let mut carry = 1;
    for i in (0..nums.len()).rev() {
        let sum = nums[i].wrapping_add(carry);
        nums[i] = sum;
        if sum == 0 {
            carry = 1;
        } else {
            carry = 0;
            break;
        }
    }
    carry
}

/// Estimate the json string size of ObkvObjectMeta
pub fn estimate_size(value: &ObkvObjectMeta) -> usize {
    // {}
    let mut size = 2;
    // size of key name && , && "" && :
    size += (8 + 13 + 4 + 9 + 9 + 5 + 7) + 4 * 7;
    size += value.location.len() + 2;
    // last_modified
    size += 8;
    // size
    size += 8;
    // unique_id
    if let Some(id) = &value.unique_id {
        size += id.len() + 2;
    } else {
        size += 4;
    }
    // part_size
    size += 8;
    // parts
    for part in &value.parts {
        // part.len && "" &&: &&,
        size += part.len() + 4;
    }
    //{}
    size += 2;
    // version
    size += value.version.len();
    size
}

#[cfg(test)]
mod test {

    use crate::obkv::{
        meta::ObkvObjectMeta,
        util::{estimate_size, inc_by_one, scan_request_with_prefix},
    };

    #[test]
    fn test_add_one() {
        let mut case0 = vec![0xff_u8, 0xff, 0xff];
        let case0_expect = vec![0x00, 0x00, 0x00];
        assert_eq!(1, inc_by_one(&mut case0));
        assert_eq!(case0, case0_expect);

        let mut case1 = vec![0x00_u8, 0xff, 0xff];
        let case1_expect = vec![0x01, 0x00, 0x00];
        assert_eq!(0, inc_by_one(&mut case1));
        assert_eq!(case1, case1_expect);

        let mut case2 = vec![0x00_u8, 0x00, 0x00];
        let case2_expect = vec![0x00, 0x00, 0x01];
        assert_eq!(0, inc_by_one(&mut case2));
        assert_eq!(case2, case2_expect);
    }

    #[test]
    fn test_scan_request_with_prefix() {
        let case0 = vec![0xff_u8, 0xff, 0xff];
        let case0_expect = table_kv::ScanRequest {
            start: table_kv::KeyBoundary::included(&case0),
            end: table_kv::KeyBoundary::MaxIncluded,
            reverse: false,
        };
        let case0_actual = scan_request_with_prefix(&case0);
        assert_eq!(case0_expect, case0_actual);

        let case1 = "abc".as_bytes();
        let case1_expect_bytes = "abd".as_bytes();
        let case1_expect = table_kv::ScanRequest {
            start: table_kv::KeyBoundary::included(case1),
            end: table_kv::KeyBoundary::excluded(case1_expect_bytes),
            reverse: false,
        };
        let case1_actual = scan_request_with_prefix(case1);
        assert_eq!(case1_expect, case1_actual);

        let case2 = vec![0x00_u8, 0x00, 0x00];
        let case2_expect_bytes = vec![0x00_u8, 0x00, 0x01];
        let case2_expect = table_kv::ScanRequest {
            start: table_kv::KeyBoundary::included(&case2),
            end: table_kv::KeyBoundary::excluded(&case2_expect_bytes),
            reverse: false,
        };
        let case2_actual = scan_request_with_prefix(&case2);
        assert_eq!(case2_expect, case2_actual);

        let case3 = vec![0x00_u8, 0x00, 0xff];
        let case3_expect_bytes = vec![0x00_u8, 0x01, 0x00];
        let case3_expect = table_kv::ScanRequest {
            start: table_kv::KeyBoundary::included(&case3),
            end: table_kv::KeyBoundary::excluded(&case3_expect_bytes),
            reverse: false,
        };
        let case3_actual = scan_request_with_prefix(&case3);
        assert_eq!(case3_expect, case3_actual);
    }

    #[test]
    fn test_estimate_size() {
        let meta = ObkvObjectMeta {
            location: String::from("/test/xxxxxxxxxxxxxxxxxxxxxxxxxxxxxfdsfjlajflk"),
            last_modified: 123456789,
            size: 10000000,
            unique_id: Some(String::from("1245689u438uferjalfjkda")),
            part_size: 1024,
            parts: vec![
                String::from("/test/xx/0"),
                String::from("/test/xx/1"),
                String::from("/test/xx/4"),
                String::from("/test/xx/5"),
                String::from("/test/xx/0"),
                String::from("/test/xx/1"),
                String::from("/test/xx/4"),
                String::from("/test/xx/5"),
            ],
            version: String::from("123456fsdalfkassa;l;kjfaklasadffsd"),
        };

        let expect = estimate_size(&meta);
        let json = &serde_json::to_string(&meta).unwrap();
        let real = json.len();
        println!("expect:{expect},real:{real}");
        assert!(expect.abs_diff(real) as f32 / (real as f32) < 0.1);
    }
}
