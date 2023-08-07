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

use table_kv::{KeyBoundary, ScanRequest};

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

#[cfg(test)]
mod test {

    use crate::obkv::util::{inc_by_one, scan_request_with_prefix};

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
}
