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

use std::hash::{Hash, Hasher};

use twox_hash::XxHash64;

/// Hash seed to build hasher. Modify the seed will result in different route
/// result!
const HASH_SEED: u64 = 0;

pub(crate) fn hash_table(table: &str) -> u64 {
    let mut hasher = XxHash64::with_seed(HASH_SEED);
    table.hash(&mut hasher);
    hasher.finish()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_hash_table_to_determined_id() {
        let tables = ["aaa", "bbb", "", "*x21"];
        for table in tables {
            let id = hash_table(table);
            assert_eq!(id, hash_table(table));
        }
    }
}
