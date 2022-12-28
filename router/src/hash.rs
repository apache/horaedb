// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

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
