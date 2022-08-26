// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

use std::hash::{Hash, Hasher};

use twox_hash::XxHash64;

/// Hash seed to build hasher. Modify the seed will result in different route
/// result!
const HASH_SEED: u64 = 0;

pub(crate) fn hash_metric(metric: &str) -> u64 {
    let mut hasher = XxHash64::with_seed(HASH_SEED);
    metric.hash(&mut hasher);
    hasher.finish()
}
