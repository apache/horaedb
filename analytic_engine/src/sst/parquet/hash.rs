// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use common_types::datum::Datum;


// string_hash is faster than bytes_hash.
// string_hash(s) === bytes_hash(s.as_bytes().to_vec()), see Vec<u8>::hash()
pub fn string_hash(s : &str) -> u64 {
    let mut hasher = DefaultHasher::new();
    hasher.write_usize(s.len());
    Hash::hash_slice(s.as_bytes(), &mut hasher);
    hasher.finish()
}

pub fn bytes_hash(bytes: &Vec<u8>) -> u64 {
    let mut hasher = DefaultHasher::new();
    bytes.hash(&mut hasher);
    hasher.finish()
}

// TODO: replace null_hash with a constant ?
pub fn null_hash() -> u64 {
    let v = Datum::Null.to_bytes();
    bytes_hash(&v)
}