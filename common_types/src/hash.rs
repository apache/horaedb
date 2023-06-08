// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

/// Which Hash to use:
/// - Memory : aHash
/// - Disk: SeaHash
/// https://github.com/CeresDB/hash-benchmark-rs
use std::hash::BuildHasher;

pub use ahash;
use byteorder::{ByteOrder, LittleEndian};
use murmur3::murmur3_x64_128;
use seahash::{self, SeaHasher};

#[derive(Debug)]
pub struct SeaHasherBuilder;

impl BuildHasher for SeaHasherBuilder {
    type Hasher = SeaHasher;

    fn build_hasher(&self) -> Self::Hasher {
        seahash::SeaHasher::new()
    }
}

pub fn hash64(mut bytes: &[u8]) -> u64 {
    let mut out = [0; 16];
    murmur3_x64_128(&mut bytes, 0, &mut out);
    // in most cases we run on little endian target
    LittleEndian::read_u64(&out[0..8])
}

pub fn build_fixed_seed_ahasher_builder() -> ahash::RandomState {
    ahash::RandomState::with_seeds(0, 0, 0, 0)
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn empty_hash_test() {
        let res1 = hash64(&[]);
        let res2 = hash64(&[]);
        assert_eq!(res1, res2);
    }

    #[test]
    fn hash_test() {
        let test_bytes_1 = b"cse_engine_hash_mod_test_bytes1".to_vec();
        let test_bytes_2 = b"cse_engine_hash_mod_test_bytes2".to_vec();
        {
            // hash64 testing
            let res1 = hash64(&test_bytes_1);
            let res1_1 = hash64(&test_bytes_1);
            assert_eq!(res1, res1_1);

            let res2 = hash64(&test_bytes_2);
            assert_ne!(res1, res2);
        }
    }
}
