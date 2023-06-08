// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

// custom hash mod

use std::{
    collections::hash_map::DefaultHasher,
    hash::{self, BuildHasher, Hasher},
};

/* We compared the speed difference between murmur3 and ahash for a string of
    length 10, and the results show that ahash has a clear advantage.
    Average time to DefaultHash a string of length 10: 33.6364 nanoseconds
    Average time to ahash a string of length 10: 19.0412 nanoseconds
    Average time to murmur3 a string of length 10: 33.0394 nanoseconds
    Warning: Do not use this hash in non-memory scenarios,
    One of the reasons is as follows:
    https://github.com/tkaitchuck/aHash/blob/master/README.md#goals-and-non-goals
*/
use ahash::AHasher;
use byteorder::{ByteOrder, LittleEndian};
use murmur3::murmur3_x64_128;
pub use seahash::SeaHasher;
pub fn hash64(mut bytes: &[u8]) -> u64 {
    let mut out = [0; 16];
    murmur3_x64_128(&mut bytes, 0, &mut out);
    // in most cases we run on little endian target
    LittleEndian::read_u64(&out[0..8])
}

pub fn build_fixed_seed_ahasher() -> AHasher {
    ahash::RandomState::with_seeds(0, 0, 0, 0).build_hasher()
}

#[derive(Clone, Debug, Default)]
pub struct HasherWrapper<H = hash::BuildHasherDefault<DefaultHasher>>
where
    H: BuildHasher,
{
    pub hash_builder: H,
}

impl<H> HasherWrapper<H>
where
    H: BuildHasher,
{
    fn new(hash_builder: H) -> Self {
        Self { hash_builder }
    }

    fn hash<K: std::hash::Hash>(&self, key: K) -> u64 {
        let mut hasher = self.hash_builder.build_hasher();
        key.hash(&mut hasher);
        hasher.finish()
    }
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
