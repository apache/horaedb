// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

// custom hash mod

use std::hash::BuildHasher;

/* We compared the speed difference between murmur3 and ahash for a string of
    length 10, and the results show that ahash has a clear advantage.
    Average time to DefaultHash a string of length 10: 33.6364 nanoseconds
    Average time to ahash a string of length 10: 19.0412 nanoseconds
    Average time to murmur3 a string of length 10: 33.0394 nanoseconds
    Warning: Do not use this hash in non-memory scenarios,
    One of the reasons is as follows:
    https://github.com/tkaitchuck/aHash/blob/master/README.md#goals-and-non-goals
*/
pub use ahash::AHasher;
use byteorder::{ByteOrder, LittleEndian};
use murmur3::murmur3_x64_128;
pub fn hash64(mut bytes: &[u8]) -> u64 {
    let mut out = [0; 16];
    murmur3_x64_128(&mut bytes, 0, &mut out);
    // in most cases we run on little endian target
    LittleEndian::read_u64(&out[0..8])
}

pub fn build_fixed_seed_ahasher() -> AHasher {
    ahash::RandomState::with_seeds(0, 0, 0, 0).build_hasher()
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
