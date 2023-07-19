// Copyright 2022-2023 CeresDB Project Authors. Licensed under Apache-2.0.

/// Which Hash to use:
/// - Memory: aHash
/// - Disk: SeaHash
/// https://github.com/CeresDB/hash-benchmark-rs
use std::hash::BuildHasher;

pub use ahash;
use byteorder::{ByteOrder, LittleEndian};
use murmur3::murmur3_x64_128;
use seahash::SeaHasher;

#[derive(Debug)]
pub struct SeaHasherBuilder;

impl BuildHasher for SeaHasherBuilder {
    type Hasher = SeaHasher;

    fn build_hasher(&self) -> Self::Hasher {
        SeaHasher::new()
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
    use std::{collections::hash_map::DefaultHasher, hash::Hasher};

    use super::*;

    #[test]
    fn test_murmur_hash() {
        assert_eq!(hash64(&[]), 0);

        for (key, code) in [
            (b"cse_engine_hash_mod_test_bytes1", 6401327391689448380),
            (b"cse_engine_hash_mod_test_bytes2", 10824100215277000151),
        ] {
            assert_eq!(code, hash64(key));
        }
    }

    #[test]
    fn test_sea_hash() {
        let mut hasher = SeaHasher::new();
        hasher.write(&[]);
        assert_eq!(14492805990617963705, hasher.finish());

        for (key, code) in [
            (b"cse_engine_hash_mod_test_bytes1", 16301057587465450460),
            (b"cse_engine_hash_mod_test_bytes2", 10270658030298139083),
        ] {
            let mut hasher = SeaHasher::new();
            hasher.write(key);
            assert_eq!(code, hasher.finish());
        }
    }

    #[test]
    fn test_default_hash() {
        let mut hasher = DefaultHasher::new();
        hasher.write(&[]);
        assert_eq!(15130871412783076140, hasher.finish());

        for (key, code) in [
            (b"cse_engine_hash_mod_test_bytes1", 8669533354716427219),
            (b"cse_engine_hash_mod_test_bytes2", 6496951441253214618),
        ] {
            let mut hasher = DefaultHasher::new();
            hasher.write(key);
            assert_eq!(code, hasher.finish());
        }
    }
}
