// Copyright 2023 CeresDB Project Authors. Licensed under Apache-2.0.

//! hotspot LRU
use std::hash::Hash;

use lru::LruCache;

pub struct HotspotLru<K> {
    heats: LruCache<K, u64>,
}

impl<K: Hash + Eq + Clone> HotspotLru<K> {
    /// Creates a new LRU Hotspot that holds at most `cap` items
    pub fn new(cap: usize) -> HotspotLru<K> {
        Self {
            heats: LruCache::new(cap),
        }
    }

    /// Creates a new LRU Hotspot that holds at most `cap` items and
    /// uses the provided cache.
    pub fn with_cache(cache: LruCache<K, u64>) -> HotspotLru<K> {
        HotspotLru { heats: cache }
    }

    /// Incs heat into hotspot cache, If the key already exists it
    /// updates its heat value
    pub fn inc(&mut self, key: &K, heat: u64) {
        match self.heats.get_mut(key) {
            Some(val) => *val += heat,
            None => {
                self.heats.put(key.clone(), heat).unwrap();
            }
        }
    }

    /// Put new value into hotspot cache by the predicate.
    pub fn put_if<F>(&mut self, key: &K, new_val: u64, f: F)
    where
        F: Fn(u64, u64) -> bool,
    {
        match self.heats.get_mut(key) {
            Some(val) => {
                if f(*val, new_val) {
                    *val = new_val;
                }
            }
            None => {
                self.heats.put(key.clone(), new_val).unwrap();
            }
        }
    }

    /// Removes and returns the key and value corresponding to the
    /// least recently used item or `None` if the hotspot is empty.
    pub fn pop_lru(&mut self) -> Option<(K, u64)> {
        self.heats.pop_lru()
    }

    /// Removes and returns all items.
    pub fn pop_all(&mut self) -> Vec<(K, u64)> {
        let mut values = Vec::with_capacity(self.heats.len());

        while let Some(value) = self.heats.pop_lru() {
            values.push(value);
        }

        values
    }

    /// Returns the number of keys that are currently in the the cache.
    pub fn len(&self) -> usize {
        self.heats.len()
    }

    /// Returns a bool indicating whether the cache is empty or not.
    pub fn is_empty(&self) -> bool {
        self.heats.len() == 0
    }

    /// Returns the maximum number of keys the cache can hold.
    pub fn cap(&self) -> usize {
        self.heats.cap()
    }

    /// If the new capacity is smaller than the
    /// size of the current cache any entries past the new capacity are
    /// discarded.
    pub fn resize(&mut self, cap: usize) {
        self.heats.resize(cap);
    }

    /// Clears the contents of the cache.
    pub fn clear(&mut self) {
        self.heats.clear();
    }
}

#[cfg(test)]
mod tests {
    use std::fmt::Debug;

    use lru::{DefaultHasher, LruCache};

    use crate::grpc::hotspot_lru::HotspotLru;

    fn assert_opt_eq<K>(opt: Option<(K, u64)>, key: K, heat: u64)
    where
        K: PartialEq + Debug,
    {
        assert!(opt.is_some());
        let val = opt.unwrap();
        assert_eq!(val.0, key);
        assert_eq!(val.1, heat);
    }

    #[test]
    fn test_with_hasher() {
        // use std::collections::hash_map::RandomState;

        let s = DefaultHasher::default();
        let mut hotspot = HotspotLru::with_cache(LruCache::with_hasher(16, s));

        for i in 0..13370 {
            hotspot.inc(&i, 1);
        }
        assert_eq!(hotspot.len(), 16);
    }

    #[test]
    fn test_inc_and_pop() {
        let mut hotspot = HotspotLru::new(10);
        assert!(hotspot.is_empty());

        hotspot.inc(&"apple", 1);
        hotspot.inc(&"banana", 2);
        hotspot.inc(&"orange", 3);
        hotspot.inc(&"peach", 4);
        hotspot.inc(&"cherry", 5);

        assert_eq!(hotspot.cap(), 10);
        assert_eq!(hotspot.len(), 5);
        assert!(!hotspot.is_empty());
        assert_opt_eq(hotspot.pop_lru(), "apple", 1);
        assert_opt_eq(hotspot.pop_lru(), "banana", 2);
        assert_opt_eq(hotspot.pop_lru(), "orange", 3);
        assert_opt_eq(hotspot.pop_lru(), "peach", 4);
        assert_opt_eq(hotspot.pop_lru(), "cherry", 5);
    }

    #[test]
    fn test_update() {
        let mut hotspot = HotspotLru::new(1);

        hotspot.inc(&"apple", 2);
        hotspot.inc(&"apple", 1);

        assert_eq!(hotspot.len(), 1);
        assert_opt_eq(hotspot.pop_lru(), "apple", 3);
    }

    #[test]
    fn test_removes_oldest() {
        let mut hotspot = HotspotLru::new(2);

        hotspot.inc(&"apple", 1);
        hotspot.inc(&"banana", 1);
        hotspot.inc(&"pear", 2);

        assert_eq!(hotspot.len(), 2);
        assert_opt_eq(hotspot.pop_lru(), "banana", 1);
        assert_opt_eq(hotspot.pop_lru(), "pear", 2);
    }

    #[test]
    fn test_clear() {
        let mut hotspot = HotspotLru::new(2);

        hotspot.inc(&"apple", 1);
        hotspot.inc(&"banana", 1);

        assert_eq!(hotspot.len(), 2);

        hotspot.clear();
        assert_eq!(hotspot.len(), 0);
    }

    #[test]
    fn test_resize_larger() {
        let mut hotspot = HotspotLru::new(2);

        hotspot.inc(&"a", 1);
        hotspot.inc(&"b", 1);
        hotspot.resize(4);
        hotspot.inc(&"c", 1);
        hotspot.inc(&"d", 1);

        assert_eq!(hotspot.len(), 4);

        let values = hotspot.pop_all();
        assert_eq!(values.len(), 4);
    }

    #[test]
    fn test_resize_smaller() {
        let mut hotspot = HotspotLru::new(4);

        hotspot.inc(&"a", 1);
        hotspot.inc(&"b", 1);
        hotspot.inc(&"c", 1);
        hotspot.inc(&"d", 1);

        hotspot.resize(2);

        assert_eq!(hotspot.len(), 2);

        let values = hotspot.pop_all();
        assert_eq!(values.len(), 2);
    }

    #[test]
    fn test_send() {
        use std::thread;

        let mut hotspot = HotspotLru::new(4);
        hotspot.inc(&"a", 2);

        let handle = thread::spawn(move || {
            assert_opt_eq(hotspot.pop_lru(), "a", 2);
        });

        assert!(handle.join().is_ok());
    }
}
