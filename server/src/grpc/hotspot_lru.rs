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

    /// Incs heat into hotspot cache, If the key already exists it
    /// updates its heat value
    pub fn inc(&mut self, key: &K, heat: u64) {
        match self.heats.get_mut(key) {
            Some(val) => *val += heat,
            None => {
                self.heats.put(key.clone(), heat);
            }
        }
    }

    /// Removes and returns all items.
    pub fn pop_all(&mut self) -> Vec<(K, u64)> {
        let mut values = Vec::with_capacity(self.heats.len());

        while let Some(value) = self.heats.pop_lru() {
            values.push(value);
        }

        values
    }
}

#[cfg(test)]
mod tests {
    use crate::grpc::hotspot_lru::HotspotLru;

    #[test]
    fn test_inc_and_pop() {
        let mut hotspot = HotspotLru::new(10);
        hotspot.inc(&"apple", 1);
        hotspot.inc(&"banana", 2);
        hotspot.inc(&"orange", 3);
        hotspot.inc(&"peach", 4);
        hotspot.inc(&"cherry", 5);

        let result = hotspot.pop_all();
        assert_eq!(result.len(), 5);
        assert_eq!(result[0], ("apple", 1));
        assert_eq!(result[1], ("banana", 2));
        assert_eq!(result[2], ("orange", 3));
        assert_eq!(result[3], ("peach", 4));
        assert_eq!(result[4], ("cherry", 5));
    }

    #[test]
    fn test_update() {
        let mut hotspot = HotspotLru::new(1);

        hotspot.inc(&"apple", 2);
        hotspot.inc(&"apple", 1);

        let result = hotspot.pop_all();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0], ("apple", 3));
    }

    #[test]
    fn test_removes_oldest() {
        let mut hotspot = HotspotLru::new(2);

        hotspot.inc(&"apple", 1);
        hotspot.inc(&"banana", 1);
        hotspot.inc(&"pear", 2);

        let result = hotspot.pop_all();
        assert_eq!(result.len(), 2);
        assert_eq!(result[0], ("banana", 1));
        assert_eq!(result[1], ("pear", 2));
    }

    #[test]
    fn test_send() {
        use std::thread;

        let mut hotspot = HotspotLru::new(4);
        hotspot.inc(&"apple", 2);

        let handle = thread::spawn(move || {
            let result = hotspot.pop_all();
            assert_eq!(result.len(), 1);
            assert_eq!(result[0], ("apple", 2));
        });

        assert!(handle.join().is_ok());
    }
}
