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

//! hotspot LRU
use std::{hash::Hash, num::NonZeroUsize};

use clru::CLruCache;

pub struct HotspotLru<K> {
    heats: CLruCache<K, u64>,
}

impl<K: Hash + Eq + Clone> HotspotLru<K> {
    /// Creates a new LRU Hotspot that holds at most `cap` items
    pub fn new(cap: usize) -> Option<HotspotLru<K>> {
        NonZeroUsize::new(cap).map(|cap| Self {
            heats: CLruCache::new(cap),
        })
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

        while let Some(value) = self.heats.pop_back() {
            values.push(value);
        }

        self.heats.clear();
        values
    }
}

#[cfg(test)]
mod tests {
    use crate::hotspot_lru::HotspotLru;

    #[test]
    fn test_inc_and_pop() {
        let mut hotspot = HotspotLru::new(10).unwrap();
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
        let mut hotspot = HotspotLru::new(1).unwrap();

        hotspot.inc(&"apple", 2);
        hotspot.inc(&"apple", 1);

        let result = hotspot.pop_all();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0], ("apple", 3));
    }

    #[test]
    fn test_removes_oldest() {
        let mut hotspot = HotspotLru::new(2).unwrap();

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

        let mut hotspot = HotspotLru::new(4).unwrap();
        hotspot.inc(&"apple", 2);

        let handle = thread::spawn(move || {
            let result = hotspot.pop_all();
            assert_eq!(result.len(), 1);
            assert_eq!(result[0], ("apple", 2));
        });

        assert!(handle.join().is_ok());
    }
}
