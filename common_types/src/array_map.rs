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

use std::borrow::Borrow;

/// An array based map, optimized for `get` whose size is small.
pub struct ArrayMap<K, V> {
    array: Vec<(K, V)>,
}

impl<K: PartialEq, V> ArrayMap<K, V> {
    pub fn new() -> Self {
        Self { array: Vec::new() }
    }

    pub fn with_capacity(cap: usize) -> Self {
        Self {
            array: Vec::with_capacity(cap),
        }
    }

    pub fn insert(&mut self, k: K, v: V) {
        for kv in self.array.iter_mut() {
            if kv.0 == k {
                kv.1 = v;
                return;
            }
        }

        self.array.push((k, v));
    }

    pub fn get<Q: ?Sized>(&self, input_key: &Q) -> Option<&V>
    where
        K: Borrow<Q>,
        Q: PartialEq,
    {
        for (k, v) in &self.array {
            if k.borrow() == input_key {
                return Some(v);
            }
        }
        None
    }

    pub fn len(&self) -> usize {
        self.array.len()
    }
}

impl<K: PartialEq + Ord, V> FromIterator<(K, V)> for ArrayMap<K, V> {
    fn from_iter<T: IntoIterator<Item = (K, V)>>(iter: T) -> Self {
        let mut array = iter.into_iter().collect::<Vec<_>>();
        array.sort_by(|a, b| a.0.cmp(&b.0));
        array.reverse();
        array.dedup_by(|a, b| a.0.eq(&b.0));
        array.shrink_to_fit();

        Self { array }
    }
}

impl<K: PartialEq, V: PartialEq> PartialEq for ArrayMap<K, V> {
    fn eq(&self, other: &Self) -> bool {
        self.array.eq(&other.array)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn array_map_insert_get() {
        let mut map = ArrayMap::new();
        map.insert("a", 1);
        assert_eq!(1, *map.get("a").unwrap());
        map.insert("a", 2);
        assert_eq!(2, *map.get("a").unwrap());
        assert_eq!(vec![("a", 2)], map.array);

        map.insert("b", 3);
        map.insert("c", 4);
        assert_eq!(3, *map.get("b").unwrap());
        assert_eq!(4, *map.get("c").unwrap());
        assert_eq!(vec![("a", 2), ("b", 3), ("c", 4)], map.array);
    }

    #[test]
    fn array_map_from_iterator() {
        let array = [
            ("b", 2),
            ("a", 1),
            ("c", 3),
            ("a", 11),
            ("c", 33),
            ("a", 111),
        ];
        let map: ArrayMap<_, _> = array.into_iter().collect();
        assert_eq!(111, *map.get("a").unwrap());
        assert_eq!(2, *map.get("b").unwrap());
        assert_eq!(33, *map.get("c").unwrap());

        assert_eq!(vec![("c", 33), ("b", 2), ("a", 111)], map.array);
    }
}
