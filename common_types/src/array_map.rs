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

pub struct ArrayMap<K, V> {
    array: Vec<(K, V)>,
}

impl<K: Eq, V> ArrayMap<K, V> {
    pub fn new() -> Self {
        Self { array: Vec::new() }
    }

    pub fn with_capacity(cap: usize) -> Self {
        Self {
            array: Vec::with_capacity(cap),
        }
    }

    pub fn push(&mut self, k: K, v: V) {
        self.array.push((k, v));
    }

    pub fn get<Q: ?Sized>(&self, input_key: &Q) -> Option<&V>
    where
        K: Borrow<Q>,
        Q: Eq,
    {
        for (k, v) in &self.array {
            if k.borrow() == input_key {
                return Some(v);
            }
        }
        None
    }
}

impl<K, V> FromIterator<(K, V)> for ArrayMap<K, V> {
    fn from_iter<T: IntoIterator<Item = (K, V)>>(iter: T) -> Self {
        Self {
            array: iter.into_iter().collect(),
        }
    }
}

impl<K: PartialEq, V: PartialEq> PartialEq for ArrayMap<K, V> {
    fn eq(&self, other: &Self) -> bool {
        self.array.eq(&other.array)
    }
}
