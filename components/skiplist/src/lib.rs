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

//! Forked from <https://github.com/tikv/agatedb/blob/8510bff2bfde5b766c3f83cf81c00141967d48a4/skiplist>
//!
//! Differences:
//! 1. Inline key and value in Node, so all memory of skiplist is allocated from
//! arena. Drawback: we have to copy the content of key/value
//! 2. Tower stores pointer to Node instead of offset, so we can use other arena
//! implementation
//! 3. Use [ArenaSlice] to replace Bytes
//! 4. impl Send/Sync for the iterator

mod key;
mod list;
mod slice;

const MAX_HEIGHT: usize = 20;

pub use key::{BytewiseComparator, FixedLengthSuffixComparator, KeyComparator};
pub use list::{IterRef, Skiplist, MAX_KEY_SIZE};
pub use slice::ArenaSlice;
