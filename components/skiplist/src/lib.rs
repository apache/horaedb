// Copyright 2022-2023 CeresDB Project Authors. Licensed under Apache-2.0.

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
pub use list::{IterRef, Skiplist};
pub use slice::ArenaSlice;
