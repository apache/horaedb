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

//! Skiplist memtable factory

use std::sync::{atomic::AtomicU64, Arc};

use arena::MonoIncArena;
use skiplist::Skiplist;

use crate::memtable::{
    factory::{Factory, Options, Result},
    skiplist::{BytewiseComparator, SkiplistMemTable},
    MemTableRef,
};

/// Factory to create memtable
#[derive(Debug)]
pub struct SkiplistMemTableFactory;

impl Factory for SkiplistMemTableFactory {
    fn create_memtable(&self, opts: Options) -> Result<MemTableRef> {
        let arena = MonoIncArena::with_collector(opts.arena_block_size as usize, opts.collector);
        let skiplist = Skiplist::with_arena(BytewiseComparator, arena);
        let memtable = Arc::new(SkiplistMemTable {
            schema: opts.schema,
            skiplist,
            last_sequence: AtomicU64::new(opts.creation_sequence),
            metrics: Default::default(),
        });

        Ok(memtable)
    }
}
