// Copyright 2022-2023 CeresDB Project Authors. Licensed under Apache-2.0.

//! Skiplist memtable factory

use std::sync::{
    atomic::{AtomicU64, AtomicUsize},
    Arc,
};

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
            wrote_data_size: AtomicUsize::new(0),
            wrote_data_encode_size: AtomicUsize::new(0),
        });

        Ok(memtable)
    }
}
