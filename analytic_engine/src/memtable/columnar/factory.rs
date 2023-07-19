// Copyright 2022-2023 CeresDB Project Authors. Licensed under Apache-2.0.

//! Skiplist memtable factory

use std::{
    cmp::Ordering,
    collections::HashMap,
    sync::{
        atomic::{AtomicU64, AtomicUsize},
        Arc, RwLock,
    },
};

use arena::MonoIncArena;
use skiplist::{KeyComparator, Skiplist};

use crate::memtable::{
    columnar::ColumnarMemTable,
    factory::{Factory, Options, Result},
    MemTableRef,
};

/// Factory to create memtable
#[derive(Debug)]
pub struct ColumnarMemTableFactory;

impl Factory for ColumnarMemTableFactory {
    fn create_memtable(&self, opts: Options) -> Result<MemTableRef> {
        let memtable = Arc::new(ColumnarMemTable {
            memtable: Arc::new(RwLock::new(HashMap::with_capacity(
                opts.schema.num_columns(),
            ))),
            schema: opts.schema.clone(),
            last_sequence: AtomicU64::new(opts.creation_sequence),
            row_num: AtomicUsize::new(0),
            opts,
        });

        Ok(memtable)
    }
}

#[derive(Debug, Clone)]
pub struct BytewiseComparator;

impl KeyComparator for BytewiseComparator {
    #[inline]
    fn compare_key(&self, lhs: &[u8], rhs: &[u8]) -> Ordering {
        lhs.cmp(rhs)
    }

    #[inline]
    fn same_key(&self, lhs: &[u8], rhs: &[u8]) -> bool {
        lhs == rhs
    }
}
