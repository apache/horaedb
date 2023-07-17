// Copyright 2022-2023 CeresDB Project Authors. Licensed under Apache-2.0.

//! Skiplist memtable factory

use std::{
    collections::HashMap,
    sync::{atomic::AtomicU64, Arc, RwLock},
};

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
            schema: opts.schema,
            last_sequence: AtomicU64::new(opts.creation_sequence),
        });

        Ok(memtable)
    }
}
