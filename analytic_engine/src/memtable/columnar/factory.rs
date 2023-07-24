// Copyright 2023 CeresDB Project Authors. Licensed under Apache-2.0.

//! Columnar memtable factory

use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicU64, AtomicUsize},
        Arc, RwLock,
    },
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
            schema: opts.schema.clone(),
            last_sequence: AtomicU64::new(opts.creation_sequence),
            row_num: AtomicUsize::new(0),
            opts,
            memtable_size: AtomicUsize::new(0),
        });

        Ok(memtable)
    }
}
