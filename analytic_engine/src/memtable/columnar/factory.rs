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
