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

//! MemTable factory

use std::{fmt, sync::Arc};

use arena::CollectorRef;
use common_types::{schema::Schema, SequenceNumber};
use macros::define_result;
use snafu::Snafu;

use crate::memtable::MemTableRef;

#[derive(Debug, Snafu)]
pub enum Error {}

define_result!(Error);

/// MemTable options
#[derive(Clone)]
pub struct Options {
    /// Schema of the skiplist.
    pub schema: Schema,
    /// Block size of arena in bytes.
    pub arena_block_size: u32,
    /// Log sequence at the memtable creation.
    pub creation_sequence: SequenceNumber,
    /// Memory usage collector
    pub collector: CollectorRef,
}

/// MemTable factory
pub trait Factory: fmt::Debug {
    /// Create a new memtable instance
    fn create_memtable(&self, opts: Options) -> Result<MemTableRef>;
}

/// MemTable Factory reference
pub type FactoryRef = Arc<dyn Factory + Send + Sync>;
