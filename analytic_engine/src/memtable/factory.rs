// Copyright 2022-2023 CeresDB Project Authors. Licensed under Apache-2.0.

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
pub struct Options {
    /// Schema of the skiplist.
    pub schema: Schema,
    /// Block size of arena in bytes.
    pub arena_block_size: u32,
    /// Log sequence at the memtable creation.
    pub creation_sequence: SequenceNumber,
    /// Memory usage colllector
    pub collector: CollectorRef,
}

/// MemTable factory
pub trait Factory: fmt::Debug {
    /// Create a new memtable instance
    fn create_memtable(&self, opts: Options) -> Result<MemTableRef>;
}

/// MemTable Factory reference
pub type FactoryRef = Arc<dyn Factory + Send + Sync>;
