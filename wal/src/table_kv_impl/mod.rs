// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

//! Wal implementation based on TableKv.

use std::sync::Arc;

use common_util::runtime::Runtime;

pub mod encoding;
pub mod model;
mod namespace;

mod table_unit;

pub mod wal;

mod consts {
    /// Table name of the meta table.
    pub const META_TABLE_NAME: &str = "meta";
    /// Milliseconds of one day.
    pub(crate) const DAY_MS: u64 = 1000 * 3600 * 24;
}

/// Runtimes of wal.
#[derive(Clone)]
pub struct WalRuntimes {
    pub read_runtime: Arc<Runtime>,
    pub write_runtime: Arc<Runtime>,
    pub bg_runtime: Arc<Runtime>,
}
