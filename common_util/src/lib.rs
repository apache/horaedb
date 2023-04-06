// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

//! Common utils shared by the whole project

// We need to define mod with macro_use before other mod so that other
// mods in this crate can use the macros
#[macro_use]
pub mod macros;

// TODO(yingwen): Move some mod into components as a crate
pub mod alloc_tracker;
pub mod codec;
pub mod config;
pub mod error;
pub mod metric;
pub mod panic;
pub mod partitioned_lock;
pub mod record_batch;
pub mod runtime;
pub mod time;
pub mod timed_task;
pub mod toml;

#[cfg(any(test, feature = "test"))]
pub mod tests {
    use std::sync::Once;

    static INIT_LOG: Once = Once::new();

    pub fn init_log_for_test() {
        INIT_LOG.call_once(|| {
            env_logger::init();
        });
    }
}
