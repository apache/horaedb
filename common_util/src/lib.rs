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
pub mod metric;
pub mod panic;
pub mod runtime;
pub mod time;
pub mod toml;

#[cfg(any(test, feature = "test"))]
pub mod tests {
    use std::sync::Once;

    use env_logger::Env;

    static INIT_LOG: Once = Once::new();

    pub fn init_log_for_test() {
        INIT_LOG.call_once(|| {
            env_logger::init();
        });
    }

    pub fn init_log_for_test_with_level(level: &str) {
        INIT_LOG.call_once(|| {
            let env = Env::default().default_filter_or(level);
            env_logger::init_from_env(env);
        });
    }
}
