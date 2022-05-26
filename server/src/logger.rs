// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

use std::str::FromStr;

use log::SetLoggerError;
use logger::{Level, LogDispatcher, RuntimeLevel};

use crate::config::Config;

pub fn init_log(config: &Config) -> Result<RuntimeLevel, SetLoggerError> {
    let level = match Level::from_str(&config.log_level) {
        Ok(v) => v,
        Err(e) => {
            panic!(
                "Parse log level failed, level: {}, err: {:?}",
                &config.log_level, e
            );
        }
    };

    let term_drain = logger::term_drainer();
    let drain = LogDispatcher::new(term_drain);

    // Use async and init stdlog
    logger::init_log(
        drain,
        level,
        config.enable_async_log,
        config.async_log_channel_len,
        true,
    )
}
