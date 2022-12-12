// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

//! Memory profiler for running application based on jemalloc features.

use std::{
    fmt::Formatter,
    fs::File,
    io,
    io::Read,
    sync::{Mutex, MutexGuard},
    thread, time,
};

use log::{error, info};

#[derive(Debug)]
pub enum Error {
    Internal { msg: String },
    IO(io::Error),
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "Profile Error: {:?}", self)
    }
}

impl std::error::Error for Error {}

pub type Result<T> = std::result::Result<T, Error>;

pub struct Profiler {
    mem_prof_lock: Mutex<()>,
}

impl Default for Profiler {
    fn default() -> Self {
        Self::new()
    }
}

impl Profiler {
    pub fn new() -> Self {
        Self {
            mem_prof_lock: Mutex::new(()),
        }
    }

    pub fn dump_mem_prof(&self, seconds: u64, intervals: i32) -> Result<String> {
        // concurrent profiling is disabled.
        let lock_guard = self.mem_prof_lock.try_lock().map_err(|e| Error::Internal {
            msg: format!("failed to acquire mem_prof_lock, err:{}", e),
        })?;

        let heap_profiler_guard = heappy::HeapProfilerGuard::new(1).unwrap();

        info!(
            "Profiler::dump_mem_prof start memory profiling {} seconds",
            seconds
        );
        // wait for seconds for collect the profiling data
        thread::sleep(time::Duration::from_secs(seconds));

        let report = heap_profiler_guard.report();

        let filename = "/tmp/memflame.svg";
        let mut file = std::fs::File::create(filename).unwrap();
        report.flamegraph(&mut file);

        Ok(filename.to_string())
    }
}
