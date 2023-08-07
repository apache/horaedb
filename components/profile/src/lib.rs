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

//! Profiler for running application.

use std::{
    fmt::Formatter,
    fs::{File, OpenOptions},
    io,
    io::Read,
    sync::{Mutex, MutexGuard},
    thread, time,
    time::Duration,
};

use jemalloc_ctl::{Access, AsName};
use log::{error, info};

#[derive(Debug)]
pub enum Error {
    Internal { msg: String },
    IO(io::Error),
    Jemalloc(jemalloc_ctl::Error),
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "Profile Error: {self:?}")
    }
}

impl std::error::Error for Error {}

pub type Result<T> = std::result::Result<T, Error>;

#[global_allocator]
static ALLOC: jemallocator::Jemalloc = jemallocator::Jemalloc;

const PROF_ACTIVE: &[u8] = b"prof.active\0";
const PROF_DUMP: &[u8] = b"prof.dump\0";
const PROFILE_HEAP_OUTPUT_FILE_OS_PATH: &[u8] = b"/tmp/profile_heap.out\0";
const PROFILE_HEAP_OUTPUT_FILE_PATH: &str = "/tmp/profile_heap.out";
const PROFILE_CPU_OUTPUT_FILE_PATH: &str = "/tmp/flamegraph_cpu.svg";

fn set_prof_active(active: bool) -> Result<()> {
    let name = PROF_ACTIVE.name();
    name.write(active).map_err(Error::Jemalloc)
}

fn dump_profile() -> Result<()> {
    let name = PROF_DUMP.name();
    name.write(PROFILE_HEAP_OUTPUT_FILE_OS_PATH)
        .map_err(Error::Jemalloc)
}

struct ProfLockGuard<'a>(MutexGuard<'a, ()>);

/// ProfLockGuard hold the profile lock and take responsibilities for
/// (de)activating heap profiling. NOTE: Keeping heap profiling on may cause
/// some extra runtime cost so we choose to activating it  dynamically.
impl<'a> ProfLockGuard<'a> {
    pub fn new(guard: MutexGuard<'a, ()>) -> Result<Self> {
        set_prof_active(true)?;
        Ok(Self(guard))
    }
}

impl<'a> Drop for ProfLockGuard<'a> {
    fn drop(&mut self) {
        if let Err(e) = set_prof_active(false) {
            error!("Fail to deactivate profiling, err:{}", e);
        }
    }
}

pub struct Profiler {
    heap_prof_lock: Mutex<()>,
}

impl Default for Profiler {
    fn default() -> Self {
        Self::new()
    }
}

impl Profiler {
    pub fn new() -> Self {
        Self {
            heap_prof_lock: Mutex::new(()),
        }
    }

    // dump_heap_prof collects heap profiling data in `seconds`.
    // TODO(xikai): limit the profiling duration
    pub fn dump_heap_prof(&self, seconds: u64) -> Result<Vec<u8>> {
        // concurrent profiling is disabled.
        let lock_guard = self
            .heap_prof_lock
            .try_lock()
            .map_err(|e| Error::Internal {
                msg: format!("failed to acquire heap_prof_lock, err:{e}"),
            })?;
        info!(
            "Profiler::dump_heap_prof start heap profiling {} seconds",
            seconds
        );

        let _guard = ProfLockGuard::new(lock_guard)?;

        // wait for seconds for collect the profiling data
        thread::sleep(time::Duration::from_secs(seconds));

        // clearing the profile output file before dumping profile results.
        let _ = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(PROFILE_HEAP_OUTPUT_FILE_PATH)
            .map_err(|e| {
                error!("Failed to open prof data file, err:{}", e);
                Error::IO(e)
            })?;

        // dump the profile results to profile output file.
        dump_profile().map_err(|e| {
            error!(
                "Failed to dump prof to {}, err:{}",
                PROFILE_HEAP_OUTPUT_FILE_PATH, e
            );
            e
        })?;

        // read the profile results into buffer
        let mut f = File::open(PROFILE_HEAP_OUTPUT_FILE_PATH).map_err(|e| {
            error!("Failed to open prof data file, err:{}", e);
            Error::IO(e)
        })?;

        let mut buffer = Vec::new();
        f.read_to_end(&mut buffer).map_err(|e| {
            error!("Failed to read prof data file, err:{}", e);
            Error::IO(e)
        })?;

        Ok(buffer)
    }

    pub fn dump_cpu_prof(&self, seconds: u64) -> Result<()> {
        let guard = pprof::ProfilerGuardBuilder::default()
            .frequency(100)
            .blocklist(&["libc", "libgcc", "pthread", "vdso"])
            .build()
            .map_err(|e| Error::Internal {
                msg: format!("Profiler guard, err:{e}"),
            })?;

        thread::sleep(Duration::from_secs(seconds));

        let report = guard.report().build().map_err(|e| Error::Internal {
            msg: format!("Report build, err:{e}"),
        })?;
        let file = File::create(PROFILE_CPU_OUTPUT_FILE_PATH).map_err(|e| {
            error!("Failed to create cpu profile svg file, err:{}", e);
            Error::IO(e)
        })?;
        report.flamegraph(file).map_err(|e| Error::Internal {
            msg: format!("Flamegraph output, err:{e}"),
        })?;
        Ok(())
    }
}
