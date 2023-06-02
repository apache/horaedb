// Copyright 2022-2023 CeresDB Project Authors. Licensed under Apache-2.0.

//! Memory profiler for running application based on jemalloc features.

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
const PROFILE_MEM_OUTPUT_FILE_OS_PATH: &[u8] = b"/tmp/profile_mem.out\0";
const PROFILE_MEM_OUTPUT_FILE_PATH: &str = "/tmp/profile_mem.out";
const PROFILE_CPU_OUTPUT_FILE_PATH: &str = "/tmp/flamegraph_cpu.svg";

fn set_prof_active(active: bool) -> Result<()> {
    let name = PROF_ACTIVE.name();
    name.write(active).map_err(Error::Jemalloc)
}

fn dump_profile() -> Result<()> {
    let name = PROF_DUMP.name();
    name.write(PROFILE_MEM_OUTPUT_FILE_OS_PATH)
        .map_err(Error::Jemalloc)
}

struct ProfLockGuard<'a>(MutexGuard<'a, ()>);

/// ProfLockGuard hold the profile lock and take responsibilities for
/// (de)activating mem profiling. NOTE: Keeping mem profiling on may cause some
/// extra runtime cost so we choose to activating it  dynamically.
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

    // dump_mem_prof collects mem profiling data in `seconds`.
    // TODO(xikai): limit the profiling duration
    pub fn dump_mem_prof(&self, seconds: u64) -> Result<Vec<u8>> {
        // concurrent profiling is disabled.
        let lock_guard = self.mem_prof_lock.try_lock().map_err(|e| Error::Internal {
            msg: format!("failed to acquire mem_prof_lock, err:{e}"),
        })?;
        info!(
            "Profiler::dump_mem_prof start memory profiling {} seconds",
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
            .open(PROFILE_MEM_OUTPUT_FILE_PATH)
            .map_err(|e| {
                error!("Failed to open prof data file, err:{}", e);
                Error::IO(e)
            })?;

        // dump the profile results to profile output file.
        dump_profile().map_err(|e| {
            error!(
                "Failed to dump prof to {}, err:{}",
                PROFILE_MEM_OUTPUT_FILE_PATH, e
            );
            e
        })?;

        // read the profile results into buffer
        let mut f = File::open(PROFILE_MEM_OUTPUT_FILE_PATH).map_err(|e| {
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
