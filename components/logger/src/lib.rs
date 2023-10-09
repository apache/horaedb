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

use std::{
    fmt,
    fs::{File, OpenOptions},
    io,
    str::FromStr,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Mutex,
    },
    time::{Duration, Instant},
};

pub use log::{
    debug as log_debug, error as log_error, info as log_info, max_level, trace as log_trace,
    warn as log_warn, SetLoggerError,
};
use serde::{Deserialize, Serialize};
pub use slog::Level;
use slog::{slog_o, Drain, Key, OwnedKVList, Record, KV};
use slog_async::{Async, OverflowStrategy};
use slog_term::{Decorator, PlainDecorator, RecordDecorator, TermDecorator};

const ASYNC_CHAN_SIZE: usize = 102400;
const TIMESTAMP_FORMAT: &str = "%Y-%m-%d %H:%M:%S%.3f";
pub const SLOW_QUERY_TAG: &str = "slow";
pub const FAILED_QUERY_TAG: &str = "failed";
pub const DEFAULT_TAG: &str = "";

// Thanks to tikv
// https://github.com/tikv/tikv/blob/eaeb39a2c85684de08c48cf4b9426b3faf4defe6/components/tikv_util/src/logger/mod.rs

pub fn convert_slog_level_to_log_level(lv: Level) -> log::Level {
    match lv {
        Level::Critical | Level::Error => log::Level::Error,
        Level::Warning => log::Level::Warn,
        Level::Debug => log::Level::Debug,
        Level::Trace => log::Level::Trace,
        Level::Info => log::Level::Info,
    }
}

pub fn convert_log_level_to_slog_level(lv: log::Level) -> Level {
    match lv {
        log::Level::Error => Level::Error,
        log::Level::Warn => Level::Warning,
        log::Level::Debug => Level::Debug,
        log::Level::Trace => Level::Trace,
        log::Level::Info => Level::Info,
    }
}

// The `to_string()` function of `slog::Level` produces values like `erro` and
// `trce` instead of the full words. This produces the full word.
fn get_string_by_level(lv: Level) -> &'static str {
    match lv {
        Level::Critical => "critical",
        Level::Error => "error",
        Level::Warning => "warn",
        Level::Debug => "debug",
        Level::Trace => "trace",
        Level::Info => "info",
    }
}

pub fn term_drainer() -> CeresFormat<TermDecorator> {
    let decorator = TermDecorator::new().stdout().build();
    CeresFormat::new(decorator)
}

pub fn file_drainer(path: &Option<String>) -> Option<CeresFormat<PlainDecorator<File>>> {
    match path {
        Some(path) => {
            let file = OpenOptions::new()
                .create(true)
                .append(true)
                .open(path)
                .unwrap();
            let decorator = PlainDecorator::new(file);
            Some(CeresFormat::new(decorator))
        }
        None => None,
    }
}

/// Dispatcher for logs
pub struct LogDispatcher<N, S, F> {
    normal: N,
    slow: Option<S>,
    failed: Option<F>,
}

impl<N: Drain, S: Drain, F: Drain> LogDispatcher<N, S, F> {
    pub fn new(normal: N, slow: Option<S>, failed: Option<F>) -> Self {
        Self {
            normal,
            slow,
            failed,
        }
    }
}

impl<N, S, F> Drain for LogDispatcher<N, S, F>
where
    N: Drain<Ok = (), Err = io::Error>,
    S: Drain<Ok = (), Err = io::Error>,
    F: Drain<Ok = (), Err = io::Error>,
{
    type Err = io::Error;
    type Ok = ();

    fn log(&self, record: &Record, values: &OwnedKVList) -> Result<Self::Ok, Self::Err> {
        let tag = record.tag();
        if tag.is_empty() {
            self.normal.log(record, values)
        } else if self.slow.is_some() && tag == SLOW_QUERY_TAG {
            self.slow.as_ref().unwrap().log(record, values)
        } else if self.failed.is_some() && tag == FAILED_QUERY_TAG {
            self.failed.as_ref().unwrap().log(record, values)
        } else {
            Ok(())
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(default)]
/// The config for logger.
pub struct Config {
    pub level: String,
    pub enable_async: bool,
    pub async_channel_len: i32,
    pub slow_query_path: Option<String>,
    pub failed_query_path: Option<String>,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            level: "info".to_string(),
            enable_async: true,
            async_channel_len: 102400,
            slow_query_path: None,
            failed_query_path: None,
        }
    }
}

/// Initialize the logger, configured by the [Config].
pub fn init_log(config: &Config) -> Result<RuntimeLevel, SetLoggerError> {
    let level = match Level::from_str(&config.level) {
        Ok(v) => v,
        Err(e) => {
            panic!(
                "Parse log level failed, level: {}, err: {:?}",
                &config.level, e
            );
        }
    };

    let normal_drain = term_drainer();
    let slow_drain = file_drainer(&config.slow_query_path);
    let failed_drain = file_drainer(&config.failed_query_path);
    let drain = LogDispatcher::new(normal_drain, slow_drain, failed_drain);

    // Use async and init stdlog
    init_log_from_drain(
        drain,
        level,
        config.enable_async,
        config.async_channel_len,
        true,
    )
}

pub fn init_log_from_drain<D>(
    drain: D,
    level: Level,
    use_async: bool,
    async_log_channel_len: i32,
    init_stdlog: bool,
) -> Result<RuntimeLevel, SetLoggerError>
where
    D: Drain + Send + 'static,
    <D as Drain>::Err: std::fmt::Display,
{
    let runtime_level = RuntimeLevel::new(level);
    // TODO(yingwen): Consider print the error instead of just ignoring it?
    let root_logger = if use_async {
        let drain = if async_log_channel_len <= 0 {
            Async::new(drain.ignore_res())
                .chan_size(ASYNC_CHAN_SIZE)
                .overflow_strategy(OverflowStrategy::Block)
                .build()
        } else {
            Async::new(drain.ignore_res())
                .chan_size(async_log_channel_len as usize)
                .build()
        };
        let drain = RuntimeLevelFilter::new(drain, runtime_level.clone());
        slog::Logger::root(drain.ignore_res(), slog_o!())
    } else {
        let drain = RuntimeLevelFilter::new(Mutex::new(drain), runtime_level.clone());
        slog::Logger::root(drain.ignore_res(), slog_o!())
    };

    slog_global::set_global(root_logger);
    if init_stdlog {
        slog_global::redirect_std_log(Some(level))?;
    }

    Ok(runtime_level)
}

// e.g.
// ```text
// 2020-01-20 13:00:14.998 INFO [src/engine/rocksdb/rocks_kv.rs:394] RocksKV::open_with_op start, name:autogen
// ```
pub struct CeresFormat<D>
where
    D: Decorator,
{
    decorator: D,
}

impl<D> CeresFormat<D>
where
    D: Decorator,
{
    fn new(decorator: D) -> Self {
        Self { decorator }
    }
}

impl<D> Drain for CeresFormat<D>
where
    D: Decorator,
{
    type Err = io::Error;
    type Ok = ();

    fn log(&self, record: &Record, values: &OwnedKVList) -> Result<Self::Ok, Self::Err> {
        self.decorator.with_record(record, values, |decorator| {
            write_log_header(decorator, record)?;
            write_log_msg(decorator, record)?;
            write_log_fields(decorator, record, values)?;

            decorator.start_whitespace()?;
            writeln!(decorator)?;

            decorator.flush()?;

            Ok(())
        })
    }
}

#[derive(Clone)]
pub struct RuntimeLevel {
    level: Arc<AtomicUsize>,
    default_level: Level,
}

impl RuntimeLevel {
    fn new(default_level: Level) -> Self {
        Self {
            level: Arc::new(AtomicUsize::new(default_level.as_usize())),
            default_level,
        }
    }

    #[inline]
    pub fn current_level(&self) -> Level {
        Level::from_usize(self.level.load(Ordering::Relaxed)).unwrap_or(self.default_level)
    }

    pub fn set_level(&self, level: Level) {
        self.level.store(level.as_usize(), Ordering::Relaxed);
        // Log level of std log is not changed unless we call `log::set_max_level`
        log::set_max_level(convert_slog_level_to_log_level(level).to_level_filter());

        // We should not print things about logger use the logger...
        println!(
            "RuntimeLevel::set_level log level changed to {}",
            get_string_by_level(level)
        );
    }

    #[inline]
    pub fn reset(&self) {
        self.set_level(self.default_level);
    }

    #[inline]
    pub fn default_level(&self) -> Level {
        self.default_level
    }

    #[inline]
    pub fn current_level_str(&self) -> &str {
        get_string_by_level(self.current_level())
    }

    pub fn set_level_by_str(&self, level_str: &str) -> Result<(), String> {
        Level::from_str(level_str)
            .map_err(|_| format!("Invalid level {level_str}"))
            .and_then(|level| match level {
                Level::Trace | Level::Debug | Level::Info => Ok(level),
                _ => Err("Only allow to change log level to <trace|debug|info>".to_owned()),
            })
            .map(|level| self.set_level(level))
    }
}

struct RuntimeLevelFilter<D> {
    drain: D,
    runtime_level: RuntimeLevel,
}

impl<D> RuntimeLevelFilter<D> {
    fn new(drain: D, runtime_level: RuntimeLevel) -> Self {
        Self {
            drain,
            runtime_level,
        }
    }
}

impl<D> Drain for RuntimeLevelFilter<D>
where
    D: Drain,
{
    type Err = D::Err;
    type Ok = Option<D::Ok>;

    fn log(&self, record: &Record, values: &OwnedKVList) -> Result<Self::Ok, Self::Err> {
        let current_level = self.runtime_level.current_level();

        if record.level().is_at_least(current_level) {
            Ok(Some(self.drain.log(record, values)?))
        } else {
            Ok(None)
        }
    }
}

fn write_log_header(decorator: &mut dyn RecordDecorator, record: &Record<'_>) -> io::Result<()> {
    decorator.start_timestamp()?;
    write!(
        decorator,
        "{}",
        chrono::Local::now().format(TIMESTAMP_FORMAT)
    )?;

    decorator.start_whitespace()?;
    write!(decorator, " ")?;

    decorator.start_level()?;
    write!(decorator, "{}", record.level().as_short_str())?;

    decorator.start_whitespace()?;
    write!(decorator, " ")?;

    // Writes source file info.
    decorator.start_msg()?; // There is no `start_file()` or `start_line()`.
    write!(decorator, "[{}:{}]", record.file(), record.line())?;

    Ok(())
}

fn write_log_msg(decorator: &mut dyn RecordDecorator, record: &Record<'_>) -> io::Result<()> {
    decorator.start_whitespace()?;
    write!(decorator, " ")?;

    decorator.start_msg()?;
    write!(decorator, "{}", record.msg())?;

    Ok(())
}

fn write_log_fields(
    decorator: &mut dyn RecordDecorator,
    record: &Record<'_>,
    values: &OwnedKVList,
) -> io::Result<()> {
    let mut serializer = Serializer::new(decorator);

    record.kv().serialize(record, &mut serializer)?;

    values.serialize(record, &mut serializer)?;

    serializer.finish()?;

    Ok(())
}

struct Serializer<'a> {
    decorator: &'a mut dyn RecordDecorator,
}

impl<'a> Serializer<'a> {
    fn new(decorator: &'a mut dyn RecordDecorator) -> Self {
        Serializer { decorator }
    }

    fn write_whitespace(&mut self) -> io::Result<()> {
        self.decorator.start_whitespace()?;
        write!(self.decorator, " ")?;
        Ok(())
    }

    fn finish(self) -> io::Result<()> {
        Ok(())
    }
}

impl<'a> Drop for Serializer<'a> {
    fn drop(&mut self) {}
}

impl<'a> slog::Serializer for Serializer<'a> {
    fn emit_none(&mut self, key: Key) -> slog::Result {
        self.emit_arguments(key, &format_args!("None"))
    }

    fn emit_arguments(&mut self, key: Key, val: &fmt::Arguments<'_>) -> slog::Result {
        self.write_whitespace()?;

        // Write key
        write!(self.decorator, "[")?;
        self.decorator.start_key()?;
        write!(self.decorator, "{key}")?;

        // Write separator
        self.decorator.start_separator()?;
        write!(self.decorator, ":")?;

        // Write value
        self.decorator.start_value()?;
        write!(self.decorator, "{val}")?;
        self.decorator.reset()?;
        write!(self.decorator, "]")?;

        Ok(())
    }
}

pub fn init_test_logger() {
    // level
    let level = Level::Info;

    // drain
    let term_drain = term_drainer();
    let drain = LogDispatcher::new(
        term_drain,
        Option::<CeresFormat<PlainDecorator<File>>>::None,
        Option::<CeresFormat<PlainDecorator<File>>>::None,
    );

    // Use async and init stdlog
    let _ = init_log_from_drain(drain, level, false, 12400, true);
}

/// Timer for collecting slow query
#[derive(Debug)]
pub struct SlowTimer {
    slow_threshold: Duration,
    timer: Instant,
}

impl SlowTimer {
    pub fn new(threshold: Duration) -> SlowTimer {
        SlowTimer {
            slow_threshold: threshold,
            timer: Instant::now(),
        }
    }

    pub fn elapsed(&self) -> Duration {
        self.timer.elapsed()
    }

    pub fn is_slow(&self) -> bool {
        self.elapsed() >= self.slow_threshold
    }

    pub fn now(&self) -> Instant {
        self.timer
    }
}

#[macro_export(local_inner_macros)]
macro_rules! error {
    (target: $target:expr, $($arg:tt)+) => {{
        log_error!(target: $target, $($arg)+);
    }};

    ($($arg:tt)+) => {{
        log_error!(target: logger::DEFAULT_TAG, $($arg)+);
    }}
}

#[macro_export(local_inner_macros)]
macro_rules! warn {
    (target: $target:expr, $($arg:tt)+) => {{
        log_warn!(target: $target, $($arg)+);
    }};

    ($($arg:tt)+) => {{
        log_warn!(target: logger::DEFAULT_TAG, $($arg)+);
    }}
}

#[macro_export(local_inner_macros)]
macro_rules! info {
    (target: $target:expr, $($arg:tt)+) => {{
        log_info!(target: $target, $($arg)+);
    }};

    ($($arg:tt)+) => {{
        log_info!(target: logger::DEFAULT_TAG, $($arg)+);
    }}
}

#[macro_export(local_inner_macros)]
macro_rules! debug {
    (target: $target:expr, $($arg:tt)+) => {{
        log_debug!(target: $target, $($arg)+);
    }};

    ($($arg:tt)+) => {{
        log_debug!(target: logger::DEFAULT_TAG, $($arg)+);
    }}
}

#[macro_export(local_inner_macros)]
macro_rules! trace {
    (target: $target:expr, $($arg:tt)+) => {{
        log_trace!(target: $target, $($arg)+);
    }};

    ($($arg:tt)+) => {{
        log_trace!(target: logger::DEFAULT_TAG, $($arg)+);
    }}
}

#[macro_export(local_inner_macros)]
macro_rules! maybe_slow_query {
    ($t:expr, $($args:tt)*) => {{
        if $t.is_slow() {
            info!(target: logger::SLOW_QUERY_TAG, $($args)*);
        }
    }}
}

#[macro_export(local_inner_macros)]
macro_rules! failed_query {
    ($($args:tt)*) => {{
        info!(target: logger::FAILED_QUERY_TAG, $($args)*);
    }}
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_runtime_level() {
        let runtime_level = RuntimeLevel::new(Level::Info);

        assert_eq!(runtime_level.current_level(), Level::Info);
        assert_eq!(runtime_level.default_level(), Level::Info);

        runtime_level.set_level(Level::Debug);
        assert_eq!(runtime_level.current_level(), Level::Debug);
        assert_eq!(runtime_level.default_level(), Level::Info);

        runtime_level.reset();
        assert_eq!(runtime_level.current_level(), Level::Info);
        assert_eq!(runtime_level.current_level_str(), "info");

        runtime_level.set_level_by_str("trace").unwrap();
        assert_eq!(runtime_level.current_level(), Level::Trace);
        runtime_level.set_level_by_str("debug").unwrap();
        assert_eq!(runtime_level.current_level(), Level::Debug);
        runtime_level.set_level_by_str("info").unwrap();
        assert_eq!(runtime_level.current_level(), Level::Info);

        assert!(runtime_level.set_level_by_str("warn").is_err());
        assert_eq!(runtime_level.current_level(), Level::Info);
        assert!(runtime_level.set_level_by_str("warning").is_err());
        assert!(runtime_level.set_level_by_str("critical").is_err());
        assert!(runtime_level.set_level_by_str("error").is_err());
        assert!(runtime_level.set_level_by_str("no such level").is_err());

        assert_eq!(runtime_level.current_level(), Level::Info);
    }
}
