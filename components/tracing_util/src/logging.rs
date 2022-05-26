// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

// Copyright 2020 Datafuse Labs.
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
    fs::OpenOptions,
    path::Path,
    sync::{Arc, Mutex, Once},
};

use fmt::format::FmtSpan;
use lazy_static::lazy_static;
use tracing::Subscriber;
use tracing_appender::{
    non_blocking::WorkerGuard,
    rolling::{RollingFileAppender, Rotation},
};
use tracing_subscriber::{
    fmt,
    fmt::{time::ChronoLocal, Layer},
    prelude::*,
    registry::Registry,
    EnvFilter,
};

/// Write logs to stdout.
pub fn init_default_tracing() {
    static START: Once = Once::new();

    START.call_once(|| {
        init_tracing_stdout();
    });
}

/// Init tracing for unittest.
/// Write logs to file `unittest`.
pub fn init_default_ut_tracing() {
    static START: Once = Once::new();

    START.call_once(|| {
        let mut g = GLOBAL_UT_LOG_GUARD.as_ref().lock().unwrap();
        let (work_guard, sub) = init_file_subscriber("unittest", "_logs");
        tracing::subscriber::set_global_default(sub)
            .expect("error setting global tracing subscriber");

        tracing::info!("init default ut tracing");
        *g = Some(work_guard);
    });
}

lazy_static! {
    static ref GLOBAL_UT_LOG_GUARD: Arc<Mutex<Option<WorkerGuard>>> = Arc::new(Mutex::new(None));
}

fn init_tracing_stdout() {
    let fmt_layer = Layer::default()
        .with_thread_ids(true)
        .with_thread_names(false)
        .with_ansi(false)
        .with_span_events(fmt::format::FmtSpan::FULL);

    let subscriber = Registry::default()
        .with(EnvFilter::from_default_env())
        .with(fmt_layer);

    tracing::subscriber::set_global_default(subscriber)
        .expect("error setting global tracing subscriber");
}

/// Write logs to file and rotation.
pub fn init_tracing_with_file(
    app_name: &str,
    dir: impl AsRef<Path>,
    level: &str,
    rotation: Rotation,
) -> WorkerGuard {
    let file_appender = RollingFileAppender::new(rotation, dir, app_name);
    let (file_writer, file_guard) = tracing_appender::non_blocking(file_appender);
    let f_layer = Layer::new()
        .with_timer(ChronoLocal::rfc3339())
        .with_writer(file_writer)
        .with_thread_ids(true)
        .with_thread_names(true)
        .with_ansi(false)
        .with_span_events(FmtSpan::ENTER | FmtSpan::CLOSE);

    let subscriber = Registry::default()
        .with(EnvFilter::new(level))
        .with(f_layer);

    tracing::subscriber::set_global_default(subscriber)
        .expect("error setting global tracing subscriber");

    file_guard
}

/// Create a file based tracing/logging subscriber.
/// A guard must be held during using the logging.
fn init_file_subscriber(app_name: &str, dir: &str) -> (WorkerGuard, impl Subscriber) {
    let path_str = dir.to_string() + "/" + app_name;
    let path: &Path = path_str.as_ref();

    // open log file

    let mut open_options = OpenOptions::new();
    open_options.append(true).create(true);

    let mut open_res = open_options.open(path);
    if open_res.is_err() {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).unwrap();
            open_res = open_options.open(path);
        }
    }

    let f = open_res.unwrap();

    // build subscriber

    let (writer, writer_guard) = tracing_appender::non_blocking(f);

    let f_layer = Layer::new()
        .with_timer(ChronoLocal::rfc3339())
        .with_writer(writer)
        .with_thread_ids(true)
        .with_thread_names(false)
        .with_ansi(false)
        .with_span_events(FmtSpan::ENTER | FmtSpan::CLOSE);

    let subscriber = Registry::default()
        .with(EnvFilter::from_default_env())
        .with(f_layer);

    (writer_guard, subscriber)
}
