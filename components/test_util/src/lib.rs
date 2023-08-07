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

//! Common utils shared by the whole project

use std::{io::Write, sync::Once};

use arrow::util::pretty;
use common_types::record_batch::RecordBatch;

static INIT_LOG: Once = Once::new();

pub fn init_log_for_test() {
    INIT_LOG.call_once(|| {
        env_logger::Builder::from_default_env()
            .format(|buf, record| {
                writeln!(
                    buf,
                    "{} {} [{}:{}] {}",
                    chrono::Local::now().format("%Y-%m-%dT%H:%M:%S.%3f"),
                    buf.default_styled_level(record.level()),
                    record.file().unwrap_or("unknown"),
                    record.line().unwrap_or(0),
                    record.args()
                )
            })
            .init();
    });
}

/// A helper function to assert record batch.
pub fn assert_record_batches_eq(expected: &[&str], record_batches: Vec<RecordBatch>) {
    let arrow_record_batch = record_batches
        .into_iter()
        .map(|record| record.into_arrow_record_batch())
        .collect::<Vec<_>>();

    let expected_lines: Vec<String> = expected.iter().map(|&s| s.into()).collect();
    let formatted = pretty::pretty_format_batches(arrow_record_batch.as_slice())
        .unwrap()
        .to_string();
    let actual_lines: Vec<&str> = formatted.trim().lines().collect();
    assert_eq!(
        expected_lines, actual_lines,
        "\n\nexpected:\n\n{expected_lines:#?}\nactual:\n\n{actual_lines:#?}\n\n"
    );
}
