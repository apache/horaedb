// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

use arrow_deps::arrow::util::pretty;
use common_types::record_batch::RecordBatch;

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
        "\n\nexpected:\n\n{:#?}\nactual:\n\n{:#?}\n\n",
        expected_lines, actual_lines
    );
}
