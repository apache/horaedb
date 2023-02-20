// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

use std::{env, error::Error, fs, path::PathBuf, str::FromStr};

use arrow::{array::*, datatypes::DataType, record_batch::RecordBatch};
use parquet::record::{Field, Row};

fn get_data_dir(
    udf_env: &str,
    submodule_data: &str,
) -> std::result::Result<PathBuf, Box<dyn Error>> {
    // Try user defined env.
    if let Ok(dir) = env::var(udf_env) {
        let trimmed = dir.trim().to_string();
        if !trimmed.is_empty() {
            let pb = PathBuf::from(trimmed);
            if pb.is_dir() {
                return Ok(pb);
            } else {
                return Err(format!(
                    "the data dir `{}` defined by env {} not found",
                    pb.display(),
                    udf_env
                )
                .into());
            }
        }
    }

    // The env is undefined or its value is trimmed to empty, let's try default dir.

    // env "CARGO_MANIFEST_DIR" is "the directory containing the manifest of your
    // package", set by `cargo run` or `cargo test`, see:
    // https://doc.rust-lang.org/cargo/reference/environment-variables.html
    let dir = env!("CARGO_MANIFEST_DIR");

    let pb = PathBuf::from(dir).join(submodule_data);
    if pb.is_dir() {
        Ok(pb)
    } else {
        Err(format!(
            "env `{}` is undefined or has empty value, and the pre-defined data dir `{}` not found\n\
                HINT: try running `git submodule update --init`",
            udf_env,
            pb.display(),
        ).into())
    }
}

fn parquet_test_data() -> String {
    match get_data_dir("PARQUET_TEST_DATA", "../parquet-testing/data") {
        Ok(pb) => pb.display().to_string(),
        Err(err) => panic!("failed to get parquet data dir: {err}"),
    }
}

/// Returns path to the test parquet file in 'data' directory
fn get_test_path(file_name: &str) -> PathBuf {
    let mut pathbuf = PathBuf::from_str(&parquet_test_data()).unwrap();
    pathbuf.push(file_name);
    pathbuf
}

/// Returns file handle for a test parquet file from 'data' directory
pub fn get_test_file(file_name: &str) -> fs::File {
    let path = get_test_path(file_name);
    fs::File::open(path.as_path()).unwrap_or_else(|err| {
        panic!(
            "Test file {} could not be opened, did you do `git submodule update`?: {}",
            path.display(),
            err
        )
    })
}

struct RowViewOfRecordBatch<'a> {
    record_batch: &'a RecordBatch,
    row_idx: usize,
}

impl<'a> RowViewOfRecordBatch<'a> {
    fn check_row(&self, expect_row: &Row) {
        for (col_idx, (_, field)) in expect_row.get_column_iter().enumerate() {
            let array_ref = self.record_batch.column(col_idx);

            match array_ref.data_type() {
                DataType::Binary => {
                    let array = array_ref.as_any().downcast_ref::<BinaryArray>().unwrap();
                    let v = array.value(self.row_idx);

                    if let Field::Bytes(field_value) = field {
                        assert_eq!(v, field_value.data());
                    } else {
                        panic!("different value type");
                    }
                }
                _ => unimplemented!("not support {:?}", array_ref.data_type()),
            }
        }
    }
}

pub fn check_rows_and_record_batches(rows: &[Row], record_batches: &[RecordBatch]) {
    let mut row_idx = 0;
    for record_batch in record_batches {
        for row_idx_in_batch in 0..record_batch.num_rows() {
            let expect_row = &rows[row_idx];
            let row_view = RowViewOfRecordBatch {
                record_batch,
                row_idx: row_idx_in_batch,
            };
            row_view.check_row(expect_row);
            row_idx += 1;
        }
    }
}
