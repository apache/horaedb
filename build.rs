// Copyright 2022-2023 CeresDB Project Authors. Licensed under Apache-2.0.

//! Build script

use vergen::EmitBuilder;

fn main() {
    EmitBuilder::builder()
        .all_cargo()
        .all_build()
        .all_rustc()
        .git_branch()
        .git_sha(true)
        .emit()
        .expect("Should succeed emit version message");
}
