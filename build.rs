// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

//! Build script

use std::env;

use vergen::{vergen, Config, ShaKind};

fn main() {
    // Generate the default 'cargo:' instruction output
    let mut config = Config::default();
    // Change the SHA output to the short variant
    *config.git_mut().sha_kind_mut() = ShaKind::Short;
    // Override git branch by env if provided.
    if let Some(branch) = env::var_os("GITBRANCH") {
        let branch = branch
            .into_string()
            .expect("Convert git branch env to string");
        if !branch.is_empty() {
            *config.git_mut().branch_mut() = false;
            println!("cargo:rustc-env=VERGEN_GIT_BRANCH={}", branch);
        }
    }

    let _ = vergen(config);
}
