// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_build::configure().out_dir("./src/protos").compile(
        &[
            "protos/analytic_common.proto",
            "protos/common.proto",
            "protos/meta_update.proto",
            "protos/sst.proto",
            "protos/sys_catalog.proto",
            "protos/table_requests.proto",
        ],
        &["protos"],
    )?;
    Ok(())
}
