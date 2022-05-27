// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

use protobuf_builder::Builder;

fn generate_pb() {
    Builder::new()
        .out_dir("./src/protos")
        .search_dir_for_protos("protos")
        .generate();
}

fn main() {
    generate_pb();
}
