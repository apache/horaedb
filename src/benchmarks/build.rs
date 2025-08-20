// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use std::{env, fs, path::PathBuf, process::Command};

fn main() {
    // Similar to prost, we generate rust-protobuf and quick-protobuf code to
    // OUT_DIR instead of the src directory.
    let proto_path = "../pb_types/protos/remote_write.proto";
    let include_path = "../pb_types/protos";
    let out_dir = env::var("OUT_DIR").unwrap();
    let out_dir_path = PathBuf::from(&out_dir);

    // Generate rust-protobuf code to OUT_DIR.
    protobuf_codegen::Codegen::new()
        .pure()
        .out_dir(&out_dir)
        .input(proto_path)
        .include(include_path)
        .run()
        .expect("rust-protobuf code generation failed");

    // Rename rust-protobuf generated file to avoid potential conflicts.
    let src_file = out_dir_path.join("remote_write.rs");
    let dst_file = out_dir_path.join("rust_protobuf_remote_write.rs");
    fs::rename(&src_file, &dst_file).expect("rust-protobuf file rename failed");

    // Generate quick-protobuf code to OUT_DIR using pb-rs command line tool.
    let quick_protobuf_file = out_dir_path.join("quick_protobuf_remote_write.rs");
    let output = Command::new("pb-rs")
        .args([
            "-I",
            include_path,
            "-o",
            quick_protobuf_file.to_str().unwrap(),
            "-s",
            proto_path,
        ])
        .output()
        .expect("pb-rs command execution failed");

    if !output.status.success() {
        panic!(
            "pb-rs command execution failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
    }

    // Fix package namespace conflicts and inner attributes using sed.
    let _ = Command::new("sed")
        .args([
            "-i",
            "-e",
            "s/remote_write:://g",
            "-e",
            r"s/#!\[/#[/g",
            "-e",
            r"s/^\/\/! /\/\/ /g",
            "-e",
            "s/pb_types::mod_MetricMetadata::MetricType/mod_MetricMetadata::MetricType/g",
            quick_protobuf_file.to_str().unwrap(),
        ])
        .output()
        .expect("sed command execution failed");

    // Fix inner attributes in rust-protobuf generated file.
    let _ = Command::new("sed")
        .args([
            "-i",
            "-e",
            r"s/#!\[/#[/g",
            "-e",
            r"s/^\/\/! /\/\/ /g",
            dst_file.to_str().unwrap(),
        ])
        .output()
        .expect("sed command execution failed");

    println!("cargo:rerun-if-changed={}", proto_path);
}
