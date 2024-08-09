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

use std::{io::Result, path::Path};

fn main() -> Result<()> {
    if let Ok(flatc_bin) = std::env::var("FLATC") {
        std::env::set_var(
            "PATH",
            std::env::var("PATH").unwrap() + format!(":{}", flatc_bin.as_str()).as_str(),
        );
    } else {
        println!("FLATC enviroment variable not found!");
    }

    println!("cargo:rerun-if-changed=src/remote_engine.fbs");
    flatc_rust::run(flatc_rust::Args {
        inputs: &[Path::new("src/remote_engine.fbs")],
        out_dir: Path::new("target/flatbuffers/"),
        ..Default::default()
    })
    .expect("flatc");

    let greeter_service = tonic_build::manual::Service::builder()
        .name("RemoteEngineFbService")
        .package("remote_engine")
        .method(
            tonic_build::manual::Method::builder()
                .name("write")
                .route_name("Write")
                .input_type("crate::remote_service::FlatBufferBytes")
                .output_type("crate::remote_service::FlatBufferBytes")
                .codec_path("crate::remote_service::FlatBufferCodec")
                .build(),
        )
        .method(
            tonic_build::manual::Method::builder()
                .name("write_batch")
                .route_name("WriteBatch")
                .input_type("crate::remote_service::FlatBufferBytes")
                .output_type("crate::remote_service::FlatBufferBytes")
                .codec_path("crate::remote_service::FlatBufferCodec")
                .build(),
        )
        .build();

    tonic_build::manual::Builder::new()
        .out_dir("target/flatbuffers/")
        .compile(&[greeter_service]);
    Ok(())
}
