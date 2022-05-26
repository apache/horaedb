// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

//! Protobuf messages

// TODO(yingwen): All the protos need review
mod protos {
    include!(concat!(env!("OUT_DIR"), "/protos/mod.rs"));
}

pub use protos::*;
