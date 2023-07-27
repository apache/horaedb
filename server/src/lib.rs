// Copyright 2022-2023 CeresDB Project Authors. Licensed under Apache-2.0.

//! Rpc server

// TODO(yingwen):
// Borrow some ideas from tikv: https://github.com/tikv/tikv/blob/dc8ce2cf6a8904cb3dad556f71b11bac3531689b/src/server/service/kv.rs#L51

pub mod config;
mod consts;
mod dedup_requests;
mod error_util;
mod grpc;
mod http;
pub mod local_tables;
mod metrics;
mod mysql;
pub mod server;
