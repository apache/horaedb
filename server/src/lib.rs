// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

//! Rpc server

// TODO(yingwen):
// Borrow some ideas from tikv: https://github.com/tikv/tikv/blob/dc8ce2cf6a8904cb3dad556f71b11bac3531689b/src/server/service/kv.rs#L51

#[macro_use]
extern crate common_util;

pub mod config;
mod consts;
mod context;
pub(crate) mod error_util;
mod grpc;
mod handlers;
mod http;
mod instance;
pub mod limiter;
pub mod local_tables;
mod metrics;
mod mysql;
pub(crate) mod proxy;
pub mod schema_config_provider;
pub mod server;
