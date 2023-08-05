// Copyright 2023 The CeresDB Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

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
