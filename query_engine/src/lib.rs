// Copyright 2022-2023 CeresDB Project Authors. Licensed under Apache-2.0.

//! Query engine
//!
//! Optimizes and executes logical plan

// TODO(yingwen): Maybe renamed to query_executor or query_backend?
// TODO(yingwen): Use datafusion or fuse-query as query backend

pub mod config;
pub mod context;
pub mod datafusion_impl;
pub mod error;
pub mod executor;
pub mod physical_planner;
pub use config::Config;
