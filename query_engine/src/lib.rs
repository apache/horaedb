// Copyright 2022-2023 CeresDB Project Authors. Licensed under Apache-2.0.

//! Query engine
//!
//! Optimizes and executes logical plan

pub mod config;
pub mod context;
pub mod datafusion_impl;
pub mod error;
pub mod executor;
pub mod physical_planner;
pub use config::Config;
