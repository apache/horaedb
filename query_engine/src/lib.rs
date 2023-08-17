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

//! Query engine
//!
//! Optimizes and executes logical plan

pub mod config;
pub mod context;
pub mod datafusion_impl;
pub mod encoding;
pub mod error;
pub mod executor;
pub mod physical_planner;
use std::fmt;

pub use config::Config;

use crate::{
    encoding::PhysicalPlanEncoderRef, executor::ExecutorRef, physical_planner::PhysicalPlannerRef,
};

pub trait QueryEngine: fmt::Debug + Send + Sync {
    fn physical_planner(&self) -> PhysicalPlannerRef;

    fn executor(&self) -> ExecutorRef;

    fn physical_plan_encoder(&self) -> PhysicalPlanEncoderRef;
}

pub type QueryEngineRef = Box<dyn QueryEngine>;
