// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

//! This crate exists to add a dependency on (likely as yet
//! unpublished) versions of arrow / datafusion so we can
//! manage the version used by ceresdb in a single crate.

pub mod display;
pub mod util;

// export arrow and datafusion publically so we can have a single
// reference in cargo
pub use arrow;
pub use datafusion;
pub use parquet;
