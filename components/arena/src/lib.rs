// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

//! `Arena` Trait and implementations.

mod arena_trait;
mod fixed_size;
mod mono_inc;

pub use arena_trait::{Arena, BasicStats, Collector, CollectorRef};
pub use fixed_size::FixedSizeArena;
pub use mono_inc::{MonoIncArena, NoopCollector};
