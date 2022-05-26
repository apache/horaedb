// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

//! Manage meta data of the engine

pub mod details;
pub mod meta_data;
pub mod meta_update;

use std::fmt;

use async_trait::async_trait;

use crate::meta::{meta_data::ManifestData, meta_update::MetaUpdate};

/// Manifest holds meta data of all tables
#[async_trait]
pub trait Manifest: fmt::Debug {
    type Error: std::error::Error + Send + Sync + 'static;

    /// Store update to manifest
    async fn store_update(&self, update: MetaUpdate) -> Result<(), Self::Error>;

    /// Load all data from manifest.
    ///
    /// If `do_snapshot` is true, the manifest will try to create a snapshot of
    /// the manifest data. The caller should ensure `store_update()` wont be
    /// called during loading data.
    async fn load_data(&self, do_snapshot: bool) -> Result<ManifestData, Self::Error>;
}
