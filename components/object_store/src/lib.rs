// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

//! Re-export of [object_store] crate.

use std::sync::Arc;

pub use upstream::{
    local::LocalFileSystem, path::Path, Error as ObjectStoreError, GetResult, ListResult,
    ObjectMeta, ObjectStore,
};

pub mod aliyun;
pub mod disk_cache;
pub mod mem_cache;
pub mod metrics;
pub mod multipart;
pub mod obkv;
pub mod prefix;

pub type ObjectStoreRef = Arc<dyn ObjectStore>;
