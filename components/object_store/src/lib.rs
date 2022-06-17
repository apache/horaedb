// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

//! Re-export of [object_store] crate.

pub use upstream::{
    local::LocalFileSystem, path::Path, Error as ObjectStoreError, GetResult, ListResult,
    ObjectMeta, ObjectStore,
};

pub mod aliyun;
pub mod cache;
