// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

//! Re-export of [object_store] crate.

pub use object_store::{
    local::LocalFileSystem, path::Path, Error as ObjectStoreError, GetResult, ListResult,
    ObjectMeta, ObjectStore,
};
