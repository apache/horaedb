// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

//! utilities for sst.

use std::iter::FromIterator;

use object_store::Path;
use table_engine::table::TableId;

use crate::{space::SpaceId, sst::manager::FileId};

const SST_FILE_SUFFIX: &str = "sst";
const SST_CUSTOM_METADATA_FILE_SUFFIX: &str = "custommeta";

#[inline]
/// Generate the sst file name.
pub fn sst_file_name(id: FileId) -> String {
    format!("{id}.{SST_FILE_SUFFIX}")
}

pub fn new_sst_file_path(space_id: SpaceId, table_id: TableId, file_id: FileId) -> Path {
    Path::from_iter([
        space_id.to_string(),
        table_id.to_string(),
        sst_file_name(file_id),
    ])
}

/// Convert sst_file_path into custom metadata path
/// TODO: Using more complex mechanisms to get the path
pub fn new_custom_metadata_path(sst_file_path: &Path) -> Path {
    Path::from(format!("{sst_file_path}{SST_CUSTOM_METADATA_FILE_SUFFIX}"))
}
