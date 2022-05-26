// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

//! utilities for sst.

use object_store::path::ObjectStorePath;
use table_engine::table::TableId;

use crate::{space::SpaceId, sst::manager::FileId};

const SST_FILE_SUFFIX: &str = "sst";

#[inline]
/// Generate the sst file name.
pub fn sst_file_name(id: FileId) -> String {
    format!("{}.{}", id, SST_FILE_SUFFIX)
}

/// Set the sst file path.
pub fn set_sst_file_path<P: ObjectStorePath>(
    space_id: SpaceId,
    table_id: TableId,
    file_id: FileId,
    path: &mut P,
) {
    path.push_all_dirs([space_id.to_string().as_str(), table_id.to_string().as_str()]);
    path.set_file_name(sst_file_name(file_id));
}
