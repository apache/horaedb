// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

//! utilities for sst.

use std::iter::FromIterator;

use iox_object_store::Path;
use table_engine::table::TableId;

use crate::{space::SpaceId, sst::manager::FileId};

const SST_FILE_SUFFIX: &str = "sst";

#[inline]
/// Generate the sst file name.
pub fn sst_file_name(id: FileId) -> String {
    format!("{}.{}", id, SST_FILE_SUFFIX)
}

pub fn new_sst_file_path(space_id: SpaceId, table_id: TableId, file_id: FileId) -> Path {
    Path::from_iter(vec![
        space_id.to_string(),
        table_id.to_string(),
        sst_file_name(file_id),
    ])
}
