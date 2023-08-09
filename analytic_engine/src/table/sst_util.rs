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

//! utilities for sst.

use std::iter::FromIterator;

use object_store::Path;
use table_engine::table::TableId;

use crate::{space::SpaceId, sst::manager::FileId};

const SST_FILE_SUFFIX: &str = "sst";
const SST_CUSTOM_METADATA_FILE_SUFFIX: &str = "metadata";

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
