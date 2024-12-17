// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use crate::{
    sst::{FileId, SstFile},
    Error, Result,
};

#[derive(Clone, Debug)]
pub struct ManifestUpdate {
    pub to_adds: Vec<SstFile>,
    pub to_deletes: Vec<FileId>,
}

impl ManifestUpdate {
    pub fn new(to_adds: Vec<SstFile>, to_deletes: Vec<FileId>) -> Self {
        Self {
            to_adds,
            to_deletes,
        }
    }
}

impl TryFrom<pb_types::ManifestUpdate> for ManifestUpdate {
    type Error = Error;

    fn try_from(value: pb_types::ManifestUpdate) -> Result<Self> {
        let to_adds = value
            .to_adds
            .into_iter()
            .map(SstFile::try_from)
            .collect::<Result<Vec<_>>>()?;

        Ok(Self {
            to_adds,
            to_deletes: value.to_deletes,
        })
    }
}

impl From<ManifestUpdate> for pb_types::ManifestUpdate {
    fn from(value: ManifestUpdate) -> Self {
        let to_adds = value
            .to_adds
            .into_iter()
            .map(pb_types::SstFile::from)
            .collect();

        pb_types::ManifestUpdate {
            to_adds,
            to_deletes: value.to_deletes,
        }
    }
}
