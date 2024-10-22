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

use anyhow::Context;
use object_store::path::Path;

use crate::{
    sst::{FileId, FileMeta},
    types::ObjectStoreRef,
    Result,
};

pub const PREFIX_PATH: &str = "manifest";
pub const SNAPSHOT_FILENAME: &str = "snapshot";

pub struct Manifest {
    path: String,
    store: ObjectStoreRef,
}

impl Manifest {
    pub async fn try_new(path: String, store: ObjectStoreRef) -> Result<Self> {
        let snapshot_path = Path::from(format!("{path}/{SNAPSHOT_FILENAME}"));
        let bytes = store
            .get(&snapshot_path)
            .await
            .with_context(|| format!("failed to get manifest snapshot, path: {path}"))?;
        // TODO: decode bytes into manifest details
        Ok(Self { path, store })
    }

    pub async fn add_file(&self, id: FileId, meta: FileMeta) -> Result<()> {
        // TODO: implement this later
        Ok(())
    }
}
