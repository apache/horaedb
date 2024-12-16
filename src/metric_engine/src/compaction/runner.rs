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

use std::sync::Arc;

use crate::{
    compaction::Task, manifest::ManifestRef, sst::SstPathGenerator, types::ObjectStoreRef, Result,
};

pub struct Runner {
    store: ObjectStoreRef,
    manifest: ManifestRef,
    sst_path_gen: Arc<SstPathGenerator>,
}

impl Runner {
    pub fn new(
        store: ObjectStoreRef,
        manifest: ManifestRef,
        sst_path_gen: Arc<SstPathGenerator>,
    ) -> Self {
        Self {
            store,
            manifest,
            sst_path_gen,
        }
    }

    // TODO: Merge input sst files into one new sst file
    // and delete the expired sst files
    pub async fn do_compaction(&self, task: Task) -> Result<()> {
        for f in &task.inputs {
            assert!(f.is_compaction());
        }
        for f in &task.expireds {
            assert!(f.is_compaction());
        }
        todo!()
    }
}
