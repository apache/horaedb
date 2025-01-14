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

use horaedb_storage::storage::TimeMergeStorageRef;

use crate::{types::Sample, Result};

pub struct SampleManager {
    inner: Arc<Inner>,
}

impl SampleManager {
    pub fn new(storage: TimeMergeStorageRef) -> Self {
        Self {
            inner: Arc::new(Inner { storage }),
        }
    }

    /// Populate series ids from labels.
    /// It will also build inverted index for labels.
    pub async fn persist(&self, _samples: Vec<Sample>) -> Result<()> {
        todo!()
    }
}

struct Inner {
    storage: TimeMergeStorageRef,
}
