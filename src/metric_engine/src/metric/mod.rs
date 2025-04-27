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

use columnar_storage::storage::TimeMergeStorageRef;

use crate::{types::Sample, Result};

pub struct MetricManager {
    inner: Arc<Inner>,
}

impl MetricManager {
    pub fn new(storage: TimeMergeStorageRef) -> Self {
        Self {
            inner: Arc::new(Inner { storage }),
        }
    }

    /// Populate metric ids from names.
    /// If a name does not exist, it will be created on demand.
    pub async fn populate_metric_ids(&self, _samples: &mut [Sample]) -> Result<()> {
        todo!()
    }
}

struct Inner {
    storage: TimeMergeStorageRef,
}

impl Inner {
    async fn populate_metric_ids(&self, _samples: &mut [Sample]) -> Result<()> {
        todo!()
    }
}
