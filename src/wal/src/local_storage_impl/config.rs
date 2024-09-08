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

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct LocalStorageConfig {
    pub data_dir: String,
    pub segment_size: usize,
    pub cache_size: usize,
}

impl Default for LocalStorageConfig {
    fn default() -> Self {
        Self {
            data_dir: "/tmp/horaedb".to_string(),
            segment_size: 64 * 1024 * 1024, // 64MB
            cache_size: 3,
        }
    }
}
