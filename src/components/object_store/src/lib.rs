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

//! Re-export of [object_store] crate.

use std::sync::Arc;

pub use multi_part::{ConcurrentMultipartUpload, MultiUploadWriter};
use tokio::sync::Mutex;
pub use upstream::{
    local::LocalFileSystem, path::Path, Error as ObjectStoreError, Error, GetResult, ListResult,
    ObjectMeta, ObjectStore, PutPayloadMut,
};

pub mod aliyun;
pub mod config;
pub mod disk_cache;
pub mod mem_cache;
pub mod metrics;
mod multi_part;
pub mod prefix;
pub mod s3;
#[cfg(test)]
pub mod test_util;

pub type ObjectStoreRef = Arc<dyn ObjectStore>;

// TODO: remove Mutex and make ConcurrentMultipartUpload thread-safe
pub type WriteMultipartRef = Arc<Mutex<ConcurrentMultipartUpload>>;
