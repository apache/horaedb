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

use object_store_opendal::OpendalStore;
use opendal::{
    layers::{RetryLayer, TimeoutLayer},
    services::Fs,
    Operator, Result,
};

use crate::config::LocalOptions;

pub fn try_new(local_opts: &LocalOptions) -> Result<OpendalStore> {
    let builder = Fs::default().root(&local_opts.data_dir);
    let op = Operator::new(builder)?
        .layer(
            TimeoutLayer::new()
                .with_timeout(local_opts.timeout.timeout.0)
                .with_io_timeout(local_opts.timeout.io_timeout.0),
        )
        .layer(RetryLayer::new().with_max_times(local_opts.max_retries))
        .finish();

    Ok(OpendalStore::new(op))
}

pub fn try_new_with_default(data_dir: String) -> Result<OpendalStore> {
    let local_opts = LocalOptions::new_with_default(data_dir);
    try_new(&local_opts)
}
