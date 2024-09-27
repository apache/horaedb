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
    raw::HttpClient,
    services::S3,
    Operator, Result,
};

use crate::config::S3Options;

pub fn try_new(s3_option: &S3Options) -> Result<OpendalStore> {
    let http_builder = reqwest::ClientBuilder::new()
        .pool_max_idle_per_host(s3_option.http.pool_max_idle_per_host)
        .http2_keep_alive_timeout(s3_option.http.keep_alive_timeout.0)
        .http2_keep_alive_while_idle(true)
        .http2_keep_alive_interval(s3_option.http.keep_alive_interval.0)
        .timeout(s3_option.http.timeout.0);
    let http_client = HttpClient::build(http_builder)?;

    let builder = S3::default()
        .region(&s3_option.region)
        .access_key_id(&s3_option.key_id)
        .secret_access_key(&s3_option.key_secret)
        .endpoint(&s3_option.endpoint)
        .bucket(&s3_option.bucket)
        .http_client(http_client);
    let op = Operator::new(builder)?
        .layer(
            TimeoutLayer::new()
                .with_timeout(s3_option.timeout.timeout.0)
                .with_io_timeout(s3_option.timeout.io_timeout.0),
        )
        .layer(RetryLayer::new().with_max_times(s3_option.max_retries))
        .finish();

    Ok(OpendalStore::new(op))
}
