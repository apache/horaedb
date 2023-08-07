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

use upstream::{
    aws::{AmazonS3, AmazonS3Builder},
    ClientOptions, RetryConfig,
};

use crate::config::S3Options;

pub fn try_new(s3_option: &S3Options) -> upstream::Result<AmazonS3> {
    let cli_opt = ClientOptions::new()
        .with_allow_http(true)
        .with_pool_max_idle_per_host(s3_option.http.pool_max_idle_per_host)
        .with_http2_keep_alive_timeout(s3_option.http.keep_alive_timeout.0)
        .with_http2_keep_alive_while_idle()
        .with_http2_keep_alive_interval(s3_option.http.keep_alive_interval.0)
        .with_timeout(s3_option.http.timeout.0);
    let retry_config = RetryConfig {
        max_retries: s3_option.retry.max_retries,
        retry_timeout: s3_option.retry.retry_timeout.0,
        ..Default::default()
    };

    AmazonS3Builder::new()
        .with_region(&s3_option.region)
        .with_access_key_id(&s3_option.key_id)
        .with_secret_access_key(&s3_option.key_secret)
        .with_endpoint(&s3_option.endpoint)
        .with_bucket_name(&s3_option.bucket)
        .with_client_options(cli_opt)
        .with_retry(retry_config)
        .build()
}
