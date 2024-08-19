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
    services::Oss,
    Operator, Result,
};

use crate::config::AliyunOptions;

fn normalize_endpoint(endpoint: &str, bucket: &str) -> String {
    if endpoint.starts_with("https") {
        format!(
            "https://{}.{}",
            bucket,
            endpoint.replacen("https://", "", 1)
        )
    } else {
        format!("http://{}.{}", bucket, endpoint.replacen("http://", "", 1))
    }
}

pub fn try_new(aliyun_opts: &AliyunOptions) -> Result<OpendalStore> {
    let http_builder = reqwest::ClientBuilder::new()
        .pool_max_idle_per_host(aliyun_opts.http.pool_max_idle_per_host)
        .http2_keep_alive_timeout(aliyun_opts.http.keep_alive_timeout.0)
        .http2_keep_alive_while_idle(true)
        .http2_keep_alive_interval(aliyun_opts.http.keep_alive_interval.0)
        .timeout(aliyun_opts.http.timeout.0);
    let http_client = HttpClient::build(http_builder)?;

    let endpoint = &aliyun_opts.endpoint;
    let bucket = &aliyun_opts.bucket;
    let endpoint = normalize_endpoint(endpoint, bucket);

    let builder = Oss::default()
        .access_key_id(&aliyun_opts.key_id)
        .access_key_secret(&aliyun_opts.key_secret)
        .endpoint(&endpoint)
        .bucket(bucket)
        .http_client(http_client);
    let op = Operator::new(builder)?
        .layer(
            TimeoutLayer::new()
                .with_timeout(aliyun_opts.timeout.timeout.0)
                .with_io_timeout(aliyun_opts.timeout.io_timeout.0),
        )
        .layer(RetryLayer::new().with_max_times(aliyun_opts.max_retries))
        .finish();

    Ok(OpendalStore::new(op))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_normalize_endpoint() {
        let testcase = [
            (
                "https://oss.aliyun.com",
                "test",
                "https://test.oss.aliyun.com",
            ),
            (
                "http://oss.aliyun.com",
                "test",
                "http://test.oss.aliyun.com",
            ),
            ("no-scheme.com", "test", "http://test.no-scheme.com"),
        ];

        for (endpoint, bucket, expected) in testcase {
            let actual = normalize_endpoint(endpoint, bucket);
            assert_eq!(expected, actual);
        }
    }
}
