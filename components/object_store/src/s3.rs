// Copyright 2023 CeresDB Project Authors. Licensed under Apache-2.0.

use std::time::Duration;

use upstream::{
    aws::{AmazonS3, AmazonS3Builder},
    ClientOptions, RetryConfig,
};

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

#[allow(clippy::too_many_arguments)]
pub fn try_new(
    region: impl Into<String>,
    key_id: impl Into<String>,
    key_secret: impl Into<String>,
    endpoint: impl Into<String>,
    bucket: impl Into<String>,
    pool_max_idle_per_host: impl Into<usize>,
    timeout: Duration,
    keep_alive_timeout: Duration,
    keep_alive_interval: Duration,
) -> upstream::Result<AmazonS3> {
    let cli_opt = ClientOptions::new()
        .with_allow_http(true)
        .with_pool_max_idle_per_host(pool_max_idle_per_host.into())
        .with_http2_keep_alive_timeout(keep_alive_timeout)
        .with_http2_keep_alive_while_idle()
        .with_http2_keep_alive_interval(keep_alive_interval)
        .with_timeout(timeout);
    let retry_config = RetryConfig {
        // TODO: add to config
        max_retries: 3,
        ..Default::default()
    };

    let endpoint = endpoint.into();
    let bucket = bucket.into();
    let endpoint = normalize_endpoint(&endpoint, &bucket);
    AmazonS3Builder::new()
        .with_virtual_hosted_style_request(true)
        .with_region(region)
        .with_access_key_id(key_id)
        .with_secret_access_key(key_secret)
        .with_endpoint(endpoint)
        .with_bucket_name(bucket)
        .with_client_options(cli_opt)
        .with_retry(retry_config)
        .build()
}
