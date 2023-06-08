// Copyright 2023 CeresDB Project Authors. Licensed under Apache-2.0.

use upstream::{
    aws::{AmazonS3, AmazonS3Builder},
    ClientOptions, RetryConfig,
};
use crate::storage_options::S3Options;

pub fn try_new(
    s3_option: &S3Options
) -> upstream::Result<AmazonS3> {
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
