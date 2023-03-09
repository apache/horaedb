// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

use std::time::Duration;

use upstream::{
    aws::{AmazonS3, AmazonS3Builder},
    ClientOptions,
};

pub fn try_new(
    key_id: impl Into<String>,
    key_secret: impl Into<String>,
    endpoint: impl Into<String>,
    bucket: impl Into<String>,
    pool_max_idle_per_host: impl Into<usize>,
    timeout: Duration,
) -> upstream::Result<AmazonS3> {
    let cli_opt = ClientOptions::new()
        .with_pool_max_idle_per_host(pool_max_idle_per_host.into())
        .with_timeout(timeout);
    AmazonS3Builder::new()
        .with_access_key_id(key_id)
        .with_secret_access_key(key_secret)
        .with_bucket_name(bucket)
        .with_url(endpoint)
        .with_client_options(cli_opt)
        .build()
}
