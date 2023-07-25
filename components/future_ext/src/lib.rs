// Copyright 2022-2023 CeresDB Project Authors. Licensed under Apache-2.0.

//! Future extensions.

mod cancel;
mod retry;

pub use cancel::CancellationSafeFuture;
pub use retry::{retry_async, RetryConfig};
