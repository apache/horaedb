// Copyright 2022-2023 CeresDB Project Authors. Licensed under Apache-2.0.

//! Util function to retry future.

use std::time::Duration;

use futures::Future;

pub struct RetryConfig {
    pub max_retries: usize,
    pub interval: Duration,
}

impl Default for RetryConfig {
    fn default() -> Self {
        Self {
            max_retries: 3,
            interval: Duration::from_millis(500),
        }
    }
}

// Copy from https://github.com/jimmycuadra/retry/blob/2.0.0/src/delay/random.rs#L73
pub fn jitter(duration: Duration) -> Duration {
    let jitter = rand::random::<f64>();
    let secs = ((duration.as_secs() as f64) * jitter).ceil() as u64;
    let nanos = ((f64::from(duration.subsec_nanos())) * jitter).ceil() as u32;
    Duration::new(secs, nanos)
}

pub async fn retry_async<F, Fut, T, E>(f: F, config: &RetryConfig) -> Fut::Output
where
    F: Fn() -> Fut,
    Fut: Future<Output = Result<T, E>>,
{
    for _ in 0..config.max_retries {
        let result = f().await;

        if result.is_ok() {
            return result;
        }
        tokio::time::sleep(jitter(config.interval)).await;
    }

    f().await
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicU8, Ordering};

    use super::*;

    #[tokio::test]
    async fn test_retry_async() {
        let config = RetryConfig {
            max_retries: 3,
            interval: Duration::from_millis(5),
        };

        // always fails
        {
            let runs = AtomicU8::new(0);
            let f = || {
                runs.fetch_add(1, Ordering::Relaxed);
                futures::future::err::<i32, i32>(1)
            };

            let ret = retry_async(f, &config).await;
            assert!(ret.is_err());
            assert_eq!(4, runs.load(Ordering::Relaxed));
        }

        // succeed directly
        {
            let runs = AtomicU8::new(0);
            let f = || {
                runs.fetch_add(1, Ordering::Relaxed);
                futures::future::ok::<i32, i32>(1)
            };

            let ret = retry_async(f, &config).await;
            assert_eq!(1, ret.unwrap());
            assert_eq!(1, runs.load(Ordering::Relaxed));
        }

        // fail 2 times, then succeed
        {
            let runs = AtomicU8::new(0);
            let f = || {
                if runs.fetch_add(1, Ordering::Relaxed) < 2 {
                    return futures::future::err::<_, i32>(1);
                }

                futures::future::ok::<_, i32>(2)
            };

            let ret = retry_async(f, &config).await;
            assert_eq!(2, ret.unwrap());
            assert_eq!(3, runs.load(Ordering::Relaxed));
        }
    }
}
