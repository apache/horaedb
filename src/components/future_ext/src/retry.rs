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

//! Util function to retry future.

use std::time::Duration;

use futures::Future;
use rand::prelude::*;

pub struct RetryConfig {
    pub max_retries: usize,
    pub backoff: BackoffConfig,
}

impl Default for RetryConfig {
    fn default() -> Self {
        Self {
            max_retries: 3,
            backoff: Default::default(),
        }
    }
}

pub struct BackoffConfig {
    /// The initial backoff duration
    pub init_backoff: Duration,
    /// The maximum backoff duration
    pub max_backoff: Duration,
    /// The base of the exponential to use
    pub base: f64,
}

impl Default for BackoffConfig {
    fn default() -> Self {
        Self {
            init_backoff: Duration::from_millis(100),
            max_backoff: Duration::from_millis(500),
            base: 3.,
        }
    }
}

pub struct Backoff {
    init_backoff: f64,
    next_backoff_secs: f64,
    max_backoff_secs: f64,
    base: f64,
    rng: Option<Box<dyn RngCore + Sync + Send>>,
}

impl Backoff {
    /// Create a new [`Backoff`] from the provided [`BackoffConfig`]
    pub fn new(config: &BackoffConfig) -> Self {
        Self::new_with_rng(config, None)
    }

    /// Creates a new `Backoff` with the optional `rng`
    ///
    /// Used [`rand::thread_rng()`] if no rng provided
    pub fn new_with_rng(
        config: &BackoffConfig,
        rng: Option<Box<dyn RngCore + Sync + Send>>,
    ) -> Self {
        let init_backoff = config.init_backoff.as_secs_f64();
        Self {
            init_backoff,
            next_backoff_secs: init_backoff,
            max_backoff_secs: config.max_backoff.as_secs_f64(),
            base: config.base,
            rng,
        }
    }

    /// Returns the next backoff duration to wait for
    pub fn next(&mut self) -> Duration {
        let range = self.init_backoff..(self.next_backoff_secs * self.base);

        let rand_backoff = match self.rng.as_mut() {
            Some(rng) => rng.gen_range(range),
            None => thread_rng().gen_range(range),
        };

        let next_backoff = self.max_backoff_secs.min(rand_backoff);
        Duration::from_secs_f64(std::mem::replace(&mut self.next_backoff_secs, next_backoff))
    }
}

pub async fn retry_async<F, Fut, T, E>(f: F, config: &RetryConfig) -> Fut::Output
where
    F: Fn() -> Fut,
    Fut: Future<Output = Result<T, E>>,
{
    let mut backoff = Backoff::new(&config.backoff);
    for _ in 0..config.max_retries {
        let result: Result<T, E> = f().await;

        if result.is_ok() {
            return result;
        }
        tokio::time::sleep(backoff.next()).await;
    }

    f().await
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicU8, Ordering};

    use rand::rngs::mock::StepRng;

    use super::*;

    #[tokio::test]
    async fn test_retry_async() {
        let config = RetryConfig {
            max_retries: 3,
            backoff: Default::default(),
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

    #[test]
    fn test_backoff() {
        let init_backoff_secs = 1.;
        let max_backoff_secs = 500.;
        let base = 3.;

        let config = BackoffConfig {
            init_backoff: Duration::from_secs_f64(init_backoff_secs),
            max_backoff: Duration::from_secs_f64(max_backoff_secs),
            base,
        };

        let assert_fuzzy_eq = |a: f64, b: f64| assert!((b - a).abs() < 0.0001, "{a} != {b}");

        // Create a static rng that takes the minimum of the range
        let rng = Box::new(StepRng::new(0, 0));
        let mut backoff = Backoff::new_with_rng(&config, Some(rng));

        for _ in 0..20 {
            assert_eq!(backoff.next().as_secs_f64(), init_backoff_secs);
        }

        // Create a static rng that takes the maximum of the range
        let rng = Box::new(StepRng::new(u64::MAX, 0));
        let mut backoff = Backoff::new_with_rng(&config, Some(rng));

        for i in 0..20 {
            let value = (base.powi(i) * init_backoff_secs).min(max_backoff_secs);
            assert_fuzzy_eq(backoff.next().as_secs_f64(), value);
        }

        // Create a static rng that takes the mid point of the range
        let rng = Box::new(StepRng::new(u64::MAX / 2, 0));
        let mut backoff = Backoff::new_with_rng(&config, Some(rng));

        let mut value = init_backoff_secs;
        for _ in 0..20 {
            assert_fuzzy_eq(backoff.next().as_secs_f64(), value);
            value =
                (init_backoff_secs + (value * base - init_backoff_secs) / 2.).min(max_backoff_secs);
        }
    }
}
