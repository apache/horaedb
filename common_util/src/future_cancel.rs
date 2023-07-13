// Copyright 2022-2023 CeresDB Project Authors. Licensed under Apache-2.0.

//! A future wrapper to ensure the wrapped future must be polled.
//!
//! This implementation is forked from: https://github.com/influxdata/influxdb_iox/blob/885767aa0a6010de592bde9992945b01389eb994/cache_system/src/cancellation_safe_future.rs
//! Here is the copyright and license disclaimer:
//! Copyright (c) 2020 InfluxData. Licensed under Apache-2.0.

use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

use futures::future::BoxFuture;

use crate::runtime::RuntimeRef;

/// Wrapper around a future that cannot be cancelled.
///
/// When the future is dropped/cancelled, we'll spawn a tokio task to _rescue_
/// it.
pub struct CancellationSafeFuture<F>
where
    F: Future + Send + 'static,
    F::Output: Send,
{
    /// Mark if the inner future finished. If not, we must spawn a helper task
    /// on drop.
    done: bool,

    /// Inner future.
    ///
    /// Wrapped in an `Option` so we can extract it during drop. Inside that
    /// option however we also need a pinned box because once this wrapper
    /// is polled, it will be pinned in memory -- even during drop. Now the
    /// inner future does not necessarily implement `Unpin`, so we need a
    /// heap allocation to pin it in memory even when we move it out of this
    /// option.
    inner: Option<BoxFuture<'static, F::Output>>,

    /// The runtime to execute the dropped future.
    runtime: RuntimeRef,
}

impl<F> Drop for CancellationSafeFuture<F>
where
    F: Future + Send + 'static,
    F::Output: Send,
{
    fn drop(&mut self) {
        if !self.done {
            let inner = self.inner.take().unwrap();
            let handle = self.runtime.spawn(async move { inner.await });
            drop(handle);
        }
    }
}

impl<F> CancellationSafeFuture<F>
where
    F: Future + Send,
    F::Output: Send,
{
    /// Create new future that is protected from cancellation.
    ///
    /// If [`CancellationSafeFuture`] is cancelled (i.e. dropped) and there is
    /// still some external receiver of the state left, than we will drive
    /// the payload (`f`) to completion. Otherwise `f` will be cancelled.
    pub fn new(fut: F, runtime: RuntimeRef) -> Self {
        Self {
            done: false,
            inner: Some(Box::pin(fut)),
            runtime,
        }
    }
}

impl<F> Future for CancellationSafeFuture<F>
where
    F: Future + Send,
    F::Output: Send,
{
    type Output = F::Output;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        assert!(!self.done, "Polling future that already returned");

        match self.inner.as_mut().unwrap().as_mut().poll(cx) {
            Poll::Ready(res) => {
                self.done = true;
                Poll::Ready(res)
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{
        sync::{
            atomic::{AtomicBool, Ordering},
            Arc,
        },
        time::Duration,
    };

    use tokio::sync::Barrier;

    use super::*;
    use crate::runtime::Builder;

    fn rt() -> RuntimeRef {
        let rt = Builder::default()
            .worker_threads(2)
            .thread_name("test_spawn_join")
            .enable_all()
            .build();
        assert!(rt.is_ok());
        Arc::new(rt.unwrap())
    }

    #[test]
    fn test_happy_path() {
        let runtime = rt();
        let runtime_clone = runtime.clone();
        runtime.block_on(async move {
            let done = Arc::new(AtomicBool::new(false));
            let done_captured = Arc::clone(&done);

            let fut = CancellationSafeFuture::new(
                async move {
                    done_captured.store(true, Ordering::SeqCst);
                },
                runtime_clone,
            );

            fut.await;

            assert!(done.load(Ordering::SeqCst));
        })
    }

    #[test]
    fn test_cancel_future() {
        let runtime = rt();
        let runtime_clone = runtime.clone();

        runtime.block_on(async move {
            let done = Arc::new(Barrier::new(2));
            let done_captured = Arc::clone(&done);

            let fut = CancellationSafeFuture::new(
                async move {
                    done_captured.wait().await;
                },
                runtime_clone,
            );

            drop(fut);

            tokio::time::timeout(Duration::from_secs(5), done.wait())
                .await
                .unwrap();
        });
    }
}
