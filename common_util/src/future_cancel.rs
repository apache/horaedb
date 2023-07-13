// Copyright 2022-2023 CeresDB Project Authors. Licensed under Apache-2.0.

/// A guard to detect whether a future is cancelled.
pub struct FutureCancelGuard<F: FnMut()> {
    cancelled: bool,
    on_cancel: F,
}

impl<F: FnMut()> FutureCancelGuard<F> {
    /// Create a guard to assume the future will be cancelled at the following
    /// await point.
    ///
    /// If the future is really cancelled, the provided `on_cancel` callback
    /// will be executed.
    pub fn new_cancelled(on_cancel: F) -> Self {
        Self {
            cancelled: true,
            on_cancel,
        }
    }

    /// Set the inner state is uncancelled to ensure the `on_cancel` callback
    /// won't be executed.
    pub fn uncancel(&mut self) {
        self.cancelled = false;
    }
}

impl<F: FnMut()> Drop for FutureCancelGuard<F> {
    fn drop(&mut self) {
        if self.cancelled {
            (self.on_cancel)();
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{
        sync::{Arc, Mutex},
        time::Duration,
    };

    use super::*;

    #[derive(Debug, PartialEq, Eq)]
    enum State {
        Init,
        Processing,
        Finished,
        Cancelled,
    }

    #[tokio::test]
    async fn test_future_cancel() {
        let state = Arc::new(Mutex::new(State::Init));
        let lock = Arc::new(tokio::sync::Mutex::new(()));

        // Hold the lock before the task is spawned.
        let lock_guard = lock.lock().await;

        let cloned_lock = lock.clone();
        let cloned_state = state.clone();
        let handle = tokio::spawn(async move {
            {
                let mut state = cloned_state.lock().unwrap();
                *state = State::Processing;
            }
            let mut cancel_guard = FutureCancelGuard::new_cancelled(|| {
                let mut state = cloned_state.lock().unwrap();
                *state = State::Cancelled;
            });

            // It will be cancelled at this await point.
            let _lock_guard = cloned_lock.lock().await;
            cancel_guard.uncancel();
            let mut state = cloned_state.lock().unwrap();
            *state = State::Finished;
        });

        // Ensure the spawned task is started.
        tokio::time::sleep(Duration::from_millis(50)).await;
        handle.abort();
        // Ensure the future cancel guard is dropped.
        tokio::time::sleep(Duration::from_millis(0)).await;
        drop(lock_guard);

        let state = state.lock().unwrap();
        assert_eq!(*state, State::Cancelled);
    }
}
