// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

//! Timed background tasks.

use std::{future::Future, time::Duration};

use log::info;
use tokio::{
    sync::{
        mpsc::{self, UnboundedReceiver, UnboundedSender},
        Mutex,
    },
    time,
};

use crate::runtime::{JoinHandle, Runtime};

/// A task to run periodically.
pub struct TimedTask<B> {
    name: String,
    period: Duration,
    builder: B,
}

impl<B, Fut> TimedTask<B>
where
    B: Fn() -> Fut + Send + Sync + 'static,
    Fut: Future<Output = ()> + Send,
{
    pub fn start_timed_task(
        name: String,
        runtime: &Runtime,
        period: Duration,
        builder: B,
    ) -> TaskHandle {
        let (tx, rx) = mpsc::unbounded_channel();
        let task = TimedTask {
            name,
            period,
            builder,
        };

        let handle = runtime.spawn(async move {
            task.run(rx).await;
        });
        TaskHandle {
            handle: Mutex::new(Some(handle)),
            sender: tx,
        }
    }

    async fn run(&self, mut rx: UnboundedReceiver<()>) {
        info!("TimedTask started, name:{}", self.name);

        loop {
            // TODO(yingwen): Maybe add a random offset to the peroid.
            match time::timeout(self.period, rx.recv()).await {
                Ok(_) => {
                    info!("TimedTask stopped, name:{}", self.name);

                    return;
                }
                Err(_) => {
                    let future = (self.builder)();
                    future.await;
                }
            }
        }
    }
}

/// Handle to the timed task.
///
/// The task will exit asynchronously after this handle is dropped.
pub struct TaskHandle {
    handle: Mutex<Option<JoinHandle<()>>>,
    sender: UnboundedSender<()>,
}

impl TaskHandle {
    /// Explicit stop the task and wait util the task exits.
    pub async fn stop_task(&self) -> std::result::Result<(), crate::runtime::Error> {
        self.notify_exit();

        let handle = self.handle.lock().await.take();
        if let Some(h) = handle {
            h.await?;
        }

        Ok(())
    }

    fn notify_exit(&self) {
        if self.sender.send(()).is_err() {
            info!("The sender of task is disconnected");
        }
    }
}

impl Drop for TaskHandle {
    fn drop(&mut self) {
        self.notify_exit();
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    };

    use super::*;
    use crate::runtime::Builder;

    #[test]
    fn test_timed_task() {
        let period = Duration::from_millis(100);
        let runtime = Arc::new(
            Builder::default()
                .worker_threads(1)
                .enable_all()
                .build()
                .unwrap(),
        );
        let tick_count = Arc::new(AtomicUsize::new(0));
        let expect_ticks = 5;

        let rt = runtime.clone();
        rt.block_on(async {
            let tc = tick_count.clone();
            let timed_builder = move || {
                let count = tc.clone();
                async move {
                    count.fetch_add(1, Ordering::Relaxed);
                }
            };

            let name = "test-timed".to_string();
            let handle = TimedTask::start_timed_task(name, &runtime, period, timed_builder);

            // Sleep more times, ensure the builder is called enough times.
            time::sleep(period * (expect_ticks as u32 + 2)).await;

            handle.stop_task().await.unwrap();
        });

        assert!(tick_count.load(Ordering::Relaxed) > expect_ticks);
    }
}
