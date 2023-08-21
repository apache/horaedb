// Copyright 2023 The CeresDB Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Alloc tracker

use std::sync::atomic::{AtomicUsize, Ordering};

/// Collect memory usage from tracker, useful for extending the tracker
pub trait Collector {
    /// Called when `bytes` bytes memory is allocated and tracked by the tracker
    fn on_allocate(&self, bytes: usize);

    /// Called when `bytes` bytes memory is freed and tracked by the tracker
    fn on_free(&self, bytes: usize);
}

/// A tracker to track memory in used
// TODO(yingwen): Impl a thread local or local tracker that are not thread safe,
// and collect statistics into the thread safe one for better performance
pub struct Tracker<T: Collector> {
    collector: T,
    bytes_allocated: AtomicUsize,
}

impl<T: Collector> Tracker<T> {
    pub fn new(collector: T) -> Self {
        Self {
            collector,
            bytes_allocated: AtomicUsize::new(0),
        }
    }

    /// Increase consumption of this tracker by bytes
    pub fn consume(&self, bytes: usize) {
        self.bytes_allocated.fetch_add(bytes, Ordering::Relaxed);
        self.collector.on_allocate(bytes);
    }

    /// Decrease consumption of this tracker by bytes
    ///
    /// The caller should guarantee the released bytes wont larger than bytes
    /// already consumed
    pub fn release(&self, bytes: usize) {
        self.bytes_allocated.fetch_sub(bytes, Ordering::Relaxed);
        self.collector.on_free(bytes);
    }

    /// Bytes allocated
    pub fn bytes_allocated(&self) -> usize {
        self.bytes_allocated.load(Ordering::Relaxed)
    }
}

impl<T: Collector> Drop for Tracker<T> {
    fn drop(&mut self) {
        let bytes = *self.bytes_allocated.get_mut();
        self.collector.on_free(bytes);
    }
}

/// The noop collector does nothing on alloc and free
struct NoopCollector;

impl Collector for NoopCollector {
    fn on_allocate(&self, _bytes: usize) {}

    fn on_free(&self, _bytes: usize) {}
}

/// A simple tracker hides the collector api
pub struct SimpleTracker(Tracker<NoopCollector>);

impl Default for SimpleTracker {
    fn default() -> Self {
        Self(Tracker::new(NoopCollector))
    }
}

impl SimpleTracker {
    /// Increase consumption of this tracker by bytes
    #[inline]
    pub fn consume(&self, bytes: usize) {
        self.0.consume(bytes);
    }

    /// Decrease consumption of this tracker by bytes
    ///
    /// The caller should guarantee the released bytes wont larger than bytes
    /// already consumed
    #[inline]
    pub fn release(&self, bytes: usize) {
        self.0.release(bytes);
    }

    /// Bytes allocated
    pub fn bytes_allocated(&self) -> usize {
        self.0.bytes_allocated()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_simple_tracker() {
        let tracker = SimpleTracker::default();
        tracker.consume(256);
        assert_eq!(256, tracker.bytes_allocated());

        tracker.release(100);
        assert_eq!(156, tracker.bytes_allocated());
    }

    #[test]
    fn test_collector() {
        use std::sync::atomic::AtomicBool;

        struct MockCollector {
            allocated: AtomicBool,
            freed: AtomicBool,
        }

        impl MockCollector {
            fn new() -> Self {
                Self {
                    allocated: AtomicBool::new(false),
                    freed: AtomicBool::new(false),
                }
            }
        }

        impl Drop for MockCollector {
            fn drop(&mut self) {
                assert!(*self.allocated.get_mut());
                assert!(*self.freed.get_mut());
            }
        }

        impl Collector for MockCollector {
            fn on_allocate(&self, bytes: usize) {
                assert_eq!(800, bytes);
                self.allocated.store(true, Ordering::Relaxed);
            }

            fn on_free(&self, bytes: usize) {
                if self.freed.load(Ordering::Relaxed) {
                    assert_eq!(440, bytes);
                } else {
                    assert_eq!(360, bytes);
                }
                self.freed.store(true, Ordering::Relaxed);
            }
        }

        let tracker = Tracker::new(MockCollector::new());
        tracker.consume(800);
        tracker.release(360);
    }
}
