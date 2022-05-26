// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

use std::sync::atomic::{AtomicUsize, Ordering};

use arena::{Collector, CollectorRef};

/// Space memtable memory usage collector
pub struct MemUsageCollector {
    /// Memory size allocated in bytes.
    bytes_allocated: AtomicUsize,
    /// Memory size used in bytes.
    bytes_used: AtomicUsize,
    parent: Option<CollectorRef>,
}

impl Collector for MemUsageCollector {
    fn on_alloc(&self, bytes: usize) {
        self.bytes_allocated.fetch_add(bytes, Ordering::Relaxed);

        if let Some(c) = &self.parent {
            c.on_alloc(bytes);
        }
    }

    fn on_used(&self, bytes: usize) {
        self.bytes_used.fetch_add(bytes, Ordering::Relaxed);

        if let Some(c) = &self.parent {
            c.on_used(bytes);
        }
    }

    fn on_free(&self, used: usize, allocated: usize) {
        self.bytes_allocated.fetch_sub(allocated, Ordering::Relaxed);
        self.bytes_used.fetch_sub(used, Ordering::Relaxed);

        if let Some(c) = &self.parent {
            c.on_free(used, allocated);
        }
    }
}

impl Default for MemUsageCollector {
    fn default() -> Self {
        Self {
            bytes_allocated: AtomicUsize::new(0),
            bytes_used: AtomicUsize::new(0),
            parent: None,
        }
    }
}

impl MemUsageCollector {
    pub fn with_parent(collector: CollectorRef) -> Self {
        Self {
            bytes_allocated: AtomicUsize::new(0),
            bytes_used: AtomicUsize::new(0),
            parent: Some(collector),
        }
    }

    #[inline]
    pub fn total_memory_allocated(&self) -> usize {
        self.bytes_allocated.load(Ordering::Relaxed)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{atomic::Ordering, Arc};

    use super::*;
    #[test]
    fn test_collector() {
        let collector = MemUsageCollector::default();

        collector.on_alloc(1024);
        collector.on_used(128);
        assert_eq!(1024, collector.total_memory_allocated());
        assert_eq!(128, collector.bytes_used.load(Ordering::Relaxed));

        collector.on_free(64, 512);
        assert_eq!(512, collector.total_memory_allocated());
        assert_eq!(64, collector.bytes_used.load(Ordering::Relaxed));
        collector.on_free(64, 512);
        assert_eq!(0, collector.total_memory_allocated());
        assert_eq!(0, collector.bytes_used.load(Ordering::Relaxed));
    }

    #[test]
    fn test_collector_with_parent() {
        let p = Arc::new(MemUsageCollector::default());
        let c1 = MemUsageCollector::with_parent(p.clone());
        let c2 = MemUsageCollector::with_parent(p.clone());

        c1.on_alloc(1024);
        c1.on_used(128);
        c2.on_alloc(1024);
        c2.on_used(128);
        assert_eq!(1024, c1.total_memory_allocated());
        assert_eq!(128, c1.bytes_used.load(Ordering::Relaxed));
        assert_eq!(1024, c2.total_memory_allocated());
        assert_eq!(128, c2.bytes_used.load(Ordering::Relaxed));
        assert_eq!(2048, p.total_memory_allocated());
        assert_eq!(256, p.bytes_used.load(Ordering::Relaxed));

        c1.on_free(64, 512);
        assert_eq!(512, c1.total_memory_allocated());
        assert_eq!(64, c1.bytes_used.load(Ordering::Relaxed));
        assert_eq!(1536, p.total_memory_allocated());
        assert_eq!(192, p.bytes_used.load(Ordering::Relaxed));
        c2.on_free(64, 512);
        assert_eq!(512, c2.total_memory_allocated());
        assert_eq!(64, c2.bytes_used.load(Ordering::Relaxed));
        assert_eq!(1024, p.total_memory_allocated());
        assert_eq!(128, p.bytes_used.load(Ordering::Relaxed));
    }
}
