// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

use std::{
    alloc::{alloc, dealloc, Layout},
    ptr::NonNull,
    sync::Arc,
};

use parking_lot::Mutex;

use crate::arena_trait::{Arena, BasicStats, Collector, CollectorRef};

/// The noop collector does nothing on alloc and free
pub struct NoopCollector;

impl Collector for NoopCollector {
    fn on_alloc(&self, _bytes: usize) {}

    fn on_used(&self, _bytes: usize) {}

    fn on_free(&self, _used: usize, _allocated: usize) {}
}

const DEFAULT_ALIGN: usize = 8;

/// A thread-safe arena. All allocated memory is aligned to 8. Organizes its
/// allocated memory as blocks.
#[derive(Clone)]
pub struct MonoIncArena {
    core: Arc<Mutex<ArenaCore>>,
}

impl MonoIncArena {
    pub fn new(regular_block_size: usize) -> Self {
        Self {
            core: Arc::new(Mutex::new(ArenaCore::new(
                regular_block_size,
                Arc::new(NoopCollector {}),
            ))),
        }
    }

    pub fn with_collector(regular_block_size: usize, collector: CollectorRef) -> Self {
        Self {
            core: Arc::new(Mutex::new(ArenaCore::new(regular_block_size, collector))),
        }
    }
}

impl Arena for MonoIncArena {
    type Stats = BasicStats;

    fn try_alloc(&self, layout: Layout) -> Option<NonNull<u8>> {
        Some(self.core.lock().alloc(layout))
    }

    fn stats(&self) -> Self::Stats {
        self.core.lock().stats
    }

    fn alloc(&self, layout: Layout) -> NonNull<u8> {
        self.core.lock().alloc(layout)
    }
}

struct ArenaCore {
    collector: CollectorRef,
    regular_layout: Layout,
    regular_blocks: Vec<Block>,
    special_blocks: Vec<Block>,
    stats: BasicStats,
}

impl ArenaCore {
    /// # Safety
    /// Required property is tested in debug assertions.
    fn new(regular_block_size: usize, collector: CollectorRef) -> Self {
        debug_assert_ne!(DEFAULT_ALIGN, 0);
        debug_assert_eq!(DEFAULT_ALIGN & (DEFAULT_ALIGN - 1), 0);
        // TODO(yingwen): Avoid panic.
        let regular_layout = Layout::from_size_align(regular_block_size, DEFAULT_ALIGN).unwrap();
        let regular_blocks = vec![Block::new(regular_layout)];
        let special_blocks = vec![];
        let bytes = regular_layout.size();
        collector.on_alloc(bytes);

        Self {
            collector,
            regular_layout,
            regular_blocks,
            special_blocks,
            stats: BasicStats {
                bytes_allocated: bytes,
                bytes_used: 0,
            },
        }
    }

    /// Input layout will be aligned.
    fn alloc(&mut self, layout: Layout) -> NonNull<u8> {
        let layout = layout
            .align_to(self.regular_layout.align())
            .unwrap()
            .pad_to_align();
        let bytes = layout.size();
        // TODO(Ruihang): determine threshold
        if layout.size() > self.regular_layout.size() {
            self.stats.bytes_used += bytes;
            self.collector.on_used(bytes);
            Self::add_new_block(
                layout,
                &mut self.special_blocks,
                &mut self.stats,
                &self.collector,
            );
            let block = self.special_blocks.last().unwrap();
            return block.data;
        }

        self.stats.bytes_used += bytes;
        self.collector.on_used(bytes);
        if let Some(ptr) = self.try_alloc(layout) {
            ptr
        } else {
            Self::add_new_block(
                self.regular_layout,
                &mut self.regular_blocks,
                &mut self.stats,
                &self.collector,
            );
            self.try_alloc(layout).unwrap()
        }
    }

    /// # Safety
    /// `regular_blocks` vector is guaranteed to contains at least one element.
    fn try_alloc(&mut self, layout: Layout) -> Option<NonNull<u8>> {
        self.regular_blocks.last_mut().unwrap().alloc(layout)
    }

    fn add_new_block(
        layout: Layout,
        container: &mut Vec<Block>,
        stats: &mut BasicStats,
        collector: &CollectorRef,
    ) {
        let new_block = Block::new(layout);
        container.push(new_block);
        // Update allocated stats once a new block has been allocated from the system.
        stats.bytes_allocated += layout.size();
        collector.on_alloc(layout.size());
    }
}

impl Drop for ArenaCore {
    fn drop(&mut self) {
        self.collector
            .on_free(self.stats.bytes_used, self.stats.bytes_allocated);
    }
}

struct Block {
    data: NonNull<u8>,
    len: usize,
    layout: Layout,
}

impl Block {
    /// Create a new block. Return the pointer of this new block.
    ///
    /// # Safety
    /// See [std::alloc::alloc]. The allocated memory will be deallocated in
    /// drop().
    fn new(layout: Layout) -> Block {
        let data = unsafe { alloc(layout) };

        Self {
            data: NonNull::new(data).unwrap(),
            len: 0,
            layout,
        }
    }

    /// # Safety
    /// ## ptr:add()
    /// The added offset is checked before.
    /// ## NonNull::new_unchecked()
    /// `ptr` is added from a NonNull.
    fn alloc(&mut self, layout: Layout) -> Option<NonNull<u8>> {
        let size = layout.size();

        if self.len + size <= self.layout.size() {
            let ptr = unsafe { self.data.as_ptr().add(self.len) };
            self.len += size;
            unsafe { Some(NonNull::new_unchecked(ptr)) }
        } else {
            None
        }
    }
}

impl Drop for Block {
    /// Reclaim space pointed by `data`.
    fn drop(&mut self) {
        unsafe { dealloc(self.data.as_ptr(), self.layout) }
    }
}

unsafe impl Send for Block {}
unsafe impl Sync for Block {}

#[cfg(test)]
mod test {
    use std::{
        mem,
        sync::{
            atomic::{AtomicUsize, Ordering},
            Arc,
        },
    };

    use super::*;

    /// # Safety:
    /// Caller should check the input buf has enough space.
    fn consume_buf_as_u64_slice(buf: NonNull<u8>, n: usize) {
        unsafe {
            let mut buf = buf.as_ptr() as *mut u64;
            for i in 0..n {
                *buf = i as u64;
                buf = buf.add(1);
            }
        }
    }

    #[test]
    fn test_stats() {
        let arena = MonoIncArena::new(1024 * 1024);

        // Size is 80
        let layout_slice = Layout::new::<[u64; 10]>().align_to(8).unwrap();
        for _ in 0..20 {
            arena.alloc(layout_slice);
        }

        assert_eq!(1024 * 1024, arena.stats().bytes_allocated());
        assert_eq!(1600, arena.stats().bytes_used());
    }

    struct MockCollector {
        allocated: AtomicUsize,
        used: AtomicUsize,
    }

    impl Collector for MockCollector {
        fn on_alloc(&self, bytes: usize) {
            self.allocated.fetch_add(bytes, Ordering::Relaxed);
        }

        fn on_used(&self, bytes: usize) {
            self.used.fetch_add(bytes, Ordering::Relaxed);
        }

        fn on_free(&self, _used: usize, _allocated: usize) {}
    }

    #[test]
    fn test_collector() {
        let collector = Arc::new(MockCollector {
            allocated: AtomicUsize::new(0),
            used: AtomicUsize::new(0),
        });

        let arena = MonoIncArena::with_collector(1024 * 1024, collector.clone());

        // Size is 80
        let layout_slice = Layout::new::<[u64; 10]>().align_to(8).unwrap();
        for _ in 0..20 {
            arena.alloc(layout_slice);
        }

        assert_eq!(1024 * 1024, collector.allocated.load(Ordering::Relaxed));
        assert_eq!(1600, collector.used.load(Ordering::Relaxed));
    }

    #[test]
    fn alloc_small_slice() {
        let arena = MonoIncArena::new(128);

        let layout_slice = Layout::new::<[u64; 10]>().align_to(8).unwrap();
        for _ in 0..20 {
            let buf = arena.alloc(layout_slice);
            consume_buf_as_u64_slice(buf, 10);
        }

        assert_eq!(2560, arena.stats().bytes_allocated());
        assert_eq!(1600, arena.stats().bytes_used());
    }

    #[test]
    fn alloc_huge_slice() {
        let arena = MonoIncArena::new(128);

        let layout_slice = Layout::new::<[u64; 20]>().align_to(8).unwrap();
        for _ in 0..20 {
            let buf = arena.alloc(layout_slice);
            consume_buf_as_u64_slice(buf, 20);
        }

        assert_eq!(3328, arena.stats().bytes_allocated());
        assert_eq!(3200, arena.stats().bytes_used());
    }

    #[test]
    fn alloc_various_slice() {
        let arena = MonoIncArena::new(1024);
        const SIZES: [usize; 12] = [10, 200, 30, 1024, 512, 77, 89, 1, 3, 29, 16, 480];
        let total_used: usize = SIZES.iter().map(|v| v * 8).sum();

        for size in &SIZES {
            let layout_slice = Layout::from_size_align(mem::size_of::<u64>() * *size, 8).unwrap();
            let buf = arena.alloc(layout_slice);
            consume_buf_as_u64_slice(buf, *size);
        }

        assert_eq!(20800, arena.stats().bytes_allocated());
        assert_eq!(total_used, arena.stats().bytes_used());
    }

    #[test]
    fn unaligned_alloc_request() {
        let arena = MonoIncArena::new(1024);

        let regular_req_layout = Layout::from_size_align(mem::size_of::<u64>(), 2).unwrap();
        for _ in 0..10 {
            let buf = arena.alloc(regular_req_layout).as_ptr() as usize;
            assert_eq!(0, buf % DEFAULT_ALIGN);
        }

        // 2003 is a prime number and 2004 % 8 != 0
        let special_req_layout = Layout::from_size_align(2003, 2).unwrap();
        for _ in 0..10 {
            let buf = arena.alloc(special_req_layout).as_ptr() as usize;
            assert_eq!(0, buf % DEFAULT_ALIGN);
        }
    }
}
