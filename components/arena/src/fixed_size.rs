// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

use std::{
    alloc::{alloc, dealloc, Layout},
    ptr::NonNull,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
};

use crate::arena_trait::{Arena, BasicStats};

const DEFAULT_ALIGN: usize = 8;

#[derive(Clone)]
pub struct FixedSizeArena {
    core: Arc<Core>,
}

impl FixedSizeArena {
    pub fn with_capacity(cap: usize) -> Self {
        Self {
            core: Arc::new(Core::with_capacity(cap)),
        }
    }
}

struct Core {
    len: AtomicUsize,
    cap: usize,
    ptr: NonNull<u8>,
}

impl Core {
    /// # Safety
    /// - alloc
    /// See [std::alloc::alloc].
    /// - new_unchecked
    /// `ptr` is allocated from allocator.
    fn with_capacity(cap: usize) -> Self {
        let layout = Layout::from_size_align(cap, DEFAULT_ALIGN).unwrap();
        let ptr = unsafe { alloc(layout) };

        Self {
            len: AtomicUsize::new(0),
            cap,
            ptr: unsafe { NonNull::new_unchecked(ptr) },
        }
    }

    /// # Safety
    /// `self.ptr` is allocated from allocator
    fn try_alloc(&self, layout: Layout) -> Option<NonNull<u8>> {
        let layout = layout.pad_to_align();
        let size = layout.size();

        let offset = self.len.fetch_add(size, Ordering::SeqCst);
        if offset + size > self.cap {
            self.len.fetch_sub(size, Ordering::SeqCst);
            return None;
        }

        unsafe { Some(NonNull::new_unchecked(self.ptr.as_ptr().add(size))) }
    }
}

impl Drop for Core {
    /// Reclaim space pointed by `data`.
    fn drop(&mut self) {
        unsafe {
            dealloc(
                self.ptr.as_ptr(),
                Layout::from_size_align_unchecked(self.cap, DEFAULT_ALIGN),
            )
        }
    }
}

impl Arena for FixedSizeArena {
    type Stats = BasicStats;

    fn try_alloc(&self, layout: Layout) -> Option<NonNull<u8>> {
        self.core.try_alloc(layout)
    }

    fn stats(&self) -> Self::Stats {
        Self::Stats {
            bytes_used: self.core.cap,
            bytes_allocated: self.core.len.load(Ordering::SeqCst),
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn capacity_overflow() {
        let arena = FixedSizeArena::with_capacity(1024);
        let layout = unsafe { Layout::from_size_align_unchecked(768, DEFAULT_ALIGN) };
        let _ = arena.alloc(layout);

        assert_eq!(None, arena.try_alloc(layout));
    }
}
