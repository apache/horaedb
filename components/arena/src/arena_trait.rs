// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

use std::{alloc::Layout, ptr::NonNull, sync::Arc};

/// Memory Arena trait.
///
/// The trait itself provides and enforces no guarantee about alignment. It's
/// implementation's responsibility to cover.
///
/// All memory-relavent methods (`alloc()` etc.) are not "unsafe". Compare with
/// "deallocate" which is not included in this trait, allocating is more safer
/// and not likely to run into UB. However in fact, playing with raw pointer is
/// always dangerous and needs to be careful for both who implements and uses
/// this trait.
pub trait Arena {
    type Stats;

    // required methods

    /// Try to allocate required memory described by layout. Return a pointer of
    /// allocated space in success, while `None` if failed.
    fn try_alloc(&self, layout: Layout) -> Option<NonNull<u8>>;

    /// Get arena's statistics.
    fn stats(&self) -> Self::Stats;

    // provided methods

    /// Allocate required memory. Panic if failed.
    fn alloc(&self, layout: Layout) -> NonNull<u8> {
        self.try_alloc(layout).unwrap()
    }
}

/// Basic statistics of arena. Offers [bytes_allocated]
/// and [bytes_used].
#[derive(Debug, Clone, Copy)]
pub struct BasicStats {
    pub(crate) bytes_allocated: usize,
    pub(crate) bytes_used: usize,
}

impl BasicStats {
    /// Total bytes allocated from system.
    #[inline]
    pub fn bytes_allocated(&self) -> usize {
        self.bytes_allocated
    }

    /// Total bytes allocated to user.
    #[inline]
    pub fn bytes_used(&self) -> usize {
        self.bytes_used
    }
}

/// Collect memory usage from Arean
pub trait Collector {
    /// Called when `bytes` bytes memory is allocated in arena.
    fn on_alloc(&self, bytes: usize);

    /// Called when `bytes` bytes memory is used in arena.
    fn on_used(&self, bytes: usize);

    /// Called when `allocated` bytes memory is released, and `used` bytes in
    /// it.
    fn on_free(&self, used: usize, allocated: usize);
}

pub type CollectorRef = Arc<dyn Collector + Send + Sync>;
