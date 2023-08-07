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

//! Slice with arena

use std::{fmt, ops::Deref, slice};

use arena::{Arena, BasicStats};

/// Arena slice
///
/// A slice allocated from the arena, it will holds the reference to the arena
/// so it is safe to clone and deref the slice
#[derive(Clone)]
pub struct ArenaSlice<A: Arena<Stats = BasicStats>> {
    /// Arena the slice memory allocated from.
    _arena: A,
    /// The slice pointer.
    slice_ptr: *const u8,
    /// The slice len.
    slice_len: usize,
}

impl<A: Arena<Stats = BasicStats>> ArenaSlice<A> {
    /// Create a [ArenaSlice]
    ///
    /// See the documentation of [`slice::from_raw_parts`] for slice safety
    /// requirements.
    pub(crate) unsafe fn from_raw_parts(_arena: A, slice_ptr: *const u8, slice_len: usize) -> Self {
        Self {
            _arena,
            slice_ptr,
            slice_len,
        }
    }
}

unsafe impl<A: Arena<Stats = BasicStats> + Send> Send for ArenaSlice<A> {}
unsafe impl<A: Arena<Stats = BasicStats> + Sync> Sync for ArenaSlice<A> {}

impl<A: Arena<Stats = BasicStats>> Deref for ArenaSlice<A> {
    type Target = [u8];

    fn deref(&self) -> &[u8] {
        unsafe { slice::from_raw_parts(self.slice_ptr, self.slice_len) }
    }
}

impl<A: Arena<Stats = BasicStats>> fmt::Debug for ArenaSlice<A> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_list().entries(self.iter()).finish()
    }
}

#[cfg(test)]
mod tests {
    use std::{alloc::Layout, mem, ptr};

    use arena::MonoIncArena;

    use super::*;

    #[test]
    fn test_arena_slice() {
        let hello = b"hello";
        let arena = MonoIncArena::new(1 << 10);
        let slice = unsafe {
            let data_ptr = arena
                .alloc(Layout::from_size_align(hello.len(), mem::align_of_val(hello)).unwrap());
            ptr::copy_nonoverlapping(hello.as_ptr(), data_ptr.as_ptr(), hello.len());
            ArenaSlice::from_raw_parts(arena, data_ptr.as_ptr(), hello.len())
        };
        assert_eq!(hello, &slice[..]);
    }
}
