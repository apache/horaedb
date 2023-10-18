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

use std::{
    alloc::Layout,
    convert::TryInto,
    mem, ptr,
    ptr::NonNull,
    slice,
    sync::{
        atomic::{AtomicPtr, AtomicUsize, Ordering},
        Arc,
    },
};

use arena::{Arena, BasicStats};
use rand::Rng;

use crate::{slice::ArenaSlice, KeyComparator, MAX_HEIGHT};

const HEIGHT_INCREASE: u32 = u32::MAX / 3;

type KeySize = u16;
type ValueSize = u32;

pub const MAX_KEY_SIZE: u16 = u16::MAX;

/// The layout of Node
/// 1. height: usize
/// 2. tower: AtomicPtr<Node> x (height + 1)
/// 3. key_size: KeySize
/// 4. key: u8 x key_size
/// 5. value_size: ValueSize
/// 6. value: ValueSize
// Uses C layout to make sure tower is at the bottom
#[derive(Debug)]
#[repr(C)]
pub struct Node {
    /// Height of node, different from badger, The valid range of tower is [0,
    /// height]
    height: usize,
    /// The node tower
    ///
    /// Only [0, height] parts is utilized to store node pointer, the key and
    /// value block are start from tower[height + 1]
    tower: [AtomicPtr<Node>; MAX_HEIGHT],
}

impl Node {
    /// Allocate a new node from the arena, and copy the content of key/value
    /// into the node
    /// # Safety
    /// - from_size_align_unchecked: align is got from [mem::align_of].
    /// # Notice
    /// This will only allocate the *exact* amount of memory needed within the
    /// given height.
    fn alloc<A>(arena: &A, key: &[u8], value: &[u8], height: usize) -> *mut Node
    where
        A: Arena<Stats = BasicStats>,
    {
        // Calculate node size to alloc
        let size = mem::size_of::<Node>();
        // Not all values in Node::tower will be utilized.
        let not_used = (MAX_HEIGHT - height - 1) * mem::size_of::<AtomicPtr<Node>>();
        // Space to store key/value: (key size) + key + (value size) + value
        let kv_used =
            mem::size_of::<KeySize>() + key.len() + mem::size_of::<ValueSize>() + value.len();
        // UB in fact: the `not_used` size is able to be access in a "safe" way.
        // It is guaranteed by the user to not use those memory.
        let alloc_size = size - not_used + kv_used;
        let layout =
            unsafe { Layout::from_size_align_unchecked(alloc_size, mem::align_of::<Node>()) };
        let node_ptr = arena.alloc(layout).as_ptr() as *mut Node;
        unsafe {
            let node = &mut *node_ptr;
            node.height = height;
            ptr::write_bytes(node.tower.as_mut_ptr(), 0, height + 1);
            Self::init_key_value(node, key, value);

            node_ptr
        }
    }

    /// Fetch next node ptr in given height
    fn next_ptr(&self, height: usize) -> *mut Node {
        self.tower[height].load(Ordering::SeqCst)
    }

    /// Get key
    ///
    /// REQUIRE: This Node is created via `Node::alloc()`
    unsafe fn key(&self) -> &[u8] {
        let (key_block, key_size) = self.load_key_size();

        slice::from_raw_parts(key_block, key_size as usize)
    }

    /// Get value
    ///
    /// REQUIRE: This Node is created via `Node::alloc()`
    unsafe fn value(&self) -> &[u8] {
        let (key_block, key_size) = self.load_key_size();
        let (value_block, value_size) = self.load_value_size(key_block, key_size);

        slice::from_raw_parts(value_block, value_size as usize)
    }

    /// Set key and value parts of Node during creating Node
    ///
    /// Will copy the content of key and value to the Node
    ///
    /// REQUIRE: This Node is created via Arena and node.tower and node.height
    /// is already set to correct value
    /// Panic: The size of key/value must less than max value of
    /// KeySize/ValueSize (u16/u32), otherwise this function will panic
    unsafe fn init_key_value(node: &mut Node, key: &[u8], value: &[u8]) {
        let key_block = node.tower.as_mut_ptr().add(node.height + 1) as *mut u8;
        let key_size: KeySize = key.len().try_into().unwrap();
        let key_size_bytes = key_size.to_ne_bytes();

        ptr::copy_nonoverlapping(
            key_size_bytes.as_ptr(),
            key_block,
            mem::size_of::<KeySize>(),
        );
        let key_block = key_block.add(mem::size_of::<KeySize>());
        ptr::copy_nonoverlapping(key.as_ptr(), key_block, key.len());

        let value_block = key_block.add(key.len());
        let value_size: ValueSize = value.len().try_into().unwrap();
        let value_size_bytes = value_size.to_ne_bytes();

        ptr::copy_nonoverlapping(
            value_size_bytes.as_ptr(),
            value_block,
            mem::size_of::<ValueSize>(),
        );
        let value_block = value_block.add(mem::size_of::<ValueSize>());
        ptr::copy_nonoverlapping(value.as_ptr(), value_block, value.len());
    }

    /// Load key pointer and size of key
    ///
    /// REQUIRE: This Node is created via `Node::alloc()`
    unsafe fn load_key_size(&self) -> (*const u8, KeySize) {
        let tower = self.tower.as_ptr();
        // Move to key block
        let key_block = tower.add(self.height + 1) as *const u8;
        // Load key size from key block
        let key_size = u16::from_ne_bytes(*(key_block as *const [u8; mem::size_of::<KeySize>()]));
        // Move key block to the start of key
        let key_block = key_block.add(mem::size_of::<KeySize>());

        (key_block, key_size)
    }

    /// Load value pointer and size of value
    ///
    /// Given key_block and key_size returned from `load_key_size()`, loads
    /// value pointer and value size
    ///
    /// REQUIRE: This Node is created via `Node::alloc()`
    unsafe fn load_value_size(
        &self,
        key_block: *const u8,
        key_size: KeySize,
    ) -> (*const u8, ValueSize) {
        // Move to value block
        let value_block = key_block.add(key_size as usize);
        // Load value size from value block
        let value_size =
            u32::from_ne_bytes(*(value_block as *const [u8; mem::size_of::<ValueSize>()]));
        // Move value block to the start of value
        let value_block = value_block.add(mem::size_of::<ValueSize>());

        (value_block, value_size)
    }

    /// Get key with arena
    ///
    /// REQUIRE: This Node is created via `Node::alloc()`
    unsafe fn key_with_arena<A>(&self, arena: A) -> ArenaSlice<A>
    where
        A: Arena<Stats = BasicStats>,
    {
        let (key_block, key_size) = self.load_key_size();

        ArenaSlice::from_raw_parts(arena, key_block, key_size as usize)
    }

    /// Get value with arena
    ///
    /// REQUIRE: This Node is created via `Node::alloc()`
    unsafe fn value_with_arena<A>(&self, arena: A) -> ArenaSlice<A>
    where
        A: Arena<Stats = BasicStats>,
    {
        let (key_block, key_size) = self.load_key_size();
        let (value_block, value_size) = self.load_value_size(key_block, key_size);

        ArenaSlice::from_raw_parts(arena, value_block, value_size as usize)
    }
}

struct SkiplistCore<A> {
    height: AtomicUsize,
    head: NonNull<Node>,
    arena: A,
}

/// FIXME(yingwen): Modify the skiplist to support arena that supports growth,
/// otherwise it is hard to avoid memory usage not out of the arena capacity
#[derive(Clone)]
pub struct Skiplist<C, A> {
    core: Arc<SkiplistCore<A>>,
    c: C,
}

impl<C, A: Arena<Stats = BasicStats> + Clone> Skiplist<C, A> {
    pub fn with_arena(c: C, arena: A) -> Skiplist<C, A> {
        let head = Node::alloc(&arena, &[], &[], MAX_HEIGHT - 1);
        let head = unsafe { NonNull::new_unchecked(head) };
        Skiplist {
            core: Arc::new(SkiplistCore {
                height: AtomicUsize::new(0),
                head,
                arena,
            }),
            c,
        }
    }

    fn random_height(&self) -> usize {
        let mut rng = rand::thread_rng();
        for h in 0..(MAX_HEIGHT - 1) {
            if !rng.gen_ratio(HEIGHT_INCREASE, u32::MAX) {
                return h;
            }
        }
        MAX_HEIGHT - 1
    }

    fn height(&self) -> usize {
        self.core.height.load(Ordering::SeqCst)
    }

    pub fn arena_block_size(&self) -> usize {
        self.core.arena.block_size()
    }
}

impl<C: KeyComparator, A: Arena<Stats = BasicStats> + Clone> Skiplist<C, A> {
    /// Finds the node near to key.
    ///
    /// If less=true, it finds rightmost node such that node.key < key (if
    /// allow_equal=false) or node.key <= key (if allow_equal=true).
    /// If less=false, it finds leftmost node such that node.key > key (if
    /// allowEqual=false) or node.key >= key (if allow_equal=true).
    /// Returns the node found.
    unsafe fn find_near(&self, key: &[u8], less: bool, allow_equal: bool) -> *const Node {
        let mut cursor: *const Node = self.core.head.as_ptr();
        let mut level = self.height();
        loop {
            // Assume cursor.key < key
            let next_ptr = (*cursor).next_ptr(level);
            if next_ptr.is_null() {
                // cursor.key < key < END OF LIST
                if level > 0 {
                    // Can descend further to iterate closer to the end
                    level -= 1;
                    continue;
                }
                // 1. Level=0. Cannot descend further. Let's return something that makes sense
                // 2. Try to return cursor. Make sure it is not a head node
                if !less || cursor == self.core.head.as_ptr() {
                    return ptr::null();
                }
                return cursor;
            }

            let next = &*next_ptr;
            let res = self.c.compare_key(key, next.key());
            if res == std::cmp::Ordering::Greater {
                // cursor.key < next.key < key. We can continue to move right
                cursor = next_ptr;
                continue;
            }
            if res == std::cmp::Ordering::Equal {
                // cursor.key < key == next.key
                if allow_equal {
                    return next;
                }
                if !less {
                    // We want >, so go to base level to grab the next bigger node
                    return next.next_ptr(0);
                }
                // We want <. If not base level, we should go closer in the next level.
                if level > 0 {
                    level -= 1;
                    continue;
                }
                // On base level. Return cursor
                if cursor == self.core.head.as_ptr() {
                    return ptr::null();
                }
                return cursor;
            }
            // cursor.key < key < next.key
            if level > 0 {
                level -= 1;
                continue;
            }
            // At base level. Need to return something
            if !less {
                return next;
            }
            // Try to return cursor. Make sure it is not a head node
            if cursor == self.core.head.as_ptr() {
                return ptr::null();
            }
            return cursor;
        }
    }

    /// Returns (out_before, out_after) with out_before.key <= key <=
    /// out_after.key
    ///
    /// The input `before` tells us where to start looking
    /// If we found a node with the same key, then we return out_before =
    /// out_after. Otherwise, out_before.key < key < out_after.key
    unsafe fn find_splice_for_level(
        &self,
        key: &[u8],
        mut before: *mut Node,
        level: usize,
    ) -> (*mut Node, *mut Node) {
        loop {
            // Assume before.key < key
            let next_ptr = (*before).next_ptr(level);
            if next_ptr.is_null() {
                return (before, ptr::null_mut());
            }
            let next_node = &*next_ptr;
            match self.c.compare_key(key, next_node.key()) {
                // Equality case
                std::cmp::Ordering::Equal => return (next_ptr, next_ptr),
                // before.key < key < next.key. We are done for this level
                std::cmp::Ordering::Less => return (before, next_ptr),
                // Keep moving right on this level
                _ => before = next_ptr,
            }
        }
    }

    /// Put the key-value into the skiplist if the key does not exists.
    ///
    /// The content of key and value will be copied into the list. Returns true
    /// if the node is inserted, otherwise return false (key is duplicated)
    ///
    /// Panic: The skiplist will panic if the allocated memory
    /// out of the capacity
    pub fn put(&self, key: &[u8], value: &[u8]) -> bool {
        let mut list_height = self.height();
        let mut prev = [ptr::null_mut(); MAX_HEIGHT + 1];
        let mut next = [ptr::null_mut(); MAX_HEIGHT + 1];
        prev[list_height + 1] = self.core.head.as_ptr();
        // Recompute splice levels
        for i in (0..=list_height).rev() {
            // Use higher level to speed up for current level
            let (p, n) = unsafe { self.find_splice_for_level(key, prev[i + 1], i) };
            prev[i] = p;
            next[i] = n;
            if p == n {
                // Key already exists
                return false;
            }
        }

        // Create a new node
        let height = self.random_height();
        let node_ptr = Node::alloc(&self.core.arena, key, value, height);

        // Try to increase skiplist height via CAS
        while height > list_height {
            match self.core.height.compare_exchange_weak(
                list_height,
                height,
                Ordering::SeqCst,
                Ordering::SeqCst,
            ) {
                // Successfully increased skiplist height
                Ok(_) => break,
                Err(h) => list_height = h,
            }
        }

        // We always insert from the base level and up. After you add a node in base
        // level, we cannot create a node in the level above because it would
        // have discovered the node in the base level
        let x: &mut Node = unsafe { &mut *node_ptr };
        for i in 0..=height {
            loop {
                if prev[i].is_null() {
                    // This cannot happen in base level
                    assert!(i > 1);
                    // We haven't computed prev, next for this level because height exceeds old
                    // list_height. For these levels, we expect the lists to be
                    // sparse, so we can just search from head.
                    let (p, n) =
                        unsafe { self.find_splice_for_level(x.key(), self.core.head.as_ptr(), i) };
                    prev[i] = p;
                    next[i] = n;
                    // Someone adds the exact same key before we are able to do so. This can only
                    // happen on the base level. But we know we are not on the
                    // base level.
                    assert_ne!(p, n);
                }
                x.tower[i].store(next[i], Ordering::SeqCst);
                match unsafe { &*prev[i] }.tower[i].compare_exchange(
                    next[i],
                    node_ptr,
                    Ordering::SeqCst,
                    Ordering::SeqCst,
                ) {
                    // Managed to insert x between prev[i] and next[i]. Go to the next level.
                    Ok(_) => break,
                    Err(_) => {
                        // CAS failed. We need to recompute prev and next.
                        // It is unlikely to be helpful to try to use a different level as we redo
                        // the search, because it is unlikely that lots of
                        // nodes are inserted between prev[i] and next[i].
                        let (p, n) = unsafe { self.find_splice_for_level(x.key(), prev[i], i) };
                        if p == n {
                            assert_eq!(i, 0);
                            return false;
                        }
                        prev[i] = p;
                        next[i] = n;
                    }
                }
            }
        }
        true
    }

    /// Returns if the skiplist is empty
    pub fn is_empty(&self) -> bool {
        let node = self.core.head.as_ptr();
        let next_ptr = unsafe { (*node).next_ptr(0) };
        next_ptr.is_null()
    }

    /// Returns len of the skiplist
    pub fn len(&self) -> usize {
        let mut node = self.core.head.as_ptr();
        let mut count = 0;
        loop {
            let next_ptr = unsafe { (*node).next_ptr(0) };
            if !next_ptr.is_null() {
                count += 1;
                node = next_ptr;
                continue;
            }
            return count;
        }
    }

    /// Returns the last element. If head (empty list), we return null. All the
    /// find functions will NEVER return the head nodes
    fn find_last(&self) -> *const Node {
        let mut node = self.core.head.as_ptr();
        let mut level = self.height();
        loop {
            let next_ptr = unsafe { (*node).next_ptr(level) };
            if !next_ptr.is_null() {
                node = next_ptr;
                continue;
            }
            // next is null
            if level == 0 {
                if node == self.core.head.as_ptr() {
                    return ptr::null();
                }
                return node;
            }
            level -= 1;
        }
    }

    /// Gets the value associated with the key. It returns a valid value if it
    /// finds equal or earlier version of the same key.
    pub fn get(&self, key: &[u8]) -> Option<&[u8]> {
        if let Some((_, value)) = self.get_with_key(key) {
            Some(value)
        } else {
            None
        }
    }

    /// Gets the key and value associated with the key. It returns a valid value
    /// if it finds equal or earlier version of the same key.
    pub fn get_with_key(&self, key: &[u8]) -> Option<(&[u8], &[u8])> {
        // Find greater or equal
        let node = unsafe { self.find_near(key, false, true) };
        if node.is_null() {
            return None;
        }
        if self.c.same_key(unsafe { (*node).key() }, key) {
            return Some(unsafe { ((*node).key(), (*node).value()) });
        }
        None
    }

    /// Returns a skiplist iterator
    pub fn iter_ref(&self) -> IterRef<&Skiplist<C, A>, C, A> {
        IterRef {
            list: self,
            cursor: ptr::null(),
            _key_cmp: std::marker::PhantomData,
            _arena: std::marker::PhantomData,
        }
    }

    /// Returns a skiplist iterator
    pub fn iter(&self) -> IterRef<Skiplist<C, A>, C, A> {
        IterRef {
            list: self.clone(),
            cursor: ptr::null(),
            _key_cmp: std::marker::PhantomData,
            _arena: std::marker::PhantomData,
        }
    }

    /// Consider the total bytes allocated by the arena (not the bytes used).
    pub fn mem_size(&self) -> u32 {
        self.core.arena.stats().bytes_allocated() as u32
    }
}

impl<C, A: Arena<Stats = BasicStats> + Clone> AsRef<Skiplist<C, A>> for Skiplist<C, A> {
    fn as_ref(&self) -> &Skiplist<C, A> {
        self
    }
}

unsafe impl<C: Send, A: Arena<Stats = BasicStats> + Clone + Send> Send for Skiplist<C, A> {}
unsafe impl<C: Sync, A: Arena<Stats = BasicStats> + Clone + Sync> Sync for Skiplist<C, A> {}

pub struct IterRef<T, C, A>
where
    T: AsRef<Skiplist<C, A>>,
    A: Arena<Stats = BasicStats> + Clone,
{
    list: T,
    cursor: *const Node,
    _key_cmp: std::marker::PhantomData<C>,
    _arena: std::marker::PhantomData<A>,
}

impl<T: AsRef<Skiplist<C, A>>, C: KeyComparator, A: Arena<Stats = BasicStats> + Clone>
    IterRef<T, C, A>
{
    pub fn valid(&self) -> bool {
        !self.cursor.is_null()
    }

    pub fn key(&self) -> &[u8] {
        assert!(self.valid());
        unsafe { (*self.cursor).key() }
    }

    pub fn value(&self) -> &[u8] {
        assert!(self.valid());
        unsafe { (*self.cursor).value() }
    }

    pub fn next(&mut self) {
        assert!(self.valid());
        unsafe {
            self.cursor = (*self.cursor).next_ptr(0);
        }
    }

    pub fn prev(&mut self) {
        assert!(self.valid());
        unsafe {
            self.cursor = self.list.as_ref().find_near(self.key(), true, false);
        }
    }

    pub fn seek(&mut self, target: &[u8]) {
        unsafe {
            self.cursor = self.list.as_ref().find_near(target, false, true);
        }
    }

    pub fn seek_for_prev(&mut self, target: &[u8]) {
        unsafe {
            self.cursor = self.list.as_ref().find_near(target, true, true);
        }
    }

    pub fn seek_to_first(&mut self) {
        unsafe {
            self.cursor = (*self.list.as_ref().core.head.as_ptr()).next_ptr(0);
        }
    }

    pub fn seek_to_last(&mut self) {
        self.cursor = self.list.as_ref().find_last();
    }

    pub fn key_with_arena(&self) -> ArenaSlice<A> {
        assert!(self.valid());
        unsafe { (*self.cursor).key_with_arena(self.list.as_ref().core.arena.clone()) }
    }

    pub fn value_with_arena(&self) -> ArenaSlice<A> {
        assert!(self.valid());
        unsafe { (*self.cursor).value_with_arena(self.list.as_ref().core.arena.clone()) }
    }
}

unsafe impl<T: AsRef<Skiplist<C, A>>, C: Send, A: Arena<Stats = BasicStats> + Clone + Send> Send
    for IterRef<T, C, A>
{
}
unsafe impl<T: AsRef<Skiplist<C, A>>, C: Sync, A: Arena<Stats = BasicStats> + Clone + Sync> Sync
    for IterRef<T, C, A>
{
}

#[cfg(test)]
mod tests {
    use arena::MonoIncArena;
    use bytes::Bytes;

    use super::*;
    use crate::FixedLengthSuffixComparator;

    #[test]
    fn test_node_alloc() {
        let arena = MonoIncArena::new(1 << 10);
        let key = b"key of node";
        let value = b"value of node";
        let node_ptr = Node::alloc(&arena, key, value, 5);
        unsafe {
            let node = &*node_ptr;
            assert_eq!(5, node.height);
            for i in 0..=node.height {
                assert!(node.tower[i].load(Ordering::SeqCst).is_null());
            }
            assert_eq!(key, node.key());
            assert_eq!(value, node.value());
        }
    }

    #[test]
    fn test_find_near() {
        let comp = FixedLengthSuffixComparator::new(8);
        let arena = MonoIncArena::new(1 << 10);
        let list = Skiplist::with_arena(comp, arena);
        for i in 0..1000 {
            let key = Bytes::from(format!("{:05}{:08}", i * 10 + 5, 0));
            let value = Bytes::from(format!("{i:05}"));
            list.put(&key, &value);
        }
        let mut cases = vec![
            ("00001", false, false, Some("00005")),
            ("00001", false, true, Some("00005")),
            ("00001", true, false, None),
            ("00001", true, true, None),
            ("00005", false, false, Some("00015")),
            ("00005", false, true, Some("00005")),
            ("00005", true, false, None),
            ("00005", true, true, Some("00005")),
            ("05555", false, false, Some("05565")),
            ("05555", false, true, Some("05555")),
            ("05555", true, false, Some("05545")),
            ("05555", true, true, Some("05555")),
            ("05558", false, false, Some("05565")),
            ("05558", false, true, Some("05565")),
            ("05558", true, false, Some("05555")),
            ("05558", true, true, Some("05555")),
            ("09995", false, false, None),
            ("09995", false, true, Some("09995")),
            ("09995", true, false, Some("09985")),
            ("09995", true, true, Some("09995")),
            ("59995", false, false, None),
            ("59995", false, true, None),
            ("59995", true, false, Some("09995")),
            ("59995", true, true, Some("09995")),
        ];
        for (i, (key, less, allow_equal, exp)) in cases.drain(..).enumerate() {
            let seek_key = Bytes::from(format!("{}{:08}", key, 0));
            let res = unsafe { list.find_near(&seek_key, less, allow_equal) };
            if exp.is_none() {
                assert!(res.is_null(), "{}", i);
                continue;
            }
            let e = format!("{}{:08}", exp.unwrap(), 0);
            assert_eq!(unsafe { (*res).key() }, e.as_bytes(), "{i}");
        }
    }
}
