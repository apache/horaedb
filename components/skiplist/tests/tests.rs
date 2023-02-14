// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

use std::{
    str,
    sync::{atomic::*, *},
    thread::yield_now,
    time::Duration,
};

use arena::MonoIncArena;
use bytes::*;
use skiplist::*;
use yatp::task::callback::Handle;

fn new_value(v: usize) -> Bytes {
    Bytes::from(format!("{v:05}"))
}

fn key_with_ts(key: &str, ts: u64) -> Bytes {
    Bytes::from(format!("{key}{ts:08}"))
}

#[test]
fn test_empty() {
    let key = key_with_ts("aaa", 0);
    let comp = FixedLengthSuffixComparator::new(8);
    let arena = MonoIncArena::new(1 << 10);
    let list = Skiplist::with_arena(comp, arena);
    let v = list.get(&key);
    assert!(v.is_none());

    let mut iter = list.iter_ref();
    assert!(!iter.valid());
    iter.seek_to_first();
    assert!(!iter.valid());
    iter.seek_to_last();
    assert!(!iter.valid());
    iter.seek(&key);
    assert!(!iter.valid());
    assert!(list.is_empty());
}

#[test]
fn test_basic() {
    let comp = FixedLengthSuffixComparator::new(8);
    let arena = MonoIncArena::new(1 << 10);
    let list = Skiplist::with_arena(comp, arena);
    let table = vec![
        ("key1", new_value(42)),
        ("key2", new_value(52)),
        ("key3", new_value(62)),
        ("key5", Bytes::from(format!("{:0102400}", 1))),
        ("key4", new_value(72)),
    ];

    for (key, value) in &table {
        list.put(&key_with_ts(key, 0), value);
    }

    assert_eq!(list.get(&key_with_ts("key", 0)), None);
    assert_eq!(list.len(), 5);
    assert!(!list.is_empty());
    for (key, value) in &table {
        let get_key = key_with_ts(key, 0);
        assert_eq!(list.get(&get_key), Some(&value[..]), "{key}");
    }
}

fn test_concurrent_basic(n: usize, value_len: usize) {
    let pool = yatp::Builder::new("concurrent_basic").build_callback_pool();
    let comp = FixedLengthSuffixComparator::new(8);
    let arena = MonoIncArena::new(1 << 10);
    let list = Skiplist::with_arena(comp, arena);
    let kvs: Vec<_> = (0..n)
        .map(|i| {
            (
                key_with_ts(format!("{i:05}").as_str(), 0),
                Bytes::from(format!("{i:0value_len$}")),
            )
        })
        .collect();
    let (tx, rx) = mpsc::channel();
    for (k, v) in kvs.clone() {
        let tx = tx.clone();
        let list = list.clone();
        pool.spawn(move |_: &mut Handle<'_>| {
            list.put(&k, &v);
            tx.send(()).unwrap();
        })
    }
    for _ in 0..n {
        rx.recv_timeout(Duration::from_secs(3)).unwrap();
    }
    for (k, v) in kvs {
        let tx = tx.clone();
        let list = list.clone();
        pool.spawn(move |_: &mut Handle<'_>| {
            let val = list.get(&k);
            assert_eq!(val, Some(&v[..]), "{k:?}");
            tx.send(()).unwrap();
        });
    }
    for _ in 0..n {
        rx.recv_timeout(Duration::from_secs(3)).unwrap();
    }
    assert_eq!(list.len(), n);
}

#[test]
fn test_concurrent_basic_small_value() {
    test_concurrent_basic(1000, 5);
}

#[test]
fn test_concurrent_basic_big_value() {
    test_concurrent_basic(100, 1048576);
}

#[test]
fn test_one_key() {
    let n = 10000;
    let write_pool = yatp::Builder::new("one_key_write").build_callback_pool();
    let read_pool = yatp::Builder::new("one_key_read").build_callback_pool();
    let comp = FixedLengthSuffixComparator::new(8);
    let arena = MonoIncArena::new(1 << 10);
    let list = Skiplist::with_arena(comp, arena);
    let key = key_with_ts("thekey", 0);
    let (tx, rx) = mpsc::channel();
    list.put(&key, &new_value(0));
    for i in 0..n {
        let tx = tx.clone();
        let list = list.clone();
        let key = key.clone();
        let value = new_value(i);
        write_pool.spawn(move |_: &mut Handle<'_>| {
            list.put(&key, &value);
            tx.send("w").unwrap();
            yield_now();
        })
    }
    let mark = Arc::new(AtomicBool::new(false));
    for _ in 0..n {
        let tx = tx.clone();
        let list = list.clone();
        let mark = mark.clone();
        let key = key.clone();
        read_pool.spawn(move |_: &mut Handle<'_>| {
            let val = list.get(&key);
            if val.is_none() {
                return;
            }
            let s = unsafe { str::from_utf8_unchecked(val.unwrap()) };
            let val: usize = s.parse().unwrap();
            assert!(val < n);
            mark.store(true, Ordering::SeqCst);
            tx.send("r").unwrap();
            yield_now();
        });
    }
    let mut r = 0;
    let mut w = 0;
    for _ in 0..(n * 2) {
        match rx.recv_timeout(Duration::from_secs(3)) {
            Ok("w") => w += 1,
            Ok("r") => r += 1,
            Err(err) => panic!("timeout on receiving r{r} w{w} msg {err:?}"),
            _ => panic!("unexpected value"),
        }
    }
    assert_eq!(list.len(), 1);
    assert!(mark.load(Ordering::SeqCst));
}

#[test]
fn test_iterator_next() {
    let n = 100;
    let comp = FixedLengthSuffixComparator::new(8);
    let arena = MonoIncArena::new(1 << 10);
    let list = Skiplist::with_arena(comp, arena);
    let mut iter_ref = list.iter_ref();
    assert!(!iter_ref.valid());
    iter_ref.seek_to_first();
    assert!(!iter_ref.valid());
    for i in (0..n).rev() {
        let key = key_with_ts(format!("{i:05}").as_str(), 0);
        list.put(&key, &new_value(i));
    }
    iter_ref.seek_to_first();
    for i in 0..n {
        assert!(iter_ref.valid());
        let v = iter_ref.value();
        assert_eq!(v, &new_value(i));
        iter_ref.next();
    }
    assert!(!iter_ref.valid());
}

#[test]
fn test_iterator_prev() {
    let n = 100;
    let comp = FixedLengthSuffixComparator::new(8);
    let arena = MonoIncArena::new(1 << 10);
    let list = Skiplist::with_arena(comp, arena);
    let mut iter_ref = list.iter_ref();
    assert!(!iter_ref.valid());
    iter_ref.seek_to_last();
    assert!(!iter_ref.valid());
    for i in (0..n).rev() {
        let key = key_with_ts(format!("{i:05}").as_str(), 0);
        list.put(&key, &new_value(i));
    }
    iter_ref.seek_to_last();
    for i in (0..n).rev() {
        assert!(iter_ref.valid());
        let v = iter_ref.value();
        assert_eq!(v, &new_value(i));
        iter_ref.prev();
    }
    assert!(!iter_ref.valid());
}

#[test]
fn test_iterator_seek() {
    let n = 100;
    let comp = FixedLengthSuffixComparator::new(8);
    let arena = MonoIncArena::new(1 << 10);
    let list = Skiplist::with_arena(comp, arena);
    let mut iter_ref = list.iter_ref();
    assert!(!iter_ref.valid());
    iter_ref.seek_to_first();
    assert!(!iter_ref.valid());
    for i in (0..n).rev() {
        let v = i * 10 + 1000;
        let key = key_with_ts(format!("{v:05}").as_str(), 0);
        list.put(&key, &new_value(v));
    }
    iter_ref.seek_to_first();
    assert!(iter_ref.valid());
    assert_eq!(iter_ref.value(), b"01000" as &[u8]);

    let cases = vec![
        ("00000", Some(b"01000"), None),
        ("01000", Some(b"01000"), Some(b"01000")),
        ("01005", Some(b"01010"), Some(b"01000")),
        ("01010", Some(b"01010"), Some(b"01010")),
        ("99999", None, Some(b"01990")),
    ];
    for (key, seek_expect, for_prev_expect) in cases {
        let key = key_with_ts(key, 0);
        iter_ref.seek(&key);
        assert_eq!(iter_ref.valid(), seek_expect.is_some());
        if let Some(v) = seek_expect {
            assert_eq!(iter_ref.value(), &v[..]);
        }
        iter_ref.seek_for_prev(&key);
        assert_eq!(iter_ref.valid(), for_prev_expect.is_some());
        if let Some(v) = for_prev_expect {
            assert_eq!(iter_ref.value(), &v[..]);
        }
    }
}
