// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

use std::{
    collections::*,
    sync::{atomic::*, *},
    thread,
};

use arena::MonoIncArena;
use bytes::*;
use criterion::*;
use rand::prelude::*;
use skiplist::*;

// #[cfg(not(target_env = "msvc"))]
// use tikv_jemallocator::Jemalloc;

// #[cfg(not(target_env = "msvc"))]
// #[global_allocator]
// static GLOBAL: Jemalloc = Jemalloc;

fn skiplist_round(
    l: &Skiplist<FixedLengthSuffixComparator, MonoIncArena>,
    case: &(Bytes, bool),
    exp: &Bytes,
) {
    if case.1 {
        if let Some(v) = l.get(&case.0) {
            assert_eq!(v, exp);
        }
    } else {
        l.put(&case.0, exp);
    }
}

fn append_ts(key: &mut BytesMut, ts: u64) {
    key.put_u64(ts);
}

fn random_key(rng: &mut ThreadRng) -> Bytes {
    let mut key = BytesMut::with_capacity(16);
    unsafe {
        rng.fill_bytes(&mut *(&mut key.chunk_mut()[..8] as *mut _ as *mut [u8]));
        key.advance_mut(8);
    }
    append_ts(&mut key, 0);
    key.freeze()
}

fn bench_read_write_skiplist_frac(b: &mut Bencher<'_>, frac: &usize) {
    let frac = *frac;
    let value = Bytes::from_static(b"00123");
    let comp = FixedLengthSuffixComparator::new(8);
    let arena = MonoIncArena::new(1 << 10);
    let list = Skiplist::with_arena(comp, arena);
    let l = list.clone();
    let stop = Arc::new(AtomicBool::new(false));
    let s = stop.clone();
    let v = value.clone();
    let handle = thread::spawn(move || {
        let mut rng = rand::thread_rng();
        while !s.load(Ordering::SeqCst) {
            let key = random_key(&mut rng);
            let case = (key, frac > rng.gen_range(0, 11));
            skiplist_round(&l, &case, &v);
        }
    });
    let mut rng = rand::thread_rng();
    b.iter_batched_ref(
        || (random_key(&mut rng), frac > rng.gen_range(0, 11)),
        |case| skiplist_round(&list, case, &value),
        BatchSize::SmallInput,
    );
    stop.store(true, Ordering::SeqCst);
    handle.join().unwrap();
}

fn bench_read_write_skiplist(c: &mut Criterion) {
    let mut group = c.benchmark_group("skiplist_read_write");
    for i in 0..=10 {
        group.bench_with_input(
            BenchmarkId::from_parameter(i),
            &i,
            bench_read_write_skiplist_frac,
        );
    }
    group.finish();
}

fn map_round(m: &Mutex<HashMap<Bytes, Bytes>>, case: &(Bytes, bool), exp: &Bytes) {
    if case.1 {
        let rm = m.lock().unwrap();
        let value = rm.get(&case.0);
        if let Some(v) = value {
            assert_eq!(v, exp);
        }
    } else {
        let mut rm = m.lock().unwrap();
        rm.insert(case.0.clone(), exp.clone());
    }
}

fn bench_read_write_map_frac(b: &mut Bencher<'_>, frac: &usize) {
    let frac = *frac;
    let value = Bytes::from_static(b"00123");
    let map = Arc::new(Mutex::new(HashMap::with_capacity(512 << 10)));
    let map_in_thread = map.clone();
    let stop = Arc::new(AtomicBool::new(false));
    let thread_stop = stop.clone();

    let v = value.clone();
    let handle = thread::spawn(move || {
        let mut rng = rand::thread_rng();
        while !thread_stop.load(Ordering::SeqCst) {
            let f = rng.gen_range(0, 11);
            let case = (random_key(&mut rng), f < frac);
            map_round(&map_in_thread, &case, &v);
        }
    });
    let mut rng = rand::thread_rng();
    b.iter_batched_ref(
        || {
            let f = rng.gen_range(0, 11);
            (random_key(&mut rng), f < frac)
        },
        |case| map_round(&map, case, &value),
        BatchSize::SmallInput,
    );
    stop.store(true, Ordering::SeqCst);
    handle.join().unwrap();
}

fn bench_read_write_map(c: &mut Criterion) {
    let mut group = c.benchmark_group("map_read_write");
    for i in 0..=10 {
        group.bench_with_input(
            BenchmarkId::from_parameter(i),
            &i,
            bench_read_write_map_frac,
        );
    }
    group.finish();
}

fn bench_write_skiplist(c: &mut Criterion) {
    let comp = FixedLengthSuffixComparator::new(8);
    let arena = MonoIncArena::new(1 << 10);
    let list = Skiplist::with_arena(comp, arena);
    let value = Bytes::from_static(b"00123");
    let l = list.clone();
    let stop = Arc::new(AtomicBool::new(false));
    let s = stop.clone();
    let v = value.clone();
    let handle = thread::spawn(move || {
        let mut rng = rand::thread_rng();
        while !s.load(Ordering::SeqCst) {
            let case = (random_key(&mut rng), false);
            skiplist_round(&l, &case, &v);
        }
    });
    let mut rng = rand::thread_rng();
    c.bench_function("skiplist_write", |b| {
        b.iter_batched(
            || random_key(&mut rng),
            |key| {
                list.put(&key, &value);
            },
            BatchSize::SmallInput,
        )
    });
    stop.store(true, Ordering::SeqCst);
    handle.join().unwrap();
}

criterion_group!(
    benches,
    bench_read_write_skiplist,
    bench_read_write_map,
    bench_write_skiplist
);
criterion_main!(benches);
