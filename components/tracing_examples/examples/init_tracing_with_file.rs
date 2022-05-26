// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

use tracing_util::{init_tracing_with_file, tracing_appender::rolling::Rotation};

#[tracing::instrument(level = "debug")]
fn nth_fibonacci(n: u64) -> u64 {
    if n == 0 || n == 1 {
        1
    } else {
        nth_fibonacci(n - 1) + nth_fibonacci(n - 2)
    }
}

// default leve info
#[tracing::instrument]
fn fibonacci_seq(to: u64) -> Vec<u64> {
    let mut sequence = vec![];

    for n in 0..=to {
        sequence.push(nth_fibonacci(n));
    }

    sequence
}

// cargo run --example init_tracing_with_file
// log file: /tmp/test_logs/init_tracing_with_file
// 2021-09-28T22:41:30.362078+08:00  INFO main ThreadId(01) fibonacci_seq{to=5}:
// init_tracing_with_file: enter 2021-09-28T22:41:30.364181+08:00  INFO main
// ThreadId(01) fibonacci_seq{to=5}: init_tracing_with_file: close
// time.busy=2.13ms time.idle=34.8µs
fn main() {
    let _g = init_tracing_with_file(
        "init_tracing_with_file",
        "/tmp/test_logs",
        "info",
        Rotation::NEVER,
    );
    let ret = fibonacci_seq(5);
    println!("{:?}", ret);
}
