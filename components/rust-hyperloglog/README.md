// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

hyperloglog
===========

A [HyperLogLog](https://static.googleusercontent.com/media/research.google.com/en/us/pubs/archive/40671.pdf) implementation in Rust, with bias correction.

Installation: use [Cargo](http://crates.io):

```toml
[dependencies]
hyperloglog = "0"
```

Usage:

```rust
let mut hll = HyperLogLog::new(error_rate);
hll.insert(&"test1");
hll.insert(&"test2");
let card_estimation = hll.len();

let mut hll2 = HyperLogLog::new_from_template(&hll);
hll2.insert(&"test3");

hll.merge(&hll2);
```
